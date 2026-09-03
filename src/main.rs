use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StreamId {
    //StreamID的两个部分都是非负整数，所以用u64而非i64
    milliseconds: u64,
    sequence_number: u64,
}

#[derive(Clone, Copy, Debug)]
enum StreamIdSpec {
    Explicit(StreamId),
    AutoSequence(u64),
    Auto,
}

enum XaddError {
    WrongType,
    IdNotGreater,
}

impl StreamId {
    const ZERO: Self = Self {
        milliseconds: 0,
        sequence_number: 0,
    };

    const MAX: Self = Self {
        milliseconds: u64::MAX,
        sequence_number: u64::MAX,
    };
}

#[derive(Clone)]
struct StreamEntry {
    id: StreamId,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

struct StreamReadResult {
    key: Vec<u8>,
    entries: Vec<StreamEntry>,
}

enum RedisValue {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Stream(Vec<StreamEntry>),
}

struct Entry {
    value: RedisValue,
    expires_at: Option<Instant>,
}

// 多个客户端任务共享的内存键值数据库。
type Database = Arc<Mutex<HashMap<Vec<u8>, Entry>>>;
//通知器：负责告诉BLPOP某个列表可能有新的元素，可以重新检查数据库
type ListSignals = Arc<Mutex<HashMap<Vec<u8>, Arc<Notify>>>>;

/*
所有Stream共用一个状态变化通知器
XADD成功后唤醒正在阻塞等待的XREAD
 */
type StreamSignals = Arc<Notify>;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let database: Database = Arc::new(Mutex::new(HashMap::new()));
    let list_signals: ListSignals = Arc::new(Mutex::new(HashMap::new()));
    let stream_signals: StreamSignals = Arc::new(Notify::new());

    loop {
        match listener.accept().await {
            Ok((stream, _address)) => {
                let database = Arc::clone(&database);
                let list_signals = Arc::clone(&list_signals);
                let stream_signals = Arc::clone(&stream_signals);

                tokio::spawn(handle_client(
                    stream,
                    database,
                    list_signals,
                    stream_signals,
                ));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    database: Database,
    list_signals: ListSignals,
    stream_signals: StreamSignals,
) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        let command = match read_command(&mut reader).await {
            Ok(Some(command)) => command,
            Ok(None) => break,
            Err(e) => {
                println!("error parsing command: {}", e);
                break;
            }
        };
        if command.len() == 1 && command[0].eq_ignore_ascii_case(b"PING") {
            write_half.write_all(b"+PONG\r\n").await.unwrap();
        } else if command.len() == 2 && command[0].eq_ignore_ascii_case(b"ECHO") {
            write_bulk_string(&mut write_half, &command[1])
                .await
                .unwrap()
        } else if !command.is_empty() && command[0].eq_ignore_ascii_case(b"SET") {
            let expires_at = if command.len() == 3 {
                None
            } else if command.len() == 5 && command[3].eq_ignore_ascii_case(b"PX") {
                let milliseconds = match parse_milliseconds(&command[4]) {
                    Ok(milliseconds) if milliseconds > 0 => milliseconds,
                    _ => {
                        write_half
                            .write_all(b"-ERR invalid expire time in 'set' command\r\n")
                            .await
                            .unwrap();
                        continue;
                    }
                };

                Some(Instant::now() + Duration::from_millis(milliseconds)) //当前时间+有效期=到期时刻
            } else {
                write_half
                    .write_all(b"-ERR syntax error\r\n")
                    .await
                    .unwrap();
                continue;
            };

            {
                //这一层大括号是为了缩小数据库锁的作用范围，让锁在插入完成后立即释放。
                //把SET命令中的key，value和过期时间写入共享数据库
                let mut db = database.lock().await; //等待并获取数据库的可变锁

                db.insert(
                    command[1].clone(), //复制一份key作为内存HashMap里的唯一键
                    Entry {
                        value: RedisValue::String(command[2].clone()),
                        expires_at,
                    },
                );
            } //db在这里销毁，mutex锁随之释放

            write_half.write_all(b"+OK\r\n").await.unwrap();
        } else if !command.is_empty() && command[0].eq_ignore_ascii_case(b"TYPE") {
            if command.len() != 2 {
                write_half
                    .write_all(b"-ERR wrong number of arguments for 'type' command\r\n")
                    .await
                    .unwrap();
                continue;
            }

            let value_type: &'static str = {
                let mut db = database.lock().await;
                let now = Instant::now();

                let expired = db
                    .get(&command[1])
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);

                if expired {
                    db.remove(&command[1]);
                    "none"
                } else {
                    match db.get(&command[1]) {
                        Some(Entry {
                            value: RedisValue::String(_),
                            ..
                        }) => "string",

                        Some(Entry {
                            value: RedisValue::Stream(_),
                            ..
                        }) => "stream",

                        Some(Entry {
                            value: RedisValue::List(_),
                            ..
                        }) => "list",

                        None => "none",
                    }
                }
            };

            let response = format!("+{}\r\n", value_type);

            write_half.write_all(response.as_bytes()).await.unwrap();
        } else if command.len() >= 3 && command[0].eq_ignore_ascii_case(b"RPUSH") {
            let result: Result<usize, ()> = {
                let mut db = database.lock().await;
                let now = Instant::now();

                //如果同名key已过期，先删除；之后会被当作新列表创建。
                let expired = db
                    .get(&command[1])
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);
                if expired {
                    db.remove(&command[1]);
                }
                let entry = db.entry(command[1].clone()).or_insert_with(|| Entry {
                    value: RedisValue::List(Vec::new()),
                    expires_at: None,
                });

                match &mut entry.value {
                    RedisValue::List(list) => {
                        list.extend(command[2..].iter().cloned());
                        Ok(list.len())
                    }
                    RedisValue::String(_) | RedisValue::Stream(_) => Err(()),
                }
            };
            match result {
                Ok(length) => {
                    notify_list_waiters(&list_signals, &command[1], command.len() - 2).await;

                    write_integer(&mut write_half, length).await.unwrap();
                }
                Err(()) => {
                    write_half
                        .write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",)
                        .await
                        .unwrap();
                }
            }
        } else if command.len() == 2 && command[0].eq_ignore_ascii_case(b"LLEN") {
            let result: Result<usize, ()> = {
                let mut db = database.lock().await;
                let now = Instant::now();

                let expired = db
                    .get(&command[1])
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);

                if expired {
                    db.remove(&command[1]);
                    Ok(0)
                } else {
                    match db.get(&command[1]) {
                        Some(Entry {
                            value: RedisValue::List(list),
                            ..
                        }) => Ok(list.len()),

                        Some(Entry {
                            value: RedisValue::Stream(_),
                            ..
                        }) => Err(()),

                        Some(Entry {
                            value: RedisValue::String(_),
                            ..
                        }) => Err(()),

                        None => Ok(0),
                    }
                }
            };
            match result {
                Ok(length) => {
                    write_integer(&mut write_half, length).await.unwrap();
                }
                Err(()) => {
                    write_half.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",)
                    .await
                    .unwrap();
                }
            }
        } else if !command.is_empty() && command[0].eq_ignore_ascii_case(b"BLPOP") {
            if command.len() != 3 {
                write_half
                    .write_all(b"-ERR wrong number of arguments for 'BLPOP' command\r\n")
                    .await
                    .unwrap();
                continue;
            }

            let timeout_seconds = match parse_timeout(&command[2]) {
                Ok(seconds) => seconds,

                Err(_) => {
                    write_half
                        .write_all(b"-ERR timeout is not a float or out of range\r\n")
                        .await
                        .unwrap();
                    continue;
                }
            };
            let result = blocking_pop(&database, &list_signals, &command[1], timeout_seconds).await;

            match result {
                Ok(Some(value)) => {
                    let response = vec![command[1].clone(), value];
                    write_array(&mut write_half, &response).await.unwrap();
                }
                Ok(None) => {
                    //超时返回Null Array
                    write_half.write_all(b"*-1\r\n").await.unwrap();
                }
                Err(()) => {
                    write_half.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",).await.unwrap();
                }
            }
        } else if !command.is_empty() && command[0].eq_ignore_ascii_case(b"LPOP") {
            if command.len() != 2 && command.len() != 3 {
                write_half
                    .write_all(b"-ERR wrong number of arguments for 'lpop'\r\n")
                    .await
                    .unwrap();
                continue;
            }

            //是否提供了count参数
            let has_count = command.len() == 3;

            //没提供count时，默认删除一个元素
            let count = if has_count {
                match parse_number(&command[2]) {
                    Ok(count) => count,
                    Err(_) => {
                        write_half
                            .write_all(b"-Err value is not an integer or not range\r\n")
                            .await
                            .unwrap();
                        continue;
                    }
                }
            } else {
                1
            };

            let result: Result<Option<Vec<Vec<u8>>>, ()> = {
                let mut db = database.lock().await;
                let now = Instant::now();

                let expired = db
                    .get(&command[1])
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);

                if expired {
                    db.remove(&command[1]);
                    Ok(None)
                } else {
                    let mut should_remove_key = false;

                    let pop_result: Result<Option<Vec<Vec<u8>>>, ()> = match db.get_mut(&command[1])
                    {
                        Some(Entry {
                            value: RedisValue::List(list),
                            ..
                        }) => {
                            //count 大雨列表长度时，只删除现有元素
                            let amount = count.min(list.len());

                            //drain 会删除范围中的元素，并返回这些元素
                            let values: Vec<Vec<u8>> = list.drain(..amount).collect();

                            should_remove_key = list.is_empty();

                            Ok(Some(values))
                        }

                        Some(Entry {
                            value: RedisValue::String(_) | RedisValue::Stream(_),
                            ..
                        }) => Err(()),

                        None => Ok(None),
                    };

                    if should_remove_key {
                        db.remove(&command[1]);
                    }

                    pop_result
                }
            };

            match result {
                Ok(Some(values)) => {
                    if has_count {
                        //LPOP key count 返回数组。
                        write_array(&mut write_half, &values).await.unwrap();
                    } else {
                        //LPOP key 返回单个Bulk String
                        match values.into_iter().next() {
                            Some(value) => {
                                write_bulk_string(&mut write_half, &value).await.unwrap();
                            }
                            None => {
                                write_half.write_all(b"$-1\r\n").await.unwrap();
                            }
                        }
                    }
                }

                Ok(None) => {
                    if has_count {
                        //带count时，不存在的key返回Null Array。
                        write_half.write_all(b"*-1\r\n").await.unwrap();
                    } else {
                        //不带count时返回Null Bulk String
                        write_half.write_all(b"$-1\r\n").await.unwrap();
                    }
                }

                Err(()) => {
                    write_half.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",).await.unwrap();
                }
            }
        } else if command.len() >= 3 && command[0].eq_ignore_ascii_case(b"LPUSH") {
            let result: Result<usize, ()> = {
                let mut db = database.lock().await;
                let now = Instant::now();

                let expired = db
                    .get(&command[1])
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);

                if expired {
                    db.remove(&command[1]);
                }

                //key不存在时创建空列表
                let entry = db.entry(command[1].clone()).or_insert_with(|| Entry {
                    value: RedisValue::List(Vec::new()),
                    expires_at: None,
                });

                match &mut entry.value {
                    RedisValue::List(list) => {
                        //这里的list实际类型是&mut Vex<Vex<u8>>,也就是“数据库内部列表的可变引用”
                        for value in &command[2..] {
                            list.insert(0, value.clone()); //因此会直接修改数据库里的列表
                        }

                        Ok(list.len())
                    }
                    RedisValue::String(_) | RedisValue::Stream(_) => Err(()),
                }
            };
            match result {
                Ok(length) => {
                    notify_list_waiters(&list_signals, &command[1], command.len() - 2).await;

                    write_integer(&mut write_half, length).await.unwrap();
                }
                Err(()) => {
                    write_half
                        .write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                        .await
                        .unwrap();
                }
            }
        } else if !command.is_empty() && command[0].eq_ignore_ascii_case(b"LRANGE") {
            if command.len() != 4 {
                write_half
                    .write_all(b"-ERR wrong number of arguments for 'lrange' command\r\n")
                    .await
                    .unwrap();
                continue;
            }

            let (start, stop) = match (parse_list_index(&command[2]), parse_list_index(&command[3]))
            {
                (Ok(start), Ok(stop)) => (start, stop),
                _ => {
                    write_half
                        .write_all(b"-ERR value is not an integer ot out of range\r\n")
                        .await
                        .unwrap();
                    continue;
                }
            };
            let result: Result<Vec<Vec<u8>>, ()> = {
                let mut db = database.lock().await;
                let now = Instant::now();

                let expired = db
                    .get(&command[1])
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);

                if expired {
                    db.remove(&command[1]);
                    Ok(Vec::new())
                } else {
                    match db.get(&command[1]) {
                        None => Ok(Vec::new()),

                        Some(Entry {
                            value: RedisValue::String(_) | RedisValue::Stream(_),
                            ..
                        }) => Err(()),

                        Some(Entry {
                            value: RedisValue::List(list),
                            ..
                        }) => match normalize_range(list.len(), start, stop) {
                            Some((start, stop)) => Ok(list[start..=stop].to_vec()),
                            None => Ok(Vec::new()),
                        },
                    }
                }
            };
            match result {
                Ok(values) => {
                    write_array(&mut write_half, &values).await.unwrap();
                }
                Err(()) => {
                    write_half.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",)
                    .await
                    .unwrap();
                }
            }
        } else if command.len() == 2 && command[0].eq_ignore_ascii_case(b"GET") {
            let result: Result<Option<Vec<u8>>, ()> = {
                let mut db = database.lock().await;
                let now = Instant::now();

                let expired = db
                    .get(&command[1])
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);
                if expired {
                    db.remove(&command[1]);
                    Ok(None)
                } else {
                    match db.get(&command[1]) {
                        Some(Entry {
                            value: RedisValue::String(value),
                            ..
                        }) => Ok(Some(value.clone())),
                        Some(Entry {
                            value: RedisValue::List(_) | RedisValue::Stream(_),
                            ..
                        }) => Err(()),

                        None => Ok(None),
                    }
                }
            };
            match result {
                Ok(Some(value)) => {
                    write_bulk_string(&mut write_half, &value).await.unwrap();
                }
                Ok(None) => {
                    write_half.write_all(b"$-1\r\n").await.unwrap();
                }
                Err(()) => {
                    write_half
                        .write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",)
                        .await
                        .unwrap();
                }
            }
        } else if !command.is_empty() && command[0].eq_ignore_ascii_case(b"XADD") {
            if command.len() < 5 || (command.len() - 3) % 2 != 0 {
                write_half
                    .write_all(b"-ERR wrong number of arguments for 'XADD' command\r\n")
                    .await
                    .unwrap();
                continue;
            }

            let key = command[1].clone();

            let id_spec = match parse_stream_id_spec(&command[2]) {
                Ok(id_spec) => id_spec,

                Err(_) => {
                    write_half
                        .write_all(
                            b"-ERR Invalid stream ID specified as stream command argument\r\n",
                        )
                        .await
                        .unwrap();
                    continue;
                }
            };

            if matches!(id_spec,
                StreamIdSpec::Explicit(id) if id == StreamId::ZERO)
            {
                write_half
                    .write_all(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                    .await
                    .unwrap();
                continue;
            }

            let fields: Vec<(Vec<u8>, Vec<u8>)> = command[3..]
                .chunks_exact(2) //每两个元素组成一组
                .map(|pair| (pair[0].clone(), pair[1].clone())) //map是转换每一个元素
                .collect(); //把迭代器产生的所有元素收集到一个集合中 Vec<(field,value)>

            let result: Result<StreamId, XaddError> = {
                let mut db = database.lock().await;
                let now = Instant::now();
                let expired = db
                    .get(&key)
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);
                if expired {
                    db.remove(&key);
                }

                let entry = db.entry(key).or_insert_with(|| Entry {
                    value: RedisValue::Stream(Vec::new()),
                    expires_at: None,
                });

                match &mut entry.value {
                    RedisValue::Stream(entries) => {
                        let last_id = entries.last().map(|entry| &entry.id);

                        match resolve_stream_id(id_spec, last_id) {
                            Ok(id) => {
                                entries.push(StreamEntry { id, fields });
                                Ok(id)
                            }

                            Err(error) => Err(error),
                        }
                    }
                    RedisValue::String(_) | RedisValue::List(_) => Err(XaddError::WrongType),
                }
            };
            match result {
                Ok(id) => {
                    stream_signals.notify_waiters();

                    let response_id = stream_id_to_bytes(id);

                    write_bulk_string(&mut write_half, &response_id)
                        .await
                        .unwrap();
                }

                Err(XaddError::IdNotGreater) => {
                    write_half.write_all(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n").await.unwrap();
                }

                Err(XaddError::WrongType) => {
                    write_half.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",).await.unwrap();
                }
            }
        } else if !command.is_empty() && command[0].eq_ignore_ascii_case(b"XREAD") {
            let (block_milliseconds, streams_index) =
                if command.len() >= 2 && command[1].eq_ignore_ascii_case(b"BLOCK") {
                    if command.len() < 4 {
                        write_half
                            .write_all(b"-ERR syntax error\r\n")
                            .await
                            .unwrap();
                        continue;
                    }

                    let milliseconds = match parse_milliseconds(&command[2]) {
                        Ok(milliseconds) => milliseconds,

                        Err(_) => {
                            write_half
                                .write_all(b"-ERR timeout is not an integer or out of range\r\n")
                                .await
                                .unwrap();
                            continue;
                        }
                    };
                    if !command[3].eq_ignore_ascii_case(b"STREAMS") {
                        write_half
                            .write_all(b"-ERR syntax error\r\n")
                            .await
                            .unwrap();
                        continue;
                    }

                    (Some(milliseconds), 3)
                } else if command.len() >= 2 && command[1].eq_ignore_ascii_case(b"STREAMS") {
                    (None, 1)
                } else {
                    write_half
                        .write_all(b"-ERR syntax error\r\n")
                        .await
                        .unwrap();
                    continue;
                };

            //STREAMS 后面的参数
            let arguments_start = streams_index + 1;
            let arguments_count = command.len() - arguments_start;

            //至少需要一个key和一个ID
            //并且key数量必须与ID数量相同
            if arguments_count < 2 || arguments_count % 2 != 0 {
                write_half
                    .write_all(b"-ERR syntax error\r\n")
                    .await
                    .unwrap();

                continue;
            }

            let stream_count = arguments_count / 2;

            let keys = &command[arguments_start..arguments_start + stream_count];

            let id_arguments = &command[arguments_start + stream_count..];

            let starts: Vec<StreamId> = match id_arguments
                .iter()
                .map(|id| parse_xrange_bound(id, 0))
                .collect::<io::Result<Vec<_>>>()
            {
                Ok(starts) => starts,

                Err(_) => {
                    write_half
                        .write_all(
                            b"-ERR Invalid stream ID specified as stream command argument\r\n",
                        )
                        .await
                        .unwrap();

                    continue;
                }
            };

            let result: Result<Option<Vec<StreamReadResult>>, ()> = match block_milliseconds {
                Some(milliseconds) => {
                    blocking_read_streams(&database, &stream_signals, keys, &starts, milliseconds)
                        .await
                }

                None => read_stream_entries(&database, keys, &starts)
                    .await
                    .map(|streams| {
                        if streams.is_empty() {
                            None
                        } else {
                            Some(streams)
                        }
                    }),
            };

            match result {
                Ok(Some(streams)) => write_xread_response(&mut write_half, &streams)
                    .await
                    .unwrap(),

                Ok(None) => {
                    write_half.write_all(b"*-1\r\n").await.unwrap();
                }

                Err(()) => {
                    write_half.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",).await.unwrap();
                }
            }
        } else if !command.is_empty() && command[0].eq_ignore_ascii_case(b"XRANGE") {
            if command.len() != 4 {
                write_half
                    .write_all(b"-ERR wrong number of arguments for 'XRANGE' command\r\n")
                    .await
                    .unwrap();
                continue;
            }

            let start = if command[2].as_slice() == b"-" {
                StreamId::ZERO
            } else {
                match parse_xrange_bound(&command[2], 0) {
                    Ok(start) => start,

                    Err(_) => {
                        write_half
                            .write_all(
                                b"-ERR Invalid stream ID specified as stream command argument\r\n",
                            )
                            .await
                            .unwrap();

                        continue;
                    }
                }
            };

            let end = if command[3].as_slice() == b"+" {
                StreamId::MAX
            } else {
                match parse_xrange_bound(&command[3], u64::MAX) {
                    Ok(end) => end,

                    Err(_) => {
                        write_half
                            .write_all(
                                b"-ERR Invalid stream ID specified as stream command argument\r\n",
                            )
                            .await
                            .unwrap();
                        continue;
                    }
                }
            };

            let result: Result<Vec<StreamEntry>, ()> = {
                let mut db = database.lock().await;
                let now = Instant::now();

                let expired = db
                    .get(&command[1])
                    .and_then(|entry| entry.expires_at)
                    .is_some_and(|expires_at| now >= expires_at);

                if expired {
                    db.remove(&command[1]);
                    Ok(Vec::new())
                } else {
                    match db.get(&command[1]) {
                        None => Ok(Vec::new()),

                        Some(Entry {
                            value: RedisValue::Stream(entries),
                            ..
                        }) => Ok(entries
                            .iter()
                            .filter(|entry| entry.id >= start && entry.id <= end)
                            .cloned()
                            .collect()),

                        Some(Entry {
                            value: RedisValue::String(_) | RedisValue::List(_),
                            ..
                        }) => Err(()),
                    }
                }
            };

            match result {
                Ok(entries) => write_stream_entries(&mut write_half, &entries)
                    .await
                    .unwrap(),

                Err(()) => {
                    write_half.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").await.unwrap();
                }
            }
        } else {
            write_half
                .write_all(b"-ERR unknown command\r\n")
                .await
                .unwrap();
        }
    }
}

async fn write_integer<W>(writer: &mut W, value: usize) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let response = format!(":{}\r\n", value);
    writer.write_all(response.as_bytes()).await
}

async fn read_command<R>(reader: &mut R) -> io::Result<Option<Vec<Vec<u8>>>>
where
    R: AsyncBufRead + Unpin,
{
    let Some(array_header) = read_resp_line(reader).await? else {
        return Ok(None);
    };
    if array_header.first() != Some(&b'*') {
        return Err(invalid_data("expected RESP array"));
    }

    let argument_count = parse_number(&array_header[1..])?;
    let mut command = Vec::with_capacity(argument_count);

    for _ in 0..argument_count {
        let Some(argument) = read_bulk_string(reader).await? else {
            return Err(invalid_data("unexpected end of input"));
        };

        command.push(argument);
    }

    Ok(Some(command))
}

async fn read_resp_line<R>(reader: &mut R) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();

    let bytes_read = reader.read_until(b'\n', &mut line).await?;

    if bytes_read == 0 {
        return Ok(None);
    }

    if !line.ends_with(b"\r\n") {
        return Err(invalid_data("invalid RESP line"));
    }

    line.truncate(line.len() - 2);

    Ok(Some(line))
}

async fn read_bulk_string<R>(reader: &mut R) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let Some(header) = read_resp_line(reader).await? else {
        return Ok(None);
    };

    if header.first() != Some(&b'$') {
        return Err(invalid_data("expected RESP bulk string"));
    }

    let length = parse_number(&header[1..])?;

    let mut data = vec![0; length + 2];

    reader.read_exact(&mut data).await?;

    if data[length] != b'\r' || data[length + 1] != b'\n' {
        return Err(invalid_data("bulk string missing CRLF"));
    }

    data.truncate(length);

    Ok(Some(data))
}

async fn write_bulk_string<W>(writer: &mut W, value: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let header = format!("${}\r\n", value.len());

    writer.write_all(header.as_bytes()).await?;
    writer.write_all(value).await?;
    writer.write_all(b"\r\n").await
}

fn parse_number(bytes: &[u8]) -> io::Result<usize> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_data("invalid number"))?;

    text.parse::<usize>()
        .map_err(|_| invalid_data("invalid number"))
}

fn parse_milliseconds(bytes: &[u8]) -> io::Result<u64> {
    /*
    因为RESP解析器同一把命令参数保存为Vec<u8>,
    所以先将UTF-8/ASCII字节转换成文本
    */
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_data("invalid milliseconds"))?;
    //再将文本转换成无符号64位整数
    text.parse::<u64>()
        .map_err(|_| invalid_data("invalid expiry"))
}

/*
把列表命令中的索引参数转换成i64整数
 */
fn parse_list_index(bytes: &[u8]) -> io::Result<i64> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_data("invalid list index"))?;

    text.parse::<i64>()
        .map_err(|_| invalid_data("invalid list index"))
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn parse_stream_id_spec(bytes: &[u8]) -> io::Result<StreamIdSpec> {
    if bytes == b"*" {
        return Ok(StreamIdSpec::Auto);
    }

    let text = std::str::from_utf8(bytes).map_err(|_| invalid_data("invalid stream id"))?;

    let (milliseconds_text, sequence_text) = text
        .split_once('-')
        .ok_or_else(|| invalid_data("invalid stream id"))?;

    if milliseconds_text.is_empty() || !milliseconds_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid_data("invalid stream ID"));
    }

    let milliseconds = milliseconds_text
        .parse::<u64>()
        .map_err(|_| invalid_data("invalid Stream ID"))?;

    if sequence_text == "*" {
        return Ok(StreamIdSpec::AutoSequence(milliseconds));
    }

    if sequence_text.is_empty() || !sequence_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_data("invalid stream ID"));
    }

    let sequence_number = sequence_text
        .parse::<u64>()
        .map_err(|_| invalid_data("invalid Stream ID"))?;

    Ok(StreamIdSpec::Explicit(StreamId {
        milliseconds,
        sequence_number,
    }))
}

/*
下标规范化函数
 */
fn normalize_range(length: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if length == 0 {
        return None;
    }

    let length = length as i64;

    let mut start = if start < 0 { length + start } else { start };

    let mut stop = if stop < 0 { length + stop } else { stop };

    //start 太靠左时，从第一个元素开始
    if start < 0 {
        start = 0;
    }

    //start 已经超过列表结尾
    if start >= length {
        return None;
    }

    //stop 超过结尾，缩小到最后一个元素
    if stop >= length {
        stop = length - 1;
    }

    if start > stop {
        return None;
    }

    Some((start as usize, stop as usize))
}

/*
RESP Array写入函数:把多个值按Redis RESP的Array格式写回客户端
 */
async fn write_array<W>(
    writer: &mut W, //相当于把TCP写入端的修改权限借给write_array()
    values: &[Vec<u8>],
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let header = format!("*{}\r\n", values.len());

    writer.write_all(header.as_bytes()).await?; //因为已经拿到write_half所以可以调用writer.write_all...

    for value in values {
        write_bulk_string(writer, value).await?;
    }

    Ok(())
}

//获取某个列表的通知器
async fn notifier_for(list_signals: &ListSignals, key: &[u8]) -> Arc<Notify> {
    let mut signals = list_signals.lock().await;

    Arc::clone(
        signals
            .entry(key.to_vec())
            .or_insert_with(|| Arc::new(Notify::new())),
    )
}

//提取“弹出一个元素”的函数
async fn pop_first(database: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, ()> {
    let mut db = database.lock().await;
    let now = Instant::now();

    let expired = db
        .get(key)
        .and_then(|entry| entry.expires_at)
        .is_some_and(|expires_at| now >= expires_at);

    if expired {
        db.remove(key);
        return Ok(None);
    }

    let mut should_remove_key = false;

    let result = match db.get_mut(key) {
        Some(Entry {
            value: RedisValue::List(list),
            ..
        }) => {
            if list.is_empty() {
                should_remove_key = true;
                Ok(None)
            } else {
                let value = list.remove(0);
                should_remove_key = list.is_empty();

                Ok(Some(value))
            }
        }

        Some(Entry {
            value: RedisValue::String(_) | RedisValue::Stream(_),
            ..
        }) => Err(()),

        None => Ok(None),
    };

    if should_remove_key {
        db.remove(key);
    }

    result
}

async fn wait_for_list_value(
    database: &Database,
    list_signals: &ListSignals,
    key: &[u8],
) -> Result<Vec<u8>, ()> {
    let notifier = notifier_for(list_signals, key).await;

    loop {
        //创建等待下一个通知的Future。
        let notified = notifier.notified();

        //将Future 固定在内存中
        tokio::pin!(notified);

        //先加入公平等待队列，再检查数据库，避免检查和等待之间错过通知
        notified.as_mut().enable();

        match pop_first(database, key).await {
            Ok(Some(value)) => {
                //有元素，立即返回
                return Ok(value);
            }

            Ok(None) => {
                //当前没有元素，释放当前任务并等待通知。
                notified.await;
            }

            Err(()) => {
                //key 存在，但不是列表
                return Err(());
            }
        }
    }
}

//超时参数解析
fn parse_timeout(bytes: &[u8]) -> io::Result<f64> {
    //先将字节转换成文本
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_data("invalid timeout"))?;
    //将文本转换成浮点数
    let seconds = text
        .parse::<f64>()
        .map_err(|_| invalid_data("invalid timeout"))?;
    //判断是否为有限值，
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(invalid_data("invalid timeout"));
    }

    Ok(seconds)
}

async fn blocking_pop(
    database: &Database,
    list_signals: &ListSignals,
    key: &[u8],
    timeout_seconds: f64,
) -> Result<Option<Vec<u8>>, ()> {
    let wait = wait_for_list_value(database, list_signals, key);

    if timeout_seconds == 0.0 {
        // timeout 为 0，永久等待。
        wait.await.map(Some)
    } else {
        let duration = Duration::from_secs_f64(timeout_seconds);

        match tokio::time::timeout(duration, wait).await {
            // 超时前拿到元素。
            Ok(Ok(value)) => Ok(Some(value)),

            // key 不是列表。
            Ok(Err(())) => Err(()),

            // 达到超时时间。
            Err(_) => Ok(None),
        }
    }
}

//RPUSH/LPUSH 成功后发送通知
async fn notify_list_waiters(list_signals: &ListSignals, key: &[u8], added_count: usize) {
    let notifier = notifier_for(list_signals, key).await;

    for _ in 0..added_count {
        notifier.notify_one();
    }
}

//显式验证和自动生成
fn resolve_stream_id(
    spec: StreamIdSpec,
    last_id: Option<&StreamId>,
) -> Result<StreamId, XaddError> {
    match spec {
        StreamIdSpec::Explicit(id) => {
            if last_id.is_some_and(|last_id| id <= *last_id) {
                Err(XaddError::IdNotGreater)
            } else {
                Ok(id)
            }
        }

        StreamIdSpec::AutoSequence(milliseconds) => match last_id {
            None => {
                let sequence_number = if milliseconds == 0 { 1 } else { 0 };

                Ok(StreamId {
                    milliseconds,
                    sequence_number,
                })
            }

            Some(last_id) if milliseconds < last_id.milliseconds => Err(XaddError::IdNotGreater),

            Some(last_id) if milliseconds == last_id.milliseconds => {
                let sequence_number = last_id
                    .sequence_number
                    .checked_add(1)
                    .ok_or(XaddError::IdNotGreater)?;

                Ok(StreamId {
                    milliseconds,
                    sequence_number,
                })
            }

            Some(_) => Ok(StreamId {
                milliseconds,
                sequence_number: 0,
            }),
        },

        StreamIdSpec::Auto => {
            let now = current_unix_milliseconds();

            match last_id {
                None => Ok(StreamId {
                    milliseconds: now,
                    sequence_number: 0,
                }),

                Some(last_id) if now > last_id.milliseconds => Ok(StreamId {
                    milliseconds: now,
                    sequence_number: 0,
                }),

                Some(last_id) => {
                    let sequence_number = last_id
                        .sequence_number
                        .checked_add(1)
                        .ok_or(XaddError::IdNotGreater)?;

                    Ok(StreamId {
                        milliseconds: last_id.milliseconds,
                        sequence_number,
                    })
                }
            }
        }
    }
}

fn stream_id_to_bytes(id: StreamId) -> Vec<u8> {
    format!("{}-{}", id.milliseconds, id.sequence_number).into_bytes()
}

fn current_unix_milliseconds() -> u64 {
    //SyetemTime表示现实世界的日期和时间，可以计算Unix时间戳，Instant只适合测量“过了多久”，不能转换成Unix时间
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch");
    //duration.as_millis()理论上可能表示非常大的时间范围，所以标准库使用容量更大的u128,所以用u64::try_from尝试把u128转换成u64,如果数值超过u64：MAX，转换不能安全完成。
    u64::try_from(duration.as_millis()).expect("Unix timestamp does not fit in u64")
}

fn parse_xrange_bound(bytes: &[u8], default_sequence: u64) -> io::Result<StreamId> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_data("invalid stream id"))?;

    if let Some((milliseconds_text, sequence_text)) = text.split_once('-') {
        //检查字符串是否为空，并检查每个字节是否都是0~9
        if milliseconds_text.is_empty()
            || sequence_text.is_empty()
            || !milliseconds_text.bytes().all(|byte| byte.is_ascii_digit())
            || !sequence_text.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(invalid_data("invalid stream id"));
        }

        //检查是否能存入u64
        let milliseconds = milliseconds_text
            .parse::<u64>()
            .map_err(|_| invalid_data("invalid stream id"))?;

        let sequence_number = sequence_text
            .parse::<u64>()
            .map_err(|_| invalid_data("invalid stream id"))?;

        Ok(StreamId {
            milliseconds,
            sequence_number,
        })
    } else {
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid_data("invalid stream id"));
        }

        let milliseconds = text
            .parse::<u64>()
            .map_err(|_| invalid_data("invalid stream id"))?;

        Ok(StreamId {
            milliseconds,
            sequence_number: default_sequence,
        })
    }
}

async fn write_stream_entries<W>(writer: &mut W, entries: &[StreamEntry]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let outer_header = format!("*{}\r\n", entries.len());
    writer.write_all(outer_header.as_bytes()).await?;

    for entry in entries {
        //每个entry是包含两个元素的数组：[id,fields]
        writer.write_all(b"*2\r\n").await?;

        let id = stream_id_to_bytes(entry.id);
        write_bulk_string(writer, &id).await?;

        let fields_header = format!("*{}\r\n", entry.fields.len() * 2);
        writer.write_all(fields_header.as_bytes()).await?;

        for (field, value) in &entry.fields {
            write_bulk_string(writer, field).await?;
            write_bulk_string(writer, value).await?;
        }
    }

    Ok(())
}

//单stream的XREAD writer
async fn write_xread_response<W>(writer: &mut W, streams: &[StreamReadResult]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let outer_header = format!("*{}\r\n", streams.len());
    writer.write_all(outer_header.as_bytes()).await?;

    for stream in streams {
        //每个Stream 结果是：[key,entries]
        writer.write_all(b"*2\r\n").await?;

        write_bulk_string(writer, &stream.key).await?;

        write_stream_entries(writer, &stream.entries).await?;
    }

    Ok(())
}

async fn blocking_read_streams(
    database: &Database,
    stream_signals: &StreamSignals,
    keys: &[Vec<u8>],
    starts: &[StreamId],
    timeout_milliseconds: u64,
) -> Result<Option<Vec<StreamReadResult>>, ()> {
    let wait = wait_for_stream_entries(database, stream_signals, keys, starts);

    if timeout_milliseconds == 0 {
        return wait.await.map(Some);
    }

    let duration = Duration::from_millis(timeout_milliseconds);

    match tokio::time::timeout(duration, wait).await {
        Ok(Ok(streams)) => Ok(Some(streams)),

        Ok(Err(_)) => Err(()),

        Err(_) => Ok(None),
    }
}

//无线循环等待函数
async fn wait_for_stream_entries(
    database: &Database,
    stream_signals: &StreamSignals,
    keys: &[Vec<u8>],
    starts: &[StreamId],
) -> Result<Vec<StreamReadResult>, ()> {
    loop {
        let notified = stream_signals.notified();

        tokio::pin!(notified);

        notified.as_mut().enable();

        let streams = read_stream_entries(database, keys, starts).await?;

        if !streams.is_empty() {
            return Ok(streams);
        }

        notified.await;
    }
}

//查询多个Stream函数，将XREAD handler中的查询逻辑提取成函数
async fn read_stream_entries(
    database: &Database,
    keys: &[Vec<u8>],
    starts: &[StreamId],
) -> Result<Vec<StreamReadResult>, ()> {
    let mut db = database.lock().await;
    let now = Instant::now();

    let mut stream_results = Vec::new();

    for (key, start) in keys.iter().zip(starts.iter()) {
        let expired = db
            .get(key)
            .and_then(|entry| entry.expires_at)
            .is_some_and(|expires_at| now >= expires_at);

        if expired {
            db.remove(key);
            continue;
        }

        match db.get(key) {
            None => {}

            Some(Entry {
                value: RedisValue::Stream(entries),
                ..
            }) => {
                let selected: Vec<StreamEntry> = entries
                    .iter()
                    .filter(|entry| entry.id > *start)
                    .cloned()
                    .collect();

                if !selected.is_empty() {
                    stream_results.push(StreamReadResult {
                        key: key.to_vec(),
                        entries: selected,
                    });
                }
            }

            Some(Entry {
                value: RedisValue::String(_) | RedisValue::List(_),
                ..
            }) => {
                return Err(());
            }
        }
    }

    Ok(stream_results)
}
