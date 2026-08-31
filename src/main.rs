use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};

enum RedisValue {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
}
struct Entry {
    value: RedisValue,
    expires_at: Option<Instant>,
}

// 多个客户端任务共享的内存键值数据库。
type Database = Arc<Mutex<HashMap<Vec<u8>, Entry>>>;
//通知器：负责告诉BLPOP某个列表可能有新的元素，可以重新检查数据库
type ListSignals = Arc<Mutex<HashMap<Vec<u8>, Arc<Notify>>>>;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let database: Database = Arc::new(Mutex::new(HashMap::new()));
    let list_signals: ListSignals = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, _address)) => {
                let database = Arc::clone(&database);
                let list_signals = Arc::clone(&list_signals);

                tokio::spawn(handle_client(stream, database, list_signals));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_client(stream: TcpStream, database: Database, list_signals: ListSignals) {
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
                    RedisValue::String(_) => Err(()),
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
                            value: RedisValue::String(_),
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
                        write_half.write_all(b"$-1\r\n").await.unwrap();
                    } else {
                        //不带count时返回Null Bulk String
                        write_half.write_all(b"$-1\r\n").await.unwrap();
                    }
                }

                Err(()) => {
                    write_half.write_all(b"-WRPMHTYPE Operation against a key holding the wrong kind of value\r\n",).await.unwrap();
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
                    RedisValue::String(_) => Err(()),
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
                            value: RedisValue::String(_),
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
                            value: RedisValue::List(_),
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
            value: RedisValue::String(_),
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
