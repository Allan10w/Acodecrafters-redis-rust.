use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

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

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let database: Database = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, _address)) => {
                let database = Arc::clone(&database);
                tokio::spawn(handle_client(stream, database));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_client(stream: TcpStream, database: Database) {
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
                    write_integer(&mut write_half, length).await.unwrap();
                }
                Err(()) => {
                    write_half
                        .write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",)
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
