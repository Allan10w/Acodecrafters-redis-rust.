use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

// 多个客户端任务共享的内存键值数据库。
type Database = Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>;

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
        } else if command.len() == 3 && command[0].eq_ignore_ascii_case(b"SET") {
            {
                let mut db = database.lock().await;

                db.insert(command[1].clone(), command[2].clone());
            }
            write_half.write_all(b"+OK\r\n").await.unwrap();
        } else if command.len() == 2 && command[0].eq_ignore_ascii_case(b"GET") {
            let value = {
                let db = database.lock().await;
                db.get(&command[1]).cloned()
            };

            match value {
                Some(value) => {
                    write_bulk_string(&mut write_half, &value).await.unwrap();
                }
                None => {
                    write_half.write_all(b"$-1\r\n").await.unwrap();
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

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
