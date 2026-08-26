#![allow(unused_imports)]

use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _address)) => {
                tokio::spawn(handle_client(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_client(stream: TcpStream) {
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
            let argument = &command[1];

            let header = format!("${}\r\n", argument.len());

            write_half.write_all(header.as_bytes()).await.unwrap();
            write_half.write_all(argument).await.unwrap();
            write_half.write_all(b"\r\n").await.unwrap();
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

fn parse_number(bytes: &[u8]) -> io::Result<usize> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_data("invalid number"))?;

    text.parse::<usize>()
        .map_err(|_| invalid_data("invalid number"))
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
