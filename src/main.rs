#![allow(unused_imports)]

use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

async fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 512];

    loop {
        let bytes_read = match stream.read(&mut buf).await {
            Ok(bytes_read) => bytes_read,
            Err(e) => {
                println!("error reading client: {}", e);
                break;
            }
        };

        if bytes_read == 0 {
            break;
        }

        if let Err(e) = stream.write_all(b"+PONG\r\n").await {
            println!("error writing response: {}", e);
            break;
        }
    }
}
