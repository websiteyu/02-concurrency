use std::io;
use std::net::SocketAddr;

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, level_filters::LevelFilter, warn};
use tracing_subscriber::EnvFilter;

const BUF_SIZE: usize = 4096;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("Dredis: listening on: {}", addr);

    loop {
        let (stream, raddr) = listener.accept().await?;
        info!("Accepted connector from: {}", raddr);
        tokio::spawn(async move {
            if let Err(e) = process_redis_conn(stream, raddr).await {
                warn!("Error processing conn with {}: {:?}", raddr, e);
            }
        });
    }
}

async fn process_redis_conn(mut stream: TcpStream, raddr: SocketAddr) -> Result<()> {
    loop {
        stream.readable().await?;
        let mut buf = Vec::with_capacity(BUF_SIZE);
        match stream.try_read_buf(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                info!("Read {} bytes", n);
                let line = String::from_utf8_lossy(&buf);
                info!("{}", line);
                stream.write_all(b"+OK\r\n").await?;
            }
            Err(ref e) if (e.kind() == io::ErrorKind::WouldBlock) => continue,
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    warn!("Connection {} closed", raddr);
    Ok(())
}
