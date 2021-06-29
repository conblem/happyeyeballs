use std::future::Future;
use std::net::{SocketAddr, SocketAddrV6, SocketAddrV4};
use std::time::Duration;
use tokio::io;
use tokio::net::{lookup_host, TcpStream, ToSocketAddrs};

trait Inner<I, O>: FnOnce(I) -> O {}

impl<I, O, F> Inner<I, O> for F where F: FnOnce(I) -> O {}

pub async fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<TcpStream> {
    connect_higher(addr, lookup_host).await
}

fn partition<T: Iterator<Item = SocketAddr>>(ips: T) -> (impl Iterator<Item = SocketAddrV6>, impl Iterator<Item = SocketAddrV4>) {
    let (six, four): (Vec<SocketAddr>, Vec<SocketAddr>) =
        ips.partition(|addr| matches!(addr, SocketAddr::V6(_)));

    let six = six.into_iter().filter_map(| addr | match addr {
        SocketAddr::V6(six) => Some(six),
        SocketAddr::V4(_) => None,
    });

    let four = four.into_iter().filter_map(| addr | match addr {
        SocketAddr::V4(four) => Some(four),
        SocketAddr::V6(_) => None,
    });

    (six, four)
}

async fn connect_higher<I, F, T, L>(addr: T, lookup: L) -> io::Result<TcpStream>
where
    I: Iterator<Item = SocketAddr>,
    F: Future<Output = io::Result<I>>,
    L: FnOnce(T) -> F,
{
    let ips: I = lookup(addr).await?;
    let (mut six, mut four) = partition(ips);

    let (six, four) = match (six.next(), four.next()) {
        (None, Some(addr)) => {
            println!("only v4 address returned");
            return TcpStream::connect(addr).await;
        }
        (Some(addr), None) => {
            println!("only v6 address returned");
            return TcpStream::connect(addr).await;
        }
        (Some(six), Some(four)) => (six, four),
        (None, None) => return Err(io::ErrorKind::NotFound.into()),
    };

    let sleep = tokio::time::sleep(Duration::from_millis(250));
    tokio::pin!(sleep);
    let six_stream = TcpStream::connect(six);
    tokio::pin!(six_stream);

    let mut error = None;

    loop {
        tokio::select! {
                biased;
                stream = &mut six_stream => {
                    match stream {
                        Ok(stream) => {
                            println!("successfully connected to v6");
                            return Ok(stream)
                        }
                        Err(e) => {
                            error = Some(e);
                            break;
                        }
                    };
                }
                () = &mut sleep => {
                    println!("timeout connecting to v6");
                    break;
                }
            }
    }

    println!("i am here");

    tokio::select! {
            Ok(stream) = &mut six_stream, if error.is_none() => {
                return Ok(stream);
            }
            stream = TcpStream::connect(four) => {
                println!("successfully connected to v4");
                return stream;
            }
        }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use tokio::net::TcpListener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    async fn echo(mut stream: TcpStream) -> io::Result<()> {
        let mut buf = Vec::with_capacity(1000);
        loop {
            let n = stream.read(&mut buf[..]).await?;
            println!("read, {:?}", &buf[..n]);
            stream.write_all(&buf[..n]).await?;
        }
    }

    async fn server(listener: TcpListener) -> io::Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            tokio::spawn(echo(stream));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn it_works() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let (six, four): (Vec<SocketAddr>, Vec<SocketAddr>) = lookup_host("localhost:80")
            .await?
            .partition(|addr| matches!(addr, SocketAddr::V6(_)));

        if six.len() == 0 || four.len() == 0 {
            Err("IPv6 and IPv4 not both enabled on loopback")?
        }

        let listener_four = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener_four.local_addr()?.port();

        let listener_six = TcpListener::bind(format!("[::1]:{}", port)).await?;

        let server = |listener: TcpListener| async move {
            loop {
                listener.accept().await?;
            }
            Ok(()) as io::Result<()>
        };

        tokio::spawn(server(listener_four));
        tokio::spawn(server(listener_six));

        let mut stream = connect(format!("localhost:{}", port)).await?;

        let expected = b"hallo";
        let mut buf = Vec::with_capacity(expected.len());
        stream.write_all(expected);
        let n = stream.read_exact(&mut buf[..expected.len()]).await?;

        assert_eq!(expected.len(), n);
        assert_eq!(expected, &buf [..]);

        Ok(())
    }
}
