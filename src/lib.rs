use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;
use tokio::io;
use tokio::net::{lookup_host, TcpStream, ToSocketAddrs};

fn partition<T: Iterator<Item = SocketAddr>>(
    ips: T,
) -> (
    impl Iterator<Item = SocketAddrV6>,
    impl Iterator<Item = SocketAddrV4>,
) {
    let (six, four): (Vec<SocketAddr>, Vec<SocketAddr>) =
        ips.partition(|addr| matches!(addr, SocketAddr::V6(_)));

    let six = six.into_iter().filter_map(|addr| match addr {
        SocketAddr::V6(six) => Some(six),
        SocketAddr::V4(_) => None,
    });

    let four = four.into_iter().filter_map(|addr| match addr {
        SocketAddr::V4(four) => Some(four),
        SocketAddr::V6(_) => None,
    });

    (six, four)
}

// we use a higher order function to inject the dns resolver for future flexibility
async fn connect_higher<I, F, T, L>(addr: T, lookup: L) -> io::Result<TcpStream>
where
    I: Iterator<Item = SocketAddr>,
    F: Future<Output = io::Result<I>>,
    L: FnOnce(T) -> F,
{
    let ips: I = lookup(addr).await?;
    let (mut six, mut four) = partition(ips);

    // if dns lookup does not return both an ipv4 / ipv6 record skip happy eyeballs logic
    let (six, four) = match (six.next(), four.next()) {
        (None, Some(addr)) => {
            println!("only v4 address returned");
            return TcpStream::connect(addr).await;
        }
        (Some(addr), None) => {
            println!("only v6 address returned");
            return TcpStream::connect(addr).await;
        }
        // both adress types returned
        (Some(six), Some(four)) => (six, four),
        (None, None) => return Err(io::ErrorKind::NotFound.into()),
    };

    let connect_six = TcpStream::connect(six);
    // pin future on the stack so we repoll it after timeout while ipv4 is connecting
    tokio::pin!(connect_six);

    let timeout = tokio::time::timeout(Duration::from_millis(250), &mut connect_six);

    let err = match timeout.await {
        Ok(Ok(stream)) => return Ok(stream),
        // if ipv6 already returned an error safe it so we know not the poll the ipv6 future again
        Ok(Err(e)) => Some(e),
        Err(_elapsed) => None,
    };

    tokio::select! {
        // biased select as we want to try connecting to ipv4 first
        biased;
        stream = TcpStream::connect(four) => {
            println!("successfully connected to v4");
            return stream;
        }
        // if ipv6 did not already return an error we still try to poll it
        // in case it connects before ipv4 returns a connection
        Ok(stream) = &mut connect_six, if err.is_none() => {
            return Ok(stream);
        }
    }
}

pub async fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<TcpStream> {
    connect_higher(addr, lookup_host).await
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    async fn echo(mut stream: TcpStream) -> io::Result<()> {
        let mut buf = vec![0; 256].into_boxed_slice();
        loop {
            let n = stream.read(&mut buf[..]).await?;
            stream.write_all(&buf[..n]).await?;
        }
    }

    async fn server(listener: TcpListener) -> io::Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            tokio::spawn(echo(stream));
        }
    }

    #[tokio::test]
    async fn it_works() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        // check if ipv6 and ipv4 are both enabled on loopback interface
        let (mut six, mut four) = lookup_host("localhost:80").await.map(partition)?;

        if six.next().is_none() || four.next().is_none() {
            Err("Both IPv6 and IPv4 need to be enabled on loopback interface")?
        }

        // create an ipv4 listener on a random free port
        let listener_four = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener_four.local_addr()?.port();

        // try to bind ipv6 listener on same random port
        let listener_six = TcpListener::bind(format!("[::1]:{}", port)).await?;

        // spawn echo tcp servers
        tokio::spawn(server(listener_four));
        tokio::spawn(server(listener_six));

        // connecto to localhost using happyeyeballs
        let mut stream = connect(format!("localhost:{}", port)).await?;

        // test if connection actually works, probably pretty useless
        let expected = b"hallo";
        stream.write_all(expected).await?;

        let mut buf = vec![0; expected.len()].into_boxed_slice();
        let n = stream.read_exact(&mut buf[..expected.len()]).await?;

        assert_eq!(expected.len(), n);
        assert_eq!(expected, &buf[..n]);

        Ok(())
    }
}
