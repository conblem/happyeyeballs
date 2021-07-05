use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io;
use tokio::net::{lookup_host, TcpStream, ToSocketAddrs};
use tracing::field;
use tracing::{trace, trace_span, Instrument, Span};

fn partition<T: Iterator<Item = SocketAddr>>(ips: T) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    let (six, four): (Vec<SocketAddr>, Vec<SocketAddr>) =
        ips.partition(|ip| matches!(ip, SocketAddr::V6(_)));

    (six, four)
}

// we use a higher order function to inject the dns resolver for future flexibility
async fn connect_higher<I, F, T, L>(addr: T, lookup: L) -> io::Result<TcpStream>
where
    I: Iterator<Item = SocketAddr>,
    F: Future<Output = io::Result<I>>,
    L: FnOnce(T) -> F,
{
    let span = Span::current();
    let (mut six, mut four) = lookup(addr).await.map(partition)?;

    // if dns lookup does not return both an ipv4 / ipv6 record skip happy eyeballs logic
    let (six, four) = match (six.pop(), four.pop()) {
        (None, Some(addr)) => {
            span.record("ipv4", &field::display(&addr));
            trace!("only ipv4");
            return TcpStream::connect(addr).await;
        }
        (Some(addr), None) => {
            span.record("ipv6", &field::display(&addr));
            trace!("only ipv6");
            return TcpStream::connect(addr).await;
        }
        (Some(six), Some(four)) => (six, four),
        _ => return Err(io::ErrorKind::NotFound.into()),
    };

    span.record("ipv6", &field::display(&six));
    span.record("ipv4", &field::display(&four));
    let connect_six = TcpStream::connect(six);
    // pin future on the stack so we repoll it after timeout while ipv4 is connecting
    tokio::pin!(connect_six);

    let timeout = tokio::time::timeout(Duration::from_millis(250), &mut connect_six);

    let err = match timeout.await {
        Ok(Ok(stream)) => {
            trace!("ipv6");
            return Ok(stream);
        }
        // if ipv6 already returned an error safe it so we know not the poll the ipv6 future again
        Ok(Err(e)) => {
            trace!("ipv6 connection errored");
            Some(e)
        }
        Err(_elapsed) => {
            trace!("ipv6 connection timedout");
            None
        }
    };

    tokio::select! {
        // biased select as we want to try connecting to ipv4 first
        biased;
        stream = TcpStream::connect(four) => {
            trace!("ipv4");
            return stream;
        }
        // if ipv6 did not already return an error we still try to poll it
        // in case it connects before ipv4 returns a connection
        Ok(stream) = &mut connect_six, if err.is_none() => {
            trace!("ipv6 connected after ipv4");
            return Ok(stream);
        }
    }
}

pub async fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<TcpStream> {
    let span = trace_span!("happyeyeballs", ipv4 = field::Empty, ipv6 = field::Empty);

    connect_higher(addr, lookup_host).instrument(span).await
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tracing_test::traced_test;

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
    #[traced_test]
    async fn it_works() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        // check if ipv6 and ipv4 are both enabled on loopback interface
        let (six, four) = lookup_host("localhost:80").await.map(partition)?;

        if six.is_empty() || four.is_empty() {
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

        // connect to to localhost using happyeyeballs
        let mut stream = connect(format!("localhost:{}", port)).await?;

        // test if connection actually works, probably pretty useless
        let expected = b"hallo";
        stream.write_all(expected).await?;

        let mut buf = vec![0; expected.len()].into_boxed_slice();
        let n = stream.read_exact(&mut buf[..]).await?;

        assert_eq!(expected.len(), n);
        assert_eq!(expected, &buf[..n]);

        Ok(())
    }
}
