use std::fs::File;
use std::io;
use std::io::BufReader;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use crate::rustls::pki_types::{self, CertificateDer, PrivateKeyDer};
use anyhow::Result;
use rustls_pemfile::{certs, read_all};
use tokio::io::{copy, split, stdin as tokio_stdin, stdout as tokio_stdout, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{rustls, TlsConnector};

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:8080"
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
    let content = format!("GET / HTTP/1.0\r\nHost: {}\r\n\r\n", "occlum");

    // let mut root_cert_store = rustls::RootCertStore::empty();
    // let mut pem = BufReader::new(File::open("../tmp/rootCA.pem")?);
    // for cert in rustls_pemfile::certs(&mut pem) {
    //     root_cert_store.add(cert?).unwrap();
    // }

    // let certs = load_certs("./certificates/client.crt")?;
    // let key = load_keys("./certificates/client.key")?;

    // let config = rustls::ClientConfig::builder()
    //     .with_root_certificates(root_cert_store)
    //     .with_client_auth_cert(certs, key)?;
    // let connector = TlsConnector::from(Arc::new(config));

    let mut stream = TcpStream::connect(&addr).await?;

    let (mut stdin, mut stdout) = (tokio_stdin(), tokio_stdout());

    // let domain = pki_types::ServerName::try_from("127.0.0.1")
    //     .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid dnsname"))?
    //     .to_owned();

    // let mut stream = connector.connect(domain, stream).await?;

    stream.write_all(content.as_bytes()).await?;

    let (mut reader, mut writer) = split(stream);

    tokio::select! {
        ret = copy(&mut reader, &mut stdout) => {
            ret?;
        },
        ret = copy(&mut stdin, &mut writer) => {
            ret?;
            writer.shutdown().await?
        }
    }

    Ok(())
}

fn load_certs(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    certs(&mut BufReader::new(File::open(path)?)).collect()
}

fn load_keys(path: &str) -> Result<PrivateKeyDer<'static>> {
    for item in read_all(&mut BufReader::new(File::open(path)?)) {
        match item.unwrap() {
            rustls_pemfile::Item::Pkcs1Key(key) => return Ok(key.into()),
            rustls_pemfile::Item::Pkcs8Key(key) => return Ok(key.into()),
            rustls_pemfile::Item::Sec1Key(key) => return Ok(key.into()),
            _ => return Err(anyhow::anyhow!("invalid key")),
        }
    }
    Err(anyhow::anyhow!("there is no key"))
}
