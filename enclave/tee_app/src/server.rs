use crate::graph::EncryptedGraph;

use anyhow::Result;
use dotenv::dotenv;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::WebPkiClientVerifier;
use rustls::{DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme};
use rustls_pemfile::{certs, read_all};
use simple_cypher::*;
use tokio::io::{copy, sink, split, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::{rustls, TlsAcceptor};

use std::env;
use std::fs::File;
use std::io::{self, BufReader};
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub async fn start_server() -> Result<()> {
    dotenv().ok();

    let uri = env::var("DATABASE_URI").expect("DATABASE_URI must be set");
    let user = env::var("DATABASE_USERNAME").expect("DATABASE_USERNAME must be set");
    let pass = env::var("DATABASE_PASSWORD").expect("DATABASE_PASSWORD must be set");

    let graph = Arc::new(EncryptedGraph::new(uri, user, pass).await?);

    // test_crud(&graph).await.unwrap();
    // test_find_shortest_path(&graph).await.unwrap();

    let addr = "127.0.0.1:8080"
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))?;

    // let certs = load_certs("/server.crt")?;
    // let key = load_keys("/server.key")?;
    // let client_certs = load_certs("/rootCA.pem")?;

    // let mut roots = RootCertStore::empty();
    // for client_cert in client_certs {
    //     roots.add(client_cert);
    // }

    // let inner = WebPkiClientVerifier::builder(roots.into()).build()?;
    // let verifier = MyClientCertVerifier::new(inner).boxed();

    // let server_config = rustls::ServerConfig::builder()
    //     .with_client_cert_verifier(verifier)
    //     .with_single_cert(certs, key)?;
    // let acceptor = TlsAcceptor::from(Arc::new(server_config));

    log::info!("bind addr: {}", addr);

    let listener = TcpListener::bind(&addr).await?;

    log::info!("start listening...");

    loop {
        let (mut stream, peer_addr) = listener.accept().await?;
        log::info!("accept");

        // let acceptor = acceptor.clone();

        let fut = async move {
            // let mut stream = acceptor.accept(stream).await?;

            let mut buf = [0; 4096];
            let n = stream.read(&mut buf).await?;
            println!("The bytes: {:?}", &buf[..n]);

            // let mut serialized_query = String::new();
            // stream.read_to_string(&mut serialized_query)?;

            // let query = CypherQuery::deserialize(serialized_query)?;
            // let result = graph.clone().execute_query(query).await?;

            Ok(()) as Result<()>
        };

        tokio::spawn(async move {
            if let Err(err) = fut.await {
                eprintln!("{:?}", err);
            }
        });
    }

    println!("!!!!!!!!!");

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

#[derive(Debug)]
struct MyClientCertVerifier {
    inner: Arc<dyn ClientCertVerifier>,
}

impl MyClientCertVerifier {
    pub fn new(inner: Arc<dyn ClientCertVerifier>) -> Self {
        Self { inner }
    }

    /// Wrap this verifier in an [`Arc`] and coerce it to `dyn ClientCertVerifier`
    #[inline(always)]
    pub fn boxed(self) -> Arc<dyn ClientCertVerifier> {
        // This function is needed to keep it functioning like the original verifier.
        Arc::new(self)
    }
}

impl ClientCertVerifier for MyClientCertVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.inner
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

async fn test_crud(graph: &EncryptedGraph) -> Result<()> {
    {
        let query = CypherQueryBuilder::new()
            .CREATE()
            .node(Node::new(
                Some("a"),
                vec!["label1"],
                vec![("k", "v1"), ("k1", "vv1"), ("k2", "vvv1")],
            ))
            .RETURN(vec![Item::Var(String::from("a"))])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .CREATE()
            .node(Node::new(
                Some("n"),
                vec!["label1"],
                vec![("k", "v2"), ("k1", "vv2"), ("k2", "vvv2")],
            ))
            .relation(Relation::new(
                Some("r"),
                vec!["knows"],
                vec![("rk", "v2v3"), ("rk1", "v2v31")],
            ))
            .next_node(Node::new(
                Some("m"),
                vec!["label1"],
                vec![("k", "v3"), ("k1", "vv3"), ("k2", "vvv3")],
            ))
            .RETURN(vec![
                Item::Var(String::from("n")),
                Item::Var(String::from("r")),
                Item::Var(String::from("m")),
            ])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["label1"], vec![("k", "v1")]))
            .relation(Relation::new(
                Some("r"),
                vec!["like"],
                vec![("rk", "v1v3"), ("rk1", "v1v31")],
            ))
            .next_node(Node::new(
                Some("m"),
                vec!["label1"],
                vec![("k", "v3"), ("k1", "vv3")],
            ))
            .CREATE()
            .RETURN(vec![
                Item::Var(String::from("n")),
                Item::Var(String::from("r")),
                Item::Var(String::from("m")),
            ])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["label1"],
                Vec::<(String, String)>::new(),
            ))
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["label1"], vec![("k", "v1")]))
            .relation(Relation::new(
                Some("r"),
                vec!["like"],
                vec![("rk", "v1v3"), ("rk1", "v1v31")],
            ))
            .next_node(Node::new(
                Some("m"),
                vec!["label1"],
                vec![("k", "v3"), ("k1", "vv3")],
            ))
            .RETURN(vec![
                Item::Var(String::from("n")),
                Item::Var(String::from("r")),
                Item::Var(String::from("m")),
            ])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["label1"],
                Vec::<(String, String)>::new(),
            ))
            .SET(vec![
                Item::VarWithLabel(String::from("n"), String::from("label2")),
                Item::VarWithKeyValue(
                    String::from("n"),
                    String::from("new_k"),
                    String::from("new_v"),
                ),
            ])
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("x"),
                vec!["label1"],
                Vec::<(String, String)>::new(),
            ))
            .relation(Relation::new(
                Some("xy"),
                Vec::<String>::new(),
                Vec::<(String, String)>::new(),
            ))
            .next_node(Node::new(
                Some("y"),
                vec!["label1"],
                Vec::<(String, String)>::new(),
            ))
            .SET(vec![
                Item::VarWithLabel(String::from("x"), String::from("label3")),
                Item::VarWithKeyValue(
                    String::from("x"),
                    String::from("new_k"),
                    String::from("new_vv"),
                ),
                Item::VarWithKeyValue(
                    String::from("y"),
                    String::from("new_k2"),
                    String::from("new_v"),
                ),
                Item::VarWithKeyValue(
                    String::from("xy"),
                    String::from("new_k"),
                    String::from("new_v"),
                ),
            ])
            .RETURN(vec![
                Item::Var(String::from("x")),
                Item::Var(String::from("xy")),
                Item::Var(String::from("y")),
            ])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["label3"],
                vec![("new_k", "new_vv")],
            ))
            .REMOVE(vec![
                Item::VarWithLabel(String::from("n"), String::from("label2")),
                Item::VarWithKey(String::from("n"), String::from("new_k")),
            ])
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("x"),
                vec!["label1"],
                Vec::<(String, String)>::new(),
            ))
            .relation(Relation::new(
                Some("xy"),
                vec!["like"],
                vec![("new_k", "new_v")],
            ))
            .next_node(Node::new(
                Some("y"),
                vec!["label1"],
                Vec::<(String, String)>::new(),
            ))
            .REMOVE(vec![
                Item::VarWithLabel(String::from("x"), String::from("label3")),
                Item::VarWithKey(String::from("x"), String::from("new_k")),
                Item::VarWithKey(String::from("y"), String::from("k2")),
                Item::VarWithKey(String::from("xy"), String::from("rk1")),
            ])
            .RETURN(vec![
                Item::Var(String::from("x")),
                Item::Var(String::from("xy")),
                Item::Var(String::from("y")),
            ])
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["label1"], vec![("k", "v1")]))
            .relation(Relation::new(
                Some("r"),
                Vec::<String>::new(),
                Vec::<(String, String)>::new(),
            ))
            .next_node(Node::new(
                None::<String>,
                Vec::<String>::new(),
                Vec::<(String, String)>::new(),
            ))
            .DELETE(
                vec![Item::Var(String::from("n")), Item::Var(String::from("r"))],
                false,
            )
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["label1"],
                Vec::<(String, String)>::new(),
            ))
            .DELETE(vec![Item::Var(String::from("n"))], true)
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }

    Ok(())
}

async fn test_find_shortest_path(graph: &EncryptedGraph) -> Result<()> {
    //            c --> d
    //            ⬆     ⬇
    //      a --> b --> e --> f
    //                  ⬇     ⬇
    //                  h <-- g
    for node_name in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        let query = CypherQueryBuilder::new()
            .CREATE()
            .node(Node::new(
                None::<String>,
                vec!["Person"],
                vec![("name", node_name)],
            ))
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }

    //            c --> d
    //            ⬆     ⬇
    //      a --> b --> e --> f
    //                  ⬇     ⬇
    //                  h <-- g
    for (from, to) in [
        ("a", "b"),
        ("b", "c"),
        ("b", "e"),
        ("c", "d"),
        ("d", "e"),
        ("e", "f"),
        ("f", "g"),
        ("e", "h"),
        ("g", "h"),
    ] {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["Person"], vec![("name", from)]))
            .relation(Relation::new(
                Some("r"),
                vec!["knows"],
                Vec::<(String, String)>::new(),
            ))
            .next_node(Node::new(Some("m"), vec!["Person"], vec![("name", to)]))
            .CREATE()
            .build();
        let result = graph
            .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
            .await
            .unwrap();
        println!("{}\n{:?}", query.to_query_string()?, result);
    }

    let query = CypherQueryBuilder::new()
        .node(Node::new(
            Some("start"),
            vec!["Person"],
            vec![("name", "a")],
        ))
        .next_node(Node::new(Some("dest"), vec!["Person"], vec![("name", "g")]))
        .find_shortest_path()
        .build();
    let result = graph
        .execute_query(CypherQuery::deserialize(&query.serialize()?)?)
        .await
        .unwrap();

    let path: Vec<&String> = result.rows()[0]
        .inners()
        .iter()
        .map(|x| x.get("name").unwrap())
        .collect();
    assert_eq!(path, vec!["a", "b", "e", "f", "g"]);
    println!("{}\n{:?}", query.to_query_string()?, result);

    Ok(())
}
