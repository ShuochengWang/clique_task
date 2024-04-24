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
        let cloned_graph = graph.clone();
        let (mut stream, _peer_addr) = listener.accept().await?;
        log::info!("accept");

        // let acceptor = acceptor.clone();

        let fut = async move {
            // let mut stream = acceptor.accept(stream).await?;

            loop {
                let len = stream.read_u64().await? as usize;
                log::trace!("read {}", len);
                // todo: limit max len
                let mut buf = vec![0u8; len];
                stream.read_exact(&mut buf).await?;
                log::trace!("read {} bytes", len);

                let serialized_query = String::from_utf8(buf)?;
                let query = CypherQuery::deserialize(&serialized_query)?;
                log::trace!("query: {:?}", query);
                let result = cloned_graph.execute_query(query).await?;
                log::trace!("execute result: {:?}", result);

                let text = result.serialize()?;
                stream.write_u64(text.len() as u64).await?;
                log::trace!("write {}", text.len());
                stream.write_all(text.as_bytes()).await?;
                log::trace!("write {} bytes", text.len());
            }

            Ok(()) as Result<()>
        };

        tokio::spawn(async move {
            if let Err(err) = fut.await {
                eprintln!("{:?}", err);
            }
        });
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

async fn init_test(graph: &EncryptedGraph) -> Result<()> {
    let query = CypherQueryBuilder::new()
        .MATCH()
        .node(Node::new(
            Some("n"),
            Vec::<String>::new(),
            Vec::<(String, String)>::new(),
        ))
        .DELETE(vec![Item::Var(String::from("n"))], true)
        .build();

    graph.execute_query(query).await?;

    Ok(())
}

async fn test_crud(graph: &EncryptedGraph) -> Result<()> {
    init_test(graph).await?;

    {
        let query = CypherQueryBuilder::new()
            .CREATE()
            .node(Node::new(
                Some("a"),
                vec!["Student"],
                vec![("name", "Alice"), ("age", "25")],
            ))
            .RETURN(vec![Item::Var(String::from("a"))])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(
            result.rows()[0].inners()[0],
            Inner::new(
                vec!["Student".to_string()],
                vec![
                    ("name".to_string(), "Alice".to_string()),
                    ("age".to_string(), "25".to_string())
                ]
            )
        );
    }
    {
        let query = CypherQueryBuilder::new()
            .CREATE()
            .node(Node::new(
                Some("n"),
                vec!["Student"],
                vec![("name", "Bob"), ("age", "23"), ("home", "beijing")],
            ))
            .relation(Relation::new(
                Some("r"),
                vec!["Knows"],
                vec![("time", "1year")],
            ))
            .next_node(Node::new(
                Some("m"),
                vec!["Student"],
                vec![("name", "John"), ("age", "24"), ("home", "jiangxi")],
            ))
            .RETURN(vec![
                Item::Var(String::from("n")),
                Item::Var(String::from("r")),
                Item::Var(String::from("m")),
            ])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(
            result.rows()[0].inners()[0],
            Inner::new(
                vec!["Student".to_string()],
                vec![
                    ("name".to_string(), "Bob".to_string()),
                    ("age".to_string(), "23".to_string()),
                    ("home".to_string(), "beijing".to_string())
                ]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[1],
            Inner::new(
                vec!["Knows".to_string()],
                vec![("time".to_string(), "1year".to_string())]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[2],
            Inner::new(
                vec!["Student".to_string()],
                vec![
                    ("name".to_string(), "John".to_string()),
                    ("age".to_string(), "24".to_string()),
                    ("home".to_string(), "jiangxi".to_string())
                ]
            )
        );
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["Student"],
                vec![("name", "Alice")],
            ))
            .relation(Relation::new(
                Some("r"),
                vec!["Like"],
                vec![("time", "1month"), ("public", "yes")],
            ))
            .next_node(Node::new(
                Some("m"),
                vec!["Student"],
                vec![("name", "John"), ("age", "24")],
            ))
            .CREATE()
            .RETURN(vec![
                Item::Var(String::from("n")),
                Item::Var(String::from("r")),
                Item::Var(String::from("m")),
            ])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(
            result.rows()[0].inners()[0],
            Inner::new(
                vec!["Student".to_string()],
                vec![
                    ("name".to_string(), "Alice".to_string()),
                    ("age".to_string(), "25".to_string())
                ]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[1],
            Inner::new(
                vec!["Like".to_string()],
                vec![
                    ("time".to_string(), "1month".to_string()),
                    ("public".to_string(), "yes".to_string())
                ]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[2],
            Inner::new(
                vec!["Student".to_string()],
                vec![
                    ("name".to_string(), "John".to_string()),
                    ("age".to_string(), "24".to_string()),
                    ("home".to_string(), "jiangxi".to_string())
                ]
            )
        );
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["Student"],
                Vec::<(String, String)>::new(),
            ))
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(result.rows().len(), 3);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["Student"], vec![("age", "25")]))
            .relation(Relation::new(
                Some("r"),
                vec!["Like"],
                vec![("time", "1month")],
            ))
            .next_node(Node::new(
                Some("m"),
                vec!["Student"],
                vec![("home", "jiangxi"), ("age", "24")],
            ))
            .RETURN(vec![
                Item::Var(String::from("n")),
                Item::Var(String::from("r")),
                Item::Var(String::from("m")),
            ])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(
            result.rows()[0].inners()[0],
            Inner::new(
                vec!["Student".to_string()],
                vec![
                    ("name".to_string(), "Alice".to_string()),
                    ("age".to_string(), "25".to_string())
                ]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[1],
            Inner::new(
                vec!["Like".to_string()],
                vec![
                    ("time".to_string(), "1month".to_string()),
                    ("public".to_string(), "yes".to_string())
                ]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[2],
            Inner::new(
                vec!["Student".to_string()],
                vec![
                    ("name".to_string(), "John".to_string()),
                    ("age".to_string(), "24".to_string()),
                    ("home".to_string(), "jiangxi".to_string())
                ]
            )
        );
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["Student"],
                Vec::<(String, String)>::new(),
            ))
            .SET(vec![
                Item::VarWithLabel(String::from("n"), String::from("Undergraduate")),
                Item::VarWithKeyValue(
                    String::from("n"),
                    String::from("univ"),
                    String::from("Nanjing univ"),
                ),
            ])
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(result.rows().len(), 3);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("x"),
                vec!["Student"],
                Vec::<(String, String)>::new(),
            ))
            .relation(Relation::new(
                Some("xy"),
                vec!["Knows"],
                Vec::<(String, String)>::new(),
            ))
            .next_node(Node::new(
                Some("y"),
                vec!["Student"],
                Vec::<(String, String)>::new(),
            ))
            .SET(vec![
                Item::VarWithLabel(String::from("x"), String::from("Intern")),
                Item::VarWithKeyValue(
                    String::from("x"),
                    String::from("location"),
                    String::from("nanjing"),
                ),
                Item::VarWithKeyValue(
                    String::from("y"),
                    String::from("location"),
                    String::from("nanjing"),
                ),
                Item::VarWithKeyValue(
                    String::from("xy"),
                    String::from("level"),
                    String::from("mid"),
                ),
            ])
            .RETURN(vec![
                Item::Var(String::from("x")),
                Item::Var(String::from("xy")),
                Item::Var(String::from("y")),
            ])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(
            result.rows()[0].inners()[0],
            Inner::new(
                vec![
                    "Student".to_string(),
                    "Intern".to_string(),
                    "Undergraduate".to_string()
                ],
                vec![
                    ("name".to_string(), "Bob".to_string()),
                    ("age".to_string(), "23".to_string()),
                    ("home".to_string(), "beijing".to_string()),
                    ("univ".to_string(), "Nanjing univ".to_string()),
                    ("location".to_string(), "nanjing".to_string())
                ]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[1],
            Inner::new(
                vec!["Knows".to_string()],
                vec![
                    ("time".to_string(), "1year".to_string()),
                    ("level".to_string(), "mid".to_string())
                ]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[2],
            Inner::new(
                vec!["Student".to_string(), "Undergraduate".to_string()],
                vec![
                    ("name".to_string(), "John".to_string()),
                    ("age".to_string(), "24".to_string()),
                    ("home".to_string(), "jiangxi".to_string()),
                    ("univ".to_string(), "Nanjing univ".to_string()),
                    ("location".to_string(), "nanjing".to_string())
                ]
            )
        );
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["Intern"],
                vec![("location", "nanjing")],
            ))
            .REMOVE(vec![
                Item::VarWithLabel(String::from("n"), String::from("Intern")),
                Item::VarWithKey(String::from("n"), String::from("location")),
            ])
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(
            result.rows()[0].inners()[0],
            Inner::new(
                vec!["Student".to_string(), "Undergraduate".to_string()],
                vec![
                    ("name".to_string(), "Bob".to_string()),
                    ("age".to_string(), "23".to_string()),
                    ("home".to_string(), "beijing".to_string()),
                    ("univ".to_string(), "Nanjing univ".to_string()),
                ]
            )
        );
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("x"),
                vec!["Student"],
                Vec::<(String, String)>::new(),
            ))
            .relation(Relation::new(
                Some("xy"),
                vec!["Like"],
                vec![("time", "1month")],
            ))
            .next_node(Node::new(
                Some("y"),
                vec!["Student"],
                Vec::<(String, String)>::new(),
            ))
            .REMOVE(vec![
                Item::VarWithLabel(String::from("x"), String::from("Undergraduate")),
                Item::VarWithKey(String::from("x"), String::from("age")),
                Item::VarWithKey(String::from("y"), String::from("age")),
                Item::VarWithKey(String::from("xy"), String::from("public")),
            ])
            .RETURN(vec![
                Item::Var(String::from("x")),
                Item::Var(String::from("xy")),
                Item::Var(String::from("y")),
            ])
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);

        assert_eq!(
            result.rows()[0].inners()[0],
            Inner::new(
                vec!["Student".to_string()],
                vec![
                    ("name".to_string(), "Alice".to_string()),
                    ("univ".to_string(), "Nanjing univ".to_string())
                ]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[1],
            Inner::new(
                vec!["Like".to_string()],
                vec![("time".to_string(), "1month".to_string())]
            )
        );
        assert_eq!(
            result.rows()[0].inners()[2],
            Inner::new(
                vec!["Student".to_string(), "Undergraduate".to_string()],
                vec![
                    ("name".to_string(), "John".to_string()),
                    ("home".to_string(), "jiangxi".to_string()),
                    ("univ".to_string(), "Nanjing univ".to_string()),
                    ("location".to_string(), "nanjing".to_string())
                ]
            )
        );
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["Student"],
                vec![("name", "Alice")],
            ))
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

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);
    }
    {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["Student"],
                Vec::<(String, String)>::new(),
            ))
            .DELETE(vec![Item::Var(String::from("n"))], true)
            .build();

        println!("{}", query.to_query_string()?);
        let result = graph.execute_query(query).await.unwrap();
        println!("    {:?}", result);
    }

    Ok(())
}

async fn test_find_shortest_path(graph: &EncryptedGraph) -> Result<()> {
    init_test(graph).await?;

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
