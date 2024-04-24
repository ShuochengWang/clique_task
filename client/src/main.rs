use std::fs::File;
use std::io;
use std::io::BufReader;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use crate::rustls::pki_types::{self, CertificateDer, PrivateKeyDer};
use anyhow::Result;
use rustls_pemfile::{certs, read_all};
use simple_cypher::*;
use tokio::io::{
    copy, split, stdin as tokio_stdin, stdout as tokio_stdout, AsyncReadExt, AsyncWriteExt,
};
use tokio::net::TcpStream;
use tokio_rustls::{rustls, TlsConnector};

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:8080"
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;

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

    // let domain = pki_types::ServerName::try_from("127.0.0.1")
    //     .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid dnsname"))?
    //     .to_owned();

    // let mut stream = connector.connect(domain, stream).await?;

    test_crud(&mut stream).await.unwrap();
    test_find_shortest_path(&mut stream).await.unwrap();

    stream.shutdown().await?;

    Ok(())
}

async fn execute_query(query: CypherQuery, stream: &mut TcpStream) -> Result<Rows> {
    let serialized_query = query.serialize()?;
    stream.write_u64(serialized_query.len() as u64).await?;
    // println!("write {}", serialized_query.len());
    stream.write_all(serialized_query.as_bytes()).await?;
    // println!("write {} bytes", serialized_query.len());

    let len = stream.read_u64().await? as usize;
    // println!("read {}", len);
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    // println!("read {} bytes", len);
    let serialized_result = String::from_utf8(buf)?;
    Rows::deserialize(&serialized_result)
}

async fn init_test(stream: &mut TcpStream) -> Result<()> {
    let query = CypherQueryBuilder::new()
        .MATCH()
        .node(Node::new(
            Some("n"),
            Vec::<String>::new(),
            Vec::<(String, String)>::new(),
        ))
        .DELETE(vec![Item::Var(String::from("n"))], true)
        .build();

    execute_query(query, stream).await?;

    Ok(())
}

async fn test_crud(stream: &mut TcpStream) -> Result<()> {
    init_test(stream).await?;

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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
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
        let result = execute_query(query, stream).await.unwrap();
        println!("    {:?}", result);
    }

    Ok(())
}

async fn test_find_shortest_path(stream: &mut TcpStream) -> Result<()> {
    init_test(stream).await?;

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
        println!("{}", query.to_query_string()?);
        let result = execute_query(query, stream).await.unwrap();
        println!("    {:?}", result);
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
        println!("{}", query.to_query_string()?);
        let result = execute_query(query, stream).await.unwrap();
        println!("    {:?}", result);
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
    println!("{}", query.to_query_string()?);
    let result = execute_query(query, stream).await.unwrap();

    let path: Vec<&String> = result.rows()[0]
        .inners()
        .iter()
        .map(|x| x.get("name").unwrap())
        .collect();
    assert_eq!(path, vec!["a", "b", "e", "f", "g"]);

    println!("    {:?}", result);

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
