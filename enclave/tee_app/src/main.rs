mod crypto;
mod graph;
mod server;

use anyhow::Result;
// use dotenv::dotenv;
// use neo4rs::*;

// use std::env;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    logger::init();

    server::start_server().await;

    // let plaintext = b"Example plaintext.";
    // let crypto = crypto::Crypto::new();
    // println!("{:?}", plaintext);
    // println!("{:?}", crypto.encrypt(plaintext)?);
    // assert_eq!(
    //     plaintext,
    //     crypto.decrypt(&crypto.encrypt(plaintext)?)?.as_slice()
    // );

    // dotenv().ok();

    // let uri = env::var("DATABASE_URI").expect("DATABASE_URI must be set");
    // let user = env::var("DATABASE_USERNAME").expect("DATABASE_USERNAME must be set");
    // let pass = env::var("DATABASE_PASSWORD").expect("DATABASE_PASSWORD must be set");
    // println!("{}, {}, {}", uri, user, pass);

    // let id = uuid::Uuid::new_v4().to_string();

    // let graph = Graph::new(uri, user, pass).await.unwrap();

    // let mut result = graph
    //     // .execute(Query::from("MATCH (n:label1 {k1: 'v3'}), (m:label1 {k1: 'v5'}) RETURN n, m"))
    //     .execute(Query::from(
    //         "MATCH (n:label1 {k1: 'v3'}), (m:label1 {k1: 'v5'}) RETURN n.k1, m",
    //     ))
    //     .await
    //     .unwrap();
    // while let Ok(Some(row)) = result.next().await {
    //     // println!("{:?}{:?}", row.get::<Node>("n").unwrap(), row.get::<Node>("m").unwrap());
    //     println!("{:?}", row.get::<String>("n.k1").unwrap());
    // }

    // {
    //     let id = uuid::Uuid::new_v4().to_string();
    //     graph
    //         .run(query(&format!("CREATE (p:{} {{id: $id}})", "aaa")).param("id", id.clone()))
    //         .await
    //         .unwrap();

    //     let mut handles = Vec::new();
    //     let count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    //     for _ in 1..=8 {
    //         let graph = graph.clone();
    //         let id = id.clone();
    //         let count = count.clone();
    //         let handle = tokio::spawn(async move {
    //             let mut result = graph
    //                 .execute(query("MATCH (p:aaa {id: $id}) RETURN p").param("id", id))
    //                 .await
    //                 .unwrap();
    //             while let Ok(Some(row)) = result.next().await {
    //                 println!("{:?}", row.to::<Node>().unwrap());
    //                 count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    //             }
    //         });
    //         handles.push(handle);
    //     }

    //     futures::future::join_all(handles).await;
    //     assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 8);
    // }

    Ok(())
}

mod logger {
    use log::{Level, LevelFilter, Metadata, Record};

    pub fn init() {
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Trace))
            .expect("failed to init the logger");
    }

    static LOGGER: SimpleLogger = SimpleLogger;

    struct SimpleLogger;

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() <= Level::Trace
        }

        fn log(&self, record: &Record) {
            if self.enabled(record.metadata()) {
                println!("[{}] {}", record.level(), record.args());
            }
        }

        fn flush(&self) {}
    }
}
