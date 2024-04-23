use super::*;
use crate::graph::EncryptedGraph;

use anyhow::Result;
use dotenv::dotenv;
use simple_cypher::*;

use std::env;

pub async fn start_server() -> Result<()> {
    dotenv().ok();

    let uri = env::var("DATABASE_URI").expect("DATABASE_URI must be set");
    let user = env::var("DATABASE_USERNAME").expect("DATABASE_USERNAME must be set");
    let pass = env::var("DATABASE_PASSWORD").expect("DATABASE_PASSWORD must be set");

    let graph = EncryptedGraph::new(uri, user, pass).await?;

    let query = CypherQueryBuilder::new()
        .CREATE()
        .node(Node::new(
            Some("a"),
            vec!["label1"],
            vec![("k1", "v1"), ("k2", "v2")],
        ))
        .RETURN(vec![Item::Var(String::from("n"))])
        .build();
    let result = graph.execute_query(CypherQuery::deserialize(&query.serialize()?)?).await.unwrap();
    println!("{:?}", result);

    Ok(())
}
