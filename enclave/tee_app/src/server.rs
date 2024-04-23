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

    test(&graph).await.unwrap();

    Ok(())
}

async fn test(graph: &EncryptedGraph) -> Result<()> {
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
        println!("{:?}", result);
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
        println!("{:?}", result);
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
        println!("{:?}", result);
    }
    Ok(())
}
