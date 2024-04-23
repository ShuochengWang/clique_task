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

    // test_CRUD(&graph).await.unwrap();
    // test_find_shortest_path(&graph).await.unwrap();

    Ok(())
}

async fn test_CRUD(graph: &EncryptedGraph) -> Result<()> {
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
