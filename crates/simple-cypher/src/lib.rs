use anyhow::Result;
use serde::{Deserialize, Serialize};

mod cypher;
mod item;
mod node;
mod relation;
mod rows;

pub use self::cypher::{CRUDtype, CypherQuery, CypherQueryBuilder};
pub use self::item::Item;
pub use self::node::Node;
pub use self::relation::Relation;
pub use self::rows::{Inner, Row, Rows};

#[cfg(test)]
mod tests {
    use super::*;

    // CREATE (n:label1:label2 {k1: 'v1', k2: 'v2'}) RETURN n
    #[test]
    fn test_create() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .CREATE()
            .node(Node::new(
                Some("n"),
                vec!["label1", "label2"],
                vec![("k1", "v1"), ("k2", "v2")],
            ))
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "CREATE (n:label1:label2 {k1: 'v1', k2: 'v2'}) RETURN n"
        );

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // CREATE (a:label1:label2 {k1: 'v1', k2: 'v2'})-[r:rlabel1:rlabel2 {rk1: 'rv1', rk2: 'rv2'}]->(b:label1:label2 {k1: 'nv1', k2: 'nv2'}) RETURN a, r, b
    #[test]
    fn test_create2() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .CREATE()
            .node(Node::new(
                Some("a"),
                vec!["label1", "label2"],
                vec![("k1", "v1"), ("k2", "v2")],
            ))
            .relation(Relation::new(
                Some("r"),
                vec!["rlabel1", "rlabel2"],
                vec![("rk1", "rv1"), ("rk2", "rv2")],
            ))
            .next_node(Node::new(
                Some("b"),
                vec!["label1", "label2"],
                vec![("k1", "nv1"), ("k2", "nv2")],
            ))
            .RETURN(vec![
                Item::Var(String::from("a")),
                Item::Var(String::from("r")),
                Item::Var(String::from("b")),
            ])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(query_str, "CREATE (a:label1:label2 {k1: 'v1', k2: 'v2'})-[r:rlabel1:rlabel2 {rk1: 'rv1', rk2: 'rv2'}]->(b:label1:label2 {k1: 'nv1', k2: 'nv2'}) RETURN a, r, b");

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (a:label1:label2 {k1: 'v1', k2: 'v2'}), (b:label1:label2 {k1: 'nv1', k2: 'nv2'}) CREATE (a)-[r:rlabel1 {rk1: 'rv1', rk2: 'rv2'}]->(b) RETURN r
    #[test]
    fn test_create3() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("a"),
                vec!["label1", "label2"],
                vec![("k1", "v1"), ("k2", "v2")],
            ))
            .next_node(Node::new(
                Some("b"),
                vec!["label1", "label2"],
                vec![("k1", "nv1"), ("k2", "nv2")],
            ))
            .CREATE()
            .relation(Relation::new(
                Some("r"),
                vec!["rlabel1"],
                vec![("rk1", "rv1"), ("rk2", "rv2")],
            ))
            .RETURN(vec![Item::Var(String::from("r"))])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(query_str, "MATCH (a:label1:label2 {k1: 'v1', k2: 'v2'}), (b:label1:label2 {k1: 'nv1', k2: 'nv2'}) CREATE (a)-[r:rlabel1 {rk1: 'rv1', rk2: 'rv2'}]->(b) RETURN r");

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (n:label1:label2 {k1: 'v1'}) RETURN n
    #[test]
    fn test_match1() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                vec!["label1", "label2"],
                vec![("k1", "v1")],
            ))
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(query_str, "MATCH (n:label1:label2 {k1: 'v1'}) RETURN n");

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (a:label1 {k2: 'v2'})-[r:rlabel1 {rk1: 'rv1'}]->(b:label1) RETURN a, r, b
    #[test]
    fn test_match2() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("a"), vec!["label1"], vec![("k2", "v2")]))
            .relation(Relation::new(
                Some("r"),
                vec!["rlabel1"],
                vec![("rk1", "rv1")],
            ))
            .next_node(Node::new(
                Some("b"),
                vec!["label1"],
                Vec::<(String, String)>::new(),
            ))
            .RETURN(vec![
                Item::Var(String::from("a")),
                Item::Var(String::from("r")),
                Item::Var(String::from("b")),
            ])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "MATCH (a:label1 {k2: 'v2'})-[r:rlabel1 {rk1: 'rv1'}]->(b:label1) RETURN a, r, b"
        );

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (n:label1 {k1: 'v1'}) SET n:label3, n.k1 = 'new_v1', n.k3 = 'v3' RETURN n
    #[test]
    fn test_set() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["label1"], vec![("k1", "v1")]))
            .SET(vec![
                Item::VarWithLabel(String::from("n"), String::from("label3")),
                Item::VarWithKeyValue(
                    String::from("n"),
                    String::from("k1"),
                    String::from("new_v1"),
                ),
                Item::VarWithKeyValue(String::from("n"), String::from("k3"), String::from("v3")),
            ])
            .RETURN(vec![Item::Var(String::from("n"))])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "MATCH (n:label1 {k1: 'v1'})  SET n:label3, n.k1 = 'new_v1', n.k3 = 'v3' RETURN n"
        );

        let serialized = serde_json::to_string(&query)?;
        let deserilized: CypherQuery = serde_json::from_str(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (a:label1 {k1: 'v1'})-[r]->() SET r.rk1 = 'new_rv1', r.rk3 = 'rv3'
    #[test]
    fn test_set2() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("a"), vec!["label1"], vec![("k1", "v1")]))
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
            .SET(vec![
                Item::VarWithKeyValue(
                    String::from("r"),
                    String::from("rk1"),
                    String::from("new_rv1"),
                ),
                Item::VarWithKeyValue(String::from("r"), String::from("rk3"), String::from("rv3")),
            ])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "MATCH (a:label1 {k1: 'v1'})-[r]->()  SET r.rk1 = 'new_rv1', r.rk3 = 'rv3' "
        );

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (n:label1 {k1: 'v1'}) REMOVE n:label3, n.k3
    #[test]
    fn test_remove() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["label1"], vec![("k1", "v1")]))
            .REMOVE(vec![
                Item::VarWithLabel(String::from("n"), String::from("label3")),
                Item::VarWithKey(String::from("n"), String::from("k3")),
            ])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "MATCH (n:label1 {k1: 'v1'}) REMOVE n:label3, n.k3  "
        );

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (:label1:label2 {k1: 'v1'})-[r]->(:label1:label2 {k1: 'nv1'}) REMOVE r.rk3 RETURN r
    #[test]
    fn test_remove2() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                None::<String>,
                vec!["label1", "label2"],
                vec![("k1", "v1")],
            ))
            .relation(Relation::new(
                Some("r"),
                Vec::<String>::new(),
                Vec::<(String, String)>::new(),
            ))
            .next_node(Node::new(
                None::<String>,
                vec!["label1", "label2"],
                vec![("k1", "nv1")],
            ))
            .REMOVE(vec![Item::VarWithKey(
                String::from("r"),
                String::from("rk3"),
            )])
            .RETURN(vec![Item::Var(String::from("r"))])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "MATCH (:label1:label2 {k1: 'v1'})-[r]->(:label1:label2 {k1: 'nv1'}) REMOVE r.rk3  RETURN r"
        );

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (n:label1 {k1: 'v1'}) REMOVE n:label3, n.k3 SET n.k4 = 'v4'
    #[test]
    fn test_remove_set() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["label1"], vec![("k1", "v1")]))
            .REMOVE(vec![
                Item::VarWithLabel(String::from("n"), String::from("label3")),
                Item::VarWithKey(String::from("n"), String::from("k3")),
            ])
            .SET(vec![Item::VarWithKeyValue(
                String::from("n"),
                String::from("k4"),
                String::from("v4"),
            )])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "MATCH (n:label1 {k1: 'v1'}) REMOVE n:label3, n.k3 SET n.k4 = 'v4' "
        );

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (:label1:label2 {k1: 'v1'})-[r]->(:label1:label2 {k1: 'nv1'}) REMOVE r.rk3 SET r.rk4 = 'rv4' RETURN r
    #[test]
    fn test_remove_set2() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                None::<String>,
                vec!["label1", "label2"],
                vec![("k1", "v1")],
            ))
            .relation(Relation::new(
                Some("r"),
                Vec::<String>::new(),
                Vec::<(String, String)>::new(),
            ))
            .next_node(Node::new(
                None::<String>,
                vec!["label1", "label2"],
                vec![("k1", "nv1")],
            ))
            .REMOVE(vec![Item::VarWithKey(
                String::from("r"),
                String::from("rk3"),
            )])
            .SET(vec![Item::VarWithKeyValue(
                String::from("r"),
                String::from("rk4"),
                String::from("rv4"),
            )])
            .RETURN(vec![Item::Var(String::from("r"))])
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "MATCH (:label1:label2 {k1: 'v1'})-[r]->(:label1:label2 {k1: 'nv1'}) REMOVE r.rk3 SET r.rk4 = 'rv4' RETURN r"
        );

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (n:label1 {k1: 'v1'})-[r]->() DELETE n, r
    #[test]
    fn test_delete1() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["label1"], vec![("k1", "v1")]))
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

        let query_str = query.to_query_string()?;
        assert_eq!(
            query_str,
            "MATCH (n:label1 {k1: 'v1'})-[r]->() DELETE n, r "
        );

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (n:label1 {k1: 'v1'}) DETACH DELETE n
    #[test]
    fn test_delete2() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(Some("n"), vec!["label1"], vec![("k1", "v1")]))
            .DELETE(vec![Item::Var(String::from("n"))], true)
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(query_str, "MATCH (n:label1 {k1: 'v1'}) DETACH DELETE n ");

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    // MATCH (n) DETACH DELETE n
    #[test]
    fn test_delete3() -> Result<()> {
        let query = CypherQueryBuilder::new()
            .MATCH()
            .node(Node::new(
                Some("n"),
                Vec::<String>::new(),
                Vec::<(String, String)>::new(),
            ))
            .DELETE(vec![Item::Var(String::from("n"))], true)
            .build();

        let query_str = query.to_query_string()?;
        assert_eq!(query_str, "MATCH (n) DETACH DELETE n ");

        let serialized = query.serialize()?;
        let deserilized = CypherQuery::deserialize(&serialized)?;
        let query_str2 = deserilized.to_query_string()?;
        assert_eq!(query_str, query_str2);

        Ok(())
    }

    /*
    CREATE (n:label1:label2 {k1: 'v3', k2: 'v4'})
    RETURN n

    CREATE (a:label1:label2 {k1: 'v5', k2: 'v6'})-[r:label6 {k3: 'v1', k4: 'v1'}]->(b:label1:label2 {k1: 'v7', k2: 'v8'})
    RETURN a, r, b

    MATCH (a:label1:label2 {k1: 'v1', k2: 'v2'}), (b:label1:label2 {k1: 'v3', k2: 'v4'})
    CREATE (a)-[r:label6 {k3: 'v', k4: 'v'}]->(b)
    RETURN a, r, b

    MATCH (n:label1:label2 {k1: 'v1'})
    RETURN n

    MATCH (a:label1:label2 {k1: 'v1'})-[r:label6 {k3: 'v'}]->(b:label1)
    RETURN a, r, b

    MATCH (n:label1:label2 {k1: 'v1'})
    SET n:label4, n.k1 = 'v1', n.k2 = 'vv2', n.k3 = 'v3'
    RETURN n

    MATCH (n:label1:label2 {k1: 'v1'})-[r]->(b:label1)
    SET r.k3 = 'vv', r.k4 = 'vv', r.k5 = 'v'
    RETURN r

    MATCH (n:label1:label2 {k1: 'v1'})
    REMOVE n:label4, n.k3

    MATCH (n:label1:label2 {k1: 'v1'})-[r]->(b:label1)
    REMOVE r.k5

    MATCH (n:label1 {k1: 'v1'})-[r]->()
    DELETE n, r

    MATCH (n:label1 {k1: 'v5'})
    DETACH DELETE n

    MATCH (n)
    DETACH DELETE n

    */
}
