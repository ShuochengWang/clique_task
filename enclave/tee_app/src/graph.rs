use anyhow::Result;
use simple_cypher::*;

use crate::crypto::Crypto;

pub const MAGIC_HASH_KEY: &str = "hash";
pub const MAGIC_UID_KEY: &str = "uid";

const NODE_VAR_NAME: &str = "n";
const NEXT_NODE_VAR_NAME: &str = "m";
const RELATIONSHIP_VAR_NAME: &str = "r";

struct EncryptedGraph {
    database: neo4rs::Graph,
    crypto: Crypto,
}

impl EncryptedGraph {
    pub async fn new(
        uri: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self> {
        let database = neo4rs::Graph::new(uri, user, password).await?;
        let crypto = Crypto::new();
        Ok(Self { database, crypto })
    }

    pub async fn execute_query(&self, serialized_query: String) -> Result<Rows> {
        let mut query = CypherQuery::deserialize(&serialized_query)?;

        confuse_var_name(&mut query);

        match query.get_type()? {
            CRUDtype::Create => self.create(query).await,
            CRUDtype::Read => self.read(query).await,
            CRUDtype::Update => self.update(query).await,
            CRUDtype::Delete => self.delete(query).await,
        }
    }

    async fn create(&self, mut query: CypherQuery) -> Result<Rows> {
        // TODO: Solve the problem where the uid name may conflict with the property name in the query
        match (
            query.node.is_some(),
            query.relation.is_some(),
            query.next_node.is_some(),
            query.use_match,
        ) {
            // case 1: CREATE (n:Label {name: $value})
            (true, false, false, false) => {
                add_uid_to_node(query.node.as_mut().unwrap());
                add_hash_to_node(query.node.as_mut().unwrap());
            }
            // case 2: CREATE (n:Label)-[r:TYPE]->(m:Label)
            (true, true, true, false) => {
                let from = add_uid_to_node(query.node.as_mut().unwrap());
                let to = add_uid_to_node(query.next_node.as_mut().unwrap());
                add_uid_to_relationship(query.relation.as_mut().unwrap(), &from, &to);

                add_hash_to_node(query.node.as_mut().unwrap());
                add_hash_to_node(query.next_node.as_mut().unwrap());
                add_hash_to_relationship(query.relation.as_mut().unwrap());
            }
            // case 3: MATCH (n:Label), (m:Label) CREATE (n)-[r:TYPE]->(m)
            (true, true, true, true) => {
                // MATCH (n:Label), (m:Label) RETURN n, m
                let read_query = {
                    let mut read_query = query.clone();
                    read_query.relation.take();
                    read_query.use_create = false;
                    read_query.return_list.replace(vec![
                        Item::Var(NODE_VAR_NAME.to_string()),
                        Item::Var(NEXT_NODE_VAR_NAME.to_string()),
                    ]);
                    read_query
                };

                let plain_rows = self.read(read_query).await?;

                let mut res_rows = Rows::new_empty();
                for plain_row in plain_rows.rows() {
                    if plain_row.inners().len() != 2 {
                        return Err(anyhow::anyhow!("Data was attacked"));
                    }
                    let from_uid = plain_row.inners()[0]
                        .get(MAGIC_UID_KEY)
                        .ok_or_else(|| anyhow::anyhow!("Data was attacked"))?;
                    let to_uid = plain_row.inners()[1]
                        .get(MAGIC_UID_KEY)
                        .ok_or_else(|| anyhow::anyhow!("Data was attacked"))?;

                    let single_query = {
                        let mut single_query = query.clone();
                        single_query
                            .node
                            .as_mut()
                            .unwrap()
                            .add_property(MAGIC_UID_KEY.to_string(), from_uid.clone());
                        single_query
                            .next_node
                            .as_mut()
                            .unwrap()
                            .add_property(MAGIC_UID_KEY.to_string(), to_uid.clone());
                        add_uid_to_relationship(
                            single_query.relation.as_mut().unwrap(),
                            &from_uid,
                            &to_uid,
                        );
                        add_hash_to_relationship(single_query.relation.as_mut().unwrap());

                        self.encrypt_query(&mut single_query);
                        single_query
                    };

                    let mut result = self
                        .database
                        .execute(neo4rs::Query::from(single_query.to_query_string()?))
                        .await?;

                    let return_list = get_return_vars(&single_query);
                    let mut res_enc_row = Row::new_empty();
                    while let Ok(Some(row)) = result.next().await {
                        for var in &return_list {
                            if let Ok(n) = row.get::<neo4rs::Node>(var) {
                                res_enc_row.push(build_inner_from_neo4rs_node(n));
                            }
                            if let Ok(r) = row.get::<neo4rs::Relation>(var) {
                                res_enc_row.push(build_inner_from_neo4rs_relation(r));
                            }
                        }
                    }

                    if !res_enc_row.is_empty() {
                        res_rows.push(self.crypto.decrypt_and_verify(&res_enc_row)?);
                    }
                }
            }
            _ => return Err(anyhow::anyhow!("Invalid query: {:?}", query)),
        }

        self.encrypt_query(&mut query);

        todo!("");
    }

    async fn read(&self, mut query: CypherQuery) -> Result<Rows> {
        todo!("");
    }

    async fn update(&self, mut query: CypherQuery) -> Result<Rows> {
        todo!("");
    }

    async fn delete(&self, mut query: CypherQuery) -> Result<Rows> {
        todo!("");
    }

    fn encrypt_query(&self, query: &mut CypherQuery) -> Result<()> {
        self.crypto.enc_query(query)
    }
}

fn confuse_var_name(query: &mut CypherQuery) {
    let map_table = {
        let mut map_table = vec![];
        let var1 = query
            .node
            .as_mut()
            .and_then(|x| x.var_name.replace(NODE_VAR_NAME.to_string()));
        let var2 = query
            .relation
            .as_mut()
            .and_then(|x| x.var_name.replace(RELATIONSHIP_VAR_NAME.to_string()));
        let var3 = query
            .next_node
            .as_mut()
            .and_then(|x| x.var_name.replace(NEXT_NODE_VAR_NAME.to_string()));
        if let Some(var) = var1 {
            map_table.push((var, NODE_VAR_NAME.to_string()));
        }
        if let Some(var) = var2 {
            map_table.push((var, RELATIONSHIP_VAR_NAME.to_string()));
        }
        if let Some(var) = var3 {
            map_table.push((var, NEXT_NODE_VAR_NAME.to_string()));
        }
        map_table
    };

    let update_var_name = |list: &mut Vec<Item>| {
        for i in 0..list.len() {
            match &list[i] {
                Item::Var(var_name) => {
                    for (old_var, new_var) in &map_table {
                        if var_name == old_var {
                            list[i] = Item::Var(new_var.clone());
                            break;
                        }
                    }
                }
                Item::VarWithLabel(var_name, label) => {
                    for (old_var, new_var) in &map_table {
                        if var_name == old_var {
                            list[i] = Item::VarWithLabel(new_var.clone(), label.clone());
                            break;
                        }
                    }
                }
                Item::VarWithKey(var_name, key) => {
                    for (old_var, new_var) in &map_table {
                        if var_name == old_var {
                            list[i] = Item::VarWithKey(new_var.clone(), key.clone());
                            break;
                        }
                    }
                }
                Item::VarWithKeyValue(var_name, key, value) => {
                    for (old_var, new_var) in &map_table {
                        if var_name == old_var {
                            list[i] =
                                Item::VarWithKeyValue(new_var.clone(), key.clone(), value.clone());
                            break;
                        }
                    }
                }
            }
        }
    };

    if let Some(list) = query.return_list.as_mut() {
        update_var_name(list);
    }
    if let Some(list) = query.set_list.as_mut() {
        update_var_name(list);
    }
    if let Some(list) = query.remove_list.as_mut() {
        update_var_name(list);
    }
    if let Some((list, _)) = query.delete_list.as_mut() {
        update_var_name(list);
    }
}

fn add_hash_to_node(inner: &mut Node) {
    inner.labels.sort();
    inner.properties.sort();

    let mut hasher = blake3::Hasher::new();
    inner.labels.iter().for_each(|x| {
        hasher.update(x.as_bytes());
    });
    inner.properties.iter().for_each(|(k, v)| {
        hasher.update(k.as_bytes());
        hasher.update(v.as_bytes());
    });
    inner
        .properties
        .push((MAGIC_HASH_KEY.to_string(), hasher.finalize().to_string()));
}

fn add_hash_to_relationship(inner: &mut Relation) {
    inner.labels.sort();
    inner.properties.sort();

    let mut hasher = blake3::Hasher::new();
    inner.labels.iter().for_each(|x| {
        hasher.update(x.as_bytes());
    });
    inner.properties.iter().for_each(|(k, v)| {
        hasher.update(k.as_bytes());
        hasher.update(v.as_bytes());
    });
    inner
        .properties
        .push((MAGIC_HASH_KEY.to_string(), hasher.finalize().to_string()));
}

fn add_uid_to_node(node: &mut Node) -> String {
    let uid = uuid::Uuid::new_v4().to_string();
    node.properties
        .push((MAGIC_UID_KEY.to_string(), uid.clone()));
    uid
}

fn add_uid_to_relationship(relation: &mut Relation, from_uid: &String, to_uid: &String) {
    relation
        .properties
        .push((MAGIC_UID_KEY.to_string(), format!("{}{}", from_uid, to_uid)));
}

fn get_return_vars(query: &CypherQuery) -> Vec<String> {
    let mut vars = vec![];
    if query.return_list.is_none() {
        return vars;
    }

    for item in query.return_list.as_ref().unwrap() {
        match item {
            Item::Var(v) => vars.push(v.clone()),
            // todo: support other items
            _ => {}
        }
    }
    vars
}

fn build_inner_from_neo4rs_node(node: neo4rs::Node) -> Inner {
    let labels = node.labels().iter().map(|s| s.to_string()).collect();
    let mut properties = vec![];
    for k in node.keys() {
        properties.push((k.to_string(), node.get::<String>(k).unwrap()));
    }
    Inner::new(labels, properties)
}

fn build_inner_from_neo4rs_relation(relation: neo4rs::Relation) -> Inner {
    let labels = vec![relation.typ().to_string()];
    let mut properties = vec![];
    for k in relation.keys() {
        properties.push((k.to_string(), relation.get::<String>(k).unwrap()));
    }
    Inner::new(labels, properties)
}
