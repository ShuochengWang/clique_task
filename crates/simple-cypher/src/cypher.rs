use super::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CypherQuery {
    pub node: Option<Node>,
    pub relationship: Option<Relationship>,
    pub next_node: Option<Node>,
    pub use_match: bool,
    pub use_create: bool,
    pub return_list: Option<Vec<Item>>,
    pub set_list: Option<Vec<Item>>,
    pub remove_list: Option<Vec<Item>>,
    pub delete_list: Option<(Vec<Item>, bool)>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CypherQueryBuilder {
    node: Option<Node>,
    relationship: Option<Relationship>,
    next_node: Option<Node>,
    use_match: bool,
    use_create: bool,
    return_list: Option<Vec<Item>>,
    set_list: Option<Vec<Item>>,
    remove_list: Option<Vec<Item>>,
    delete_list: Option<(Vec<Item>, bool)>,
}

#[derive(Debug)]
pub enum CRUDtype {
    Create,
    Read,
    Update,
    Delete,
}

impl CypherQuery {
    pub fn serialize(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }

    pub fn deserialize(serialized: &str) -> Result<CypherQuery> {
        Ok(serde_json::from_str(serialized)?)
    }

    pub fn to_query_string(&self) -> Result<String> {
        match (
            &self.node,
            &self.relationship,
            &self.next_node,
            self.use_match,
            self.use_create,
            &self.return_list,
            &self.set_list,
            &self.remove_list,
            &self.delete_list,
        ) {
            (Some(node), None, None, false, true, _, None, None, None) => {
                return Ok(format!(
                    "CREATE {} {}",
                    node.to_query_string(),
                    self.to_return_query_string()?
                ));
            }
            (Some(node), Some(r), Some(next_node), false, true, _, None, None, None) => {
                return Ok(format!(
                    "CREATE {}-{}->{} {}",
                    node.to_query_string(),
                    r.to_query_string(),
                    next_node.to_query_string(),
                    self.to_return_query_string()?,
                ));
            }
            (Some(node), Some(r), Some(next_node), true, true, _, None, None, None) => {
                return Ok(format!(
                    "MATCH {}, {} CREATE ({})-{}->({}) {}",
                    node.to_query_string(),
                    next_node.to_query_string(),
                    node.var_name()
                        .ok_or_else(|| anyhow::anyhow!("Need var_name: {:?}", node))?,
                    r.to_query_string(),
                    next_node
                        .var_name()
                        .ok_or_else(|| anyhow::anyhow!("Need var_name: {:?}", node))?,
                    self.to_return_query_string()?,
                ));
            }
            (Some(node), None, None, true, false, Some(_), None, None, None) => {
                return Ok(format!(
                    "MATCH {} {}",
                    node.to_query_string(),
                    self.to_return_query_string()?
                ));
            }
            (Some(node), Some(r), Some(next_node), true, false, Some(_), None, None, None) => {
                return Ok(format!(
                    "MATCH {}-{}->{} {}",
                    node.to_query_string(),
                    r.to_query_string(),
                    next_node.to_query_string(),
                    self.to_return_query_string()?
                ));
            }
            (Some(node), None, None, true, false, _, Some(_), None, None) => {
                return Ok(format!(
                    "MATCH {} {} {}",
                    node.to_query_string(),
                    self.to_set_query_string()?,
                    self.to_return_query_string()?
                ));
            }
            (Some(node), Some(r), Some(next_node), true, false, _, Some(_), None, None) => {
                return Ok(format!(
                    "MATCH {}-{}->{} {} {}",
                    node.to_query_string(),
                    r.to_query_string(),
                    next_node.to_query_string(),
                    self.to_set_query_string()?,
                    self.to_return_query_string()?
                ));
            }
            (Some(node), None, None, true, false, _, None, Some(_), None) => {
                return Ok(format!(
                    "MATCH {} {} {}",
                    node.to_query_string(),
                    self.to_remove_query_string()?,
                    self.to_return_query_string()?
                ));
            }
            (Some(node), Some(r), Some(next_node), true, false, _, None, Some(_), None) => {
                return Ok(format!(
                    "MATCH {}-{}->{} {} {}",
                    node.to_query_string(),
                    r.to_query_string(),
                    next_node.to_query_string(),
                    self.to_remove_query_string()?,
                    self.to_return_query_string()?
                ));
            }
            (Some(node), None, None, true, false, _, None, None, Some(_)) => {
                return Ok(format!(
                    "MATCH {} {} {}",
                    node.to_query_string(),
                    self.to_delete_query_string()?,
                    self.to_return_query_string()?
                ));
            }
            (Some(node), Some(r), Some(next_node), true, false, _, None, None, Some(_)) => {
                return Ok(format!(
                    "MATCH {}-{}->{} {} {}",
                    node.to_query_string(),
                    r.to_query_string(),
                    next_node.to_query_string(),
                    self.to_delete_query_string()?,
                    self.to_return_query_string()?
                ));
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid or unsupported cypher query: {:?}",
                    self
                ))
            }
        }
    }

    pub fn get_type(&self) -> Result<CRUDtype> {
        match (
            &self.node,
            &self.relationship,
            &self.next_node,
            self.use_match,
            self.use_create,
            &self.return_list,
            &self.set_list,
            &self.remove_list,
            &self.delete_list,
        ) {
            (Some(_), None, None, false, true, _, None, None, None)
            | (Some(_), Some(_), Some(_), false, true, _, None, None, None)
            | (Some(_), Some(_), Some(_), true, true, _, None, None, None) => Ok(CRUDtype::Create),

            (Some(_), None, None, true, false, Some(_), None, None, None)
            | (Some(_), Some(_), Some(_), true, false, Some(_), None, None, None) => {
                Ok(CRUDtype::Read)
            }

            (Some(_), None, None, true, false, _, Some(_), None, None)
            | (Some(_), Some(_), Some(_), true, false, _, Some(_), None, None)
            | (Some(_), None, None, true, false, _, None, Some(_), None)
            | (Some(_), Some(_), Some(_), true, false, _, None, Some(_), None) => {
                Ok(CRUDtype::Update)
            }

            (Some(_), None, None, true, false, _, None, None, Some(_))
            | (Some(_), Some(_), Some(_), true, false, _, None, None, Some(_)) => {
                Ok(CRUDtype::Delete)
            }

            _ => Err(anyhow::anyhow!(
                "Invalid or unsupported cypher query: {:?}",
                self
            )),
        }
    }

    fn to_return_query_string(&self) -> Result<String> {
        if self.return_list.is_none() {
            return Ok(String::new());
        }

        let s = self
            .return_list
            .as_ref()
            .unwrap()
            .iter()
            .map(|x| Item::to_query_string(x))
            .collect::<Vec<String>>()
            .join(", ");
        if s.is_empty() {
            return Err(anyhow::anyhow!(
                "RETURN was used but no variable was provided: {:?}",
                self
            ));
        }
        Ok(format!("RETURN {}", s))
    }

    fn to_set_query_string(&self) -> Result<String> {
        if self.set_list.is_none() {
            return Ok(String::new());
        }

        let s = self
            .set_list
            .as_ref()
            .unwrap()
            .iter()
            .map(|x| Item::to_query_string(x))
            .collect::<Vec<String>>()
            .join(", ");
        if s.is_empty() {
            return Err(anyhow::anyhow!(
                "SET was used but no variable was provided: {:?}",
                self
            ));
        }
        Ok(format!("SET {}", s))
    }

    fn to_remove_query_string(&self) -> Result<String> {
        if self.remove_list.is_none() {
            return Ok(String::new());
        }

        let s = self
            .remove_list
            .as_ref()
            .unwrap()
            .iter()
            .map(|x| Item::to_query_string(x))
            .collect::<Vec<String>>()
            .join(", ");
        if s.is_empty() {
            return Err(anyhow::anyhow!(
                "REMOVE was used but no variable was provided: {:?}",
                self
            ));
        }
        Ok(format!("REMOVE {}", s))
    }

    fn to_delete_query_string(&self) -> Result<String> {
        if self.delete_list.is_none() {
            return Ok(String::new());
        }

        let s = self
            .delete_list
            .as_ref()
            .unwrap()
            .0
            .iter()
            .map(|x| Item::to_query_string(x))
            .collect::<Vec<String>>()
            .join(", ");
        if s.is_empty() {
            return Err(anyhow::anyhow!(
                "DELETE was used but no variable was provided: {:?}",
                self
            ));
        }
        if self.delete_list.as_ref().unwrap().1 {
            Ok(format!("DETACH DELETE {}", s))
        } else {
            Ok(format!("DELETE {}", s))
        }
    }
}

#[allow(non_snake_case)]
impl CypherQueryBuilder {
    pub fn new() -> Self {
        Self {
            node: None,
            relationship: None,
            next_node: None,
            use_match: false,
            use_create: false,
            return_list: None,
            set_list: None,
            remove_list: None,
            delete_list: None,
        }
    }

    pub fn node(mut self, node: Node) -> Self {
        assert!(self.node.is_none());

        self.node = Some(node);
        self
    }

    pub fn relationship(mut self, relationship: Relationship) -> Self {
        assert!(self.relationship.is_none());

        self.relationship = Some(relationship);
        self
    }

    pub fn next_node(mut self, next_node: Node) -> Self {
        assert!(self.next_node.is_none());

        self.next_node = Some(next_node);
        self
    }

    pub fn CREATE(mut self) -> Self {
        self.use_create = true;
        self
    }

    pub fn MATCH(mut self) -> Self {
        self.use_match = true;
        self
    }

    pub fn RETURN(mut self, list: Vec<Item>) -> Self {
        self.return_list = Some(list);
        self
    }

    pub fn SET(mut self, list: Vec<Item>) -> Self {
        self.set_list = Some(list);
        self
    }

    pub fn REMOVE(mut self, list: Vec<Item>) -> Self {
        self.remove_list = Some(list);
        self
    }

    pub fn DELETE(mut self, list: Vec<Item>, is_detach: bool) -> Self {
        self.delete_list = Some((list, is_detach));
        self
    }

    pub fn build(self) -> CypherQuery {
        CypherQuery {
            node: self.node,
            relationship: self.relationship,
            next_node: self.next_node,
            use_match: self.use_match,
            use_create: self.use_create,
            return_list: self.return_list,
            set_list: self.set_list,
            remove_list: self.remove_list,
            delete_list: self.delete_list,
        }
    }
}
