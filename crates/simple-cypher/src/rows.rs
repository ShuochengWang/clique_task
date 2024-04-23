use super::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Inner {
    labels: Vec<String>,
    properties: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Row {
    inners: Vec<Inner>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Rows {
    rows: Vec<Row>,
}

impl Inner {
    pub fn new(labels: Vec<String>, properties: Vec<(String, String)>) -> Self {
        Self { labels, properties }
    }

    pub fn labels(&self) -> &Vec<String> {
        &self.labels
    }

    pub fn properties(&self) -> &Vec<(String, String)> {
        &self.properties
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        for (k, v) in &self.properties {
            if k == key {
                return Some(v);
            }
        }
        None
    }
}

impl Row {
    pub fn new(inners: Vec<Inner>) -> Self {
        Self { inners }
    }

    pub fn new_empty() -> Self {
        Self { inners: vec![] }
    }

    pub fn inners(&self) -> &Vec<Inner> {
        &self.inners
    }

    pub fn push(&mut self, inner: Inner) {
        self.inners.push(inner);
    }
}

impl Rows {
    pub fn serialize(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }

    pub fn deserialize(serialized: &str) -> Result<Rows> {
        Ok(serde_json::from_str(serialized)?)
    }

    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    pub fn new_empty() -> Self {
        Self { rows: vec![] }
    }

    pub fn rows(&self) -> &Vec<Row> {
        &self.rows
    }

    pub fn push(&mut self, row: Row) {
        self.rows.push(row);
    }
}
