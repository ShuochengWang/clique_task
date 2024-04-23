use super::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Inner {
    pub labels: Vec<String>,
    pub properties: Vec<(String, String)>,
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

    pub fn add_label(&mut self, label: String) {
        self.labels.push(label);
    }

    pub fn remove_label(&mut self, label: &str) -> bool {
        for i in 0..self.labels.len() {
            if self.labels[i] == label {
                self.labels.remove(i);
                return true;
            }
        }
        false
    }

    pub fn add_property(&mut self, key: String, value: String) {
        self.properties.push((key, value));
    }

    pub fn update_property(&mut self, key: &str, value: String) -> bool {
        for i in 0..self.properties.len() {
            if self.properties[i].0 == key {
                self.properties[i].1 = value;
                return true;
            }
        }
        false
    }

    pub fn update_or_add_property(&mut self, key: &str, value: String) {
        for i in 0..self.properties.len() {
            if self.properties[i].0 == key {
                self.properties[i].1 = value;
                return;
            }
        }
        self.properties.push((key.to_string(), value));
    }

    pub fn remove_property(&mut self, key: &str) -> bool {
        for i in 0..self.properties.len() {
            if self.properties[i].0 == key {
                self.properties.remove(i);
                return true;
            }
        }
        false
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

    pub fn is_empty(&self) -> bool {
        self.inners.is_empty()
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

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}
