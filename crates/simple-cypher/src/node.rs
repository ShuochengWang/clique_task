use super::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub var_name: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, String)>,
}

impl Node {
    pub fn new(
        var_name: Option<impl Into<String>>,
        labels: Vec<impl Into<String>>,
        properties: Vec<(impl Into<String>, impl Into<String>)>,
    ) -> Self {
        Self {
            var_name: var_name.map(|x| x.into()),
            labels: labels.into_iter().map(|x| x.into()).collect(),
            properties: properties
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }

    pub fn var_name(&self) -> Option<&String> {
        self.var_name.as_ref()
    }

    pub fn add_property(&mut self, key: String, value: String) {
        self.properties.push((key, value));
    }

    pub(crate) fn to_query_string(&self) -> String {
        let mut res = String::from("(");

        if let Some(var_name) = self.var_name.as_ref() {
            res.push_str(var_name);
        }

        for label in &self.labels {
            res.push_str(&format!(":{}", label));
        }

        if !self.properties.is_empty() {
            res.push_str(" {");
        }

        for i in 0..self.properties.len() {
            res.push_str(&format!(
                "{}: '{}'",
                self.properties[i].0, self.properties[i].1
            ));
            if i != self.properties.len() - 1 {
                res.push_str(", ");
            }
        }

        if !self.properties.is_empty() {
            res.push('}');
        }

        res.push(')');
        res
    }
}
