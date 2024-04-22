use super::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Item {
    Var(String),
    VarWithLabel(String, String),
    VarWithKey(String, String),
    VarWithKeyValue(String, String, String),
}

impl Item {
    pub(crate) fn to_query_string(item: &Item) -> String {
        match item {
            Item::Var(var_name) => var_name.to_string(),
            Item::VarWithLabel(var_name, label) => format!("{}:{}", var_name, label),
            Item::VarWithKey(var_name, key) => format!("{}.{}", var_name, key),
            Item::VarWithKeyValue(var_name, key, value) => {
                format!("{}.{} = '{}'", var_name, key, value)
            }
        }
    }
}
