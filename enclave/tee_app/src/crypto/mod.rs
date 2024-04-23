mod seal_key;

use anyhow::Result;
use base64::{alphabet::Alphabet, engine::general_purpose, Engine as _};
use simple_cypher::*;
use soft_aes::aes::{aes_dec_ecb, aes_enc_ecb};

use std::collections::HashMap;

const MAGIC_PREFIX: &str = "a";

pub struct Crypto {
    key: [u8; 16],
    padding: Option<String>,
    base64_engine: general_purpose::GeneralPurpose,
}

impl Crypto {
    pub fn new() -> Self {
        let key = seal_key::get_key();
        let padding = Some(String::from("PKCS7"));
        let base64_alphabet =
            Alphabet::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_$")
                .unwrap();
        let base64_engine =
            general_purpose::GeneralPurpose::new(&base64_alphabet, general_purpose::NO_PAD);
        Self {
            key,
            padding,
            base64_engine,
        }
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        aes_enc_ecb(plaintext, &self.key, self.padding.as_ref().map(|x| &**x))
            .map_err(|_| anyhow::anyhow!("aes_enc_ecb failed"))
    }

    pub fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        aes_dec_ecb(encrypted, &self.key, self.padding.as_ref().map(|x| &**x))
            .map_err(|_| anyhow::anyhow!("aes_enc_ecb failed"))
    }

    pub fn encode(&self, data: &[u8]) -> Result<String> {
        Ok(self.base64_engine.encode(data))
    }

    pub fn decode(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(self.base64_engine.decode(data)?)
    }

    pub fn enc_query(&self, query: &mut CypherQuery) -> Result<()> {
        let mut plain2enc = HashMap::new();

        self.enc_node(query.node.as_mut(), &mut plain2enc)?;
        self.enc_relationship(query.relation.as_mut(), &mut plain2enc)?;
        self.enc_node(query.next_node.as_mut(), &mut plain2enc)?;
        self.enc_items(query.return_list.as_mut(), &mut plain2enc)?;
        self.enc_items(query.set_list.as_mut(), &mut plain2enc)?;
        self.enc_items(query.remove_list.as_mut(), &mut plain2enc)?;

        if let Some((list, _)) = query.delete_list.as_ref() {
            for item in list {
                match item {
                    Item::Var(_) => {}
                    _ => return Err(anyhow::anyhow!("Invalid query: {:?}", query)),
                }
            }
        }

        Ok(())
    }

    pub fn decrypt_and_verify(&self, mut enc_row: Row) -> Result<Row> {
        let mut enc2plain = HashMap::new();
        for inner in enc_row.inners_mut() {
            self.dec_inner(inner, &mut enc2plain)?;
        }
        Ok(enc_row)
    }

    fn enc_node(
        &self,
        node: Option<&mut Node>,
        plain2enc: &mut HashMap<String, String>,
    ) -> Result<()> {
        if let Some(inner) = node {
            for i in 0..inner.labels.len() {
                inner.labels[i] = self.enc_string(&inner.labels[i], plain2enc)?;
            }
            for i in 0..inner.properties.len() {
                inner.properties[i].0 = self.enc_string(&inner.properties[i].0, plain2enc)?;
                inner.properties[i].1 = self.enc_string(&inner.properties[i].1, plain2enc)?;
            }
        }
        Ok(())
    }

    fn enc_relationship(
        &self,
        relation: Option<&mut Relation>,
        plain2enc: &mut HashMap<String, String>,
    ) -> Result<()> {
        if let Some(inner) = relation {
            for i in 0..inner.labels.len() {
                inner.labels[i] = self.enc_string(&inner.labels[i], plain2enc)?;
            }
            for i in 0..inner.properties.len() {
                inner.properties[i].0 = self.enc_string(&inner.properties[i].0, plain2enc)?;
                inner.properties[i].1 = self.enc_string(&inner.properties[i].1, plain2enc)?;
            }
        }
        Ok(())
    }

    fn enc_items(
        &self,
        items: Option<&mut Vec<Item>>,
        plain2enc: &mut HashMap<String, String>,
    ) -> Result<()> {
        if let Some(items) = items {
            for i in 0..items.len() {
                match &items[i] {
                    Item::VarWithLabel(var, label) => {
                        items[i] =
                            Item::VarWithLabel(var.clone(), self.enc_string(&label, plain2enc)?);
                    }
                    Item::VarWithKey(var, key) => {
                        items[i] = Item::VarWithKey(var.clone(), self.enc_string(&key, plain2enc)?);
                    }
                    Item::VarWithKeyValue(var, key, value) => {
                        items[i] = Item::VarWithKeyValue(
                            var.clone(),
                            self.enc_string(&key, plain2enc)?,
                            self.enc_string(&value, plain2enc)?,
                        );
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    fn enc_string(
        &self,
        plain: &String,
        plain2enc: &mut HashMap<String, String>,
    ) -> Result<String> {
        if let Some(enc) = plain2enc.get(plain) {
            Ok(enc.clone())
        } else {
            let enc = add_prefix(&self.encode(&self.encrypt(plain.as_bytes())?)?);
            plain2enc.insert(plain.clone(), enc.clone());
            Ok(enc)
        }
    }

    fn dec_inner(&self, inner: &mut Inner, enc2plain: &mut HashMap<String, String>) -> Result<()> {
        for i in 0..inner.labels.len() {
            inner.labels[i] = self.dec_string(&inner.labels[i], enc2plain)?;
        }
        for i in 0..inner.properties.len() {
            inner.properties[i].0 = self.dec_string(&inner.properties[i].0, enc2plain)?;
            inner.properties[i].1 = self.dec_string(&inner.properties[i].1, enc2plain)?;
        }
        Ok(())
    }

    fn dec_string(&self, enc: &String, enc2plain: &mut HashMap<String, String>) -> Result<String> {
        if let Some(plain) = enc2plain.get(enc) {
            Ok(plain.clone())
        } else {
            let plain =
                String::from_utf8(self.decrypt(&self.decode(remove_prefix(enc)?.as_bytes())?)?)?;
            enc2plain.insert(enc.clone(), plain.clone());
            Ok(plain)
        }
    }
}

fn add_prefix(s: &str) -> String {
    format!("{}{}", MAGIC_PREFIX, s)
}

fn remove_prefix(s: &str) -> Result<String> {
    if !s.starts_with(MAGIC_PREFIX) {
        return Err(anyhow::anyhow!(
            "There is no MAGIC_LABEL_KEY_PREFIX in this str: {}",
            s
        ));
    }

    Ok(s[MAGIC_PREFIX.len()..].to_string())
}
