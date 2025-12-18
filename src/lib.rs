use std::collections::{HashMap, HashSet};

use anyhow::{Error, Result, anyhow};
use fallible_iterator::FallibleIterator;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_128;

#[derive(
    Clone, Default, PartialEq, PartialOrd, Debug, bincode::Encode, bincode::Decode, Eq, Ord, Hash,
)]
pub struct ObjectId {
    pub value: [u8; 16],
}

impl ObjectId {
    pub fn new() -> Self {
        Self {
            value: *uuid::Uuid::now_v7().as_bytes(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Object {
    pub id: ObjectId,
    pub value: serde_json::Value,
}

type FlatObject = Vec<(String, serde_json::Value)>;

fn insert_into_map(
    map: &mut serde_json::Map<String, serde_json::Value>,
    parts: &[&str],
    value: serde_json::Value,
) {
    if parts.len() == 1 {
        map.insert(parts[0].to_string(), value);
    } else {
        let first = parts[0].to_string();
        let rest = &parts[1..];
        if !map.contains_key(&first) {
            map.insert(
                first.clone(),
                serde_json::Value::Object(serde_json::Map::new()),
            );
        }
        if let Some(serde_json::Value::Object(nested_map)) = map.get_mut(&first) {
            insert_into_map(nested_map, rest, value);
        }
    }
}

fn nest(flat_object: FlatObject) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (path, value) in flat_object {
        let parts: Vec<&str> = path.split('.').collect();
        insert_into_map(&mut map, &parts, value);
    }
    serde_json::Value::Object(map)
}

#[derive(bincode::Encode, bincode::Decode, Clone)]
enum Value {
    Null,
    Integer(u64),
    Float(f64),
    Bool(bool),
    String(String),
}

impl TryFrom<serde_json::Value> for Value {
    type Error = Error;

    fn try_from(json_value: serde_json::Value) -> Result<Self> {
        match json_value {
            serde_json::Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    Ok(Value::Integer(u))
                } else if let Some(i) = n.as_i64() {
                    Ok(Value::Integer(i as u64))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float(f))
                } else {
                    Ok(Value::Float(n.as_f64().unwrap_or(0.0)))
                }
            }
            serde_json::Value::String(s) => Ok(Value::String(s)),
            serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Array(_) => Err(anyhow!("Can not convert JSON array to value")),
            serde_json::Value::Object(_) => Err(anyhow!("Can not convert JSON object to value")),
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Integer(i) => serde_json::Value::Number(i.into()),
            Value::Float(f) => serde_json::json!(f),
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::String(s) => serde_json::Value::String(s),
        }
    }
}

dream::define_index!(trove_database {
    object_id_and_path_to_value<(ObjectId, String), super::super::Value>,
} use {
    use crate::ObjectId;
});

struct Chest {
    index: trove_database::Index,
}

#[derive(Serialize, Deserialize)]
struct ChestConfig {
    index: trove_database::IndexConfig,
}

impl Chest {
    pub fn new(config: ChestConfig) -> Result<Self> {
        Ok(Self {
            index: trove_database::Index::new(config.index)?,
        })
    }

    pub fn lock_all_and_write<'a, F>(&'a mut self, mut f: F) -> Result<&'a mut Self>
    where
        F: FnMut(&mut WriteTransaction<'_, '_, '_>) -> Result<()>,
    {
        self.index.lock_all_and_write(|index_write_transaction| {
            f(&mut WriteTransaction {
                index_transaction: index_write_transaction,
            })
        })?;

        Ok(self)
    }

    pub fn lock_all_writes_and_read<F>(&self, mut f: F) -> Result<&Self>
    where
        F: FnMut(ReadTransaction) -> Result<()>,
    {
        self.index
            .lock_all_writes_and_read(|index_read_transaction| {
                f(ReadTransaction {
                    index_transaction: index_read_transaction,
                })
            })?;
        Ok(self)
    }
}

struct Digest {
    value: [u8; 16],
}

impl Digest {
    pub fn of_data(data: &Vec<u8>) -> Self {
        Self {
            value: xxhash_rust::xxh3::xxh3_128(data).to_le_bytes(),
        }
    }

    pub fn of_path_and_encoded_value(path: &str, value: &Value) -> Result<Self> {
        let encoded_value = bincode::encode_to_vec(value, bincode::config::standard())?;
        let mut data: Vec<u8> = Vec::with_capacity(path.len() + 1 + encoded_value.len());
        data.extend_from_slice(path.as_bytes());
        data.push(0u8);
        data.extend_from_slice(&encoded_value);
        Ok(Self::of_data(&data))
    }
}

struct PartitionedPath {
    base: String,
    index: Option<u64>,
}

impl PartitionedPath {
    pub fn from_path(path: String) -> Self {
        if let Some(dot_position) = path.rfind('.') {
            let (base, index_string) = path.split_at(dot_position);
            if let Ok(index) = index_string[1..].parse::<u64>() {
                Self {
                    base: base.to_string(),
                    index: Some(index),
                }
            } else {
                Self {
                    base: base.to_string(),
                    index: None,
                }
            }
        } else {
            Self {
                base: path,
                index: None,
            }
        }
    }
}

pub struct ReadTransaction<'a> {
    pub index_transaction: trove_database::ReadTransaction<'a>,
}

pub struct WriteTransaction<'a, 'b, 'c> {
    pub index_transaction: &'a mut trove_database::WriteTransaction<'b, 'c>,
}

pub struct ObjectsIterator<'a> {
    data_table_iterator:
        Box<dyn FallibleIterator<Item = ((ObjectId, String), Value), Error = Error> + 'a>,
    last_entry: Option<((ObjectId, String), Value)>,
}

macro_rules! define_read_methods {
    () => {
        pub fn objects(&'a self) -> Result<ObjectsIterator<'a>> {
            Ok(ObjectsIterator {
                data_table_iterator: self
                    .index_transaction
                    .database_transaction
                    .object_id_and_path_to_value
                    .iter(None)?,
                last_entry: None,
            })
        }
    };
}

impl<'a> ReadTransaction<'a> {
    define_read_methods!();
}

impl<'a> FallibleIterator for ObjectsIterator<'a> {
    type Item = Object;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        if self.last_entry.is_none() {
            self.last_entry = self.data_table_iterator.next()?;
        }
        if let Some(first_object_entry) = self.last_entry.clone() {
            let object_id = first_object_entry.0.0;
            let mut flat_object: FlatObject = Vec::new();
            flat_object.push((
                first_object_entry.0.1,
                serde_json::Value::from(first_object_entry.1),
            ));
            loop {
                self.last_entry = self.data_table_iterator.next()?;
                if let Some(current_entry) = &self.last_entry {
                    flat_object.push((
                        current_entry.0.1.clone(),
                        serde_json::Value::from(current_entry.1.clone()),
                    ));
                } else {
                    break;
                }
            }
            Ok(Some(Object {
                id: object_id,
                value: nest(flat_object),
            }))
        } else {
            Ok(None)
        }
    }
}

impl<'a, 'b, 'c> WriteTransaction<'a, 'b, 'c> {
    define_read_methods!();

    pub fn insert(&mut self, path: String, object: Object) -> Result<()> {
        match object.value {
            serde_json::Value::Object(map) => {
                for (key, internal_value) in map {
                    let internal_path = if path.is_empty() {
                        key
                    } else {
                        format!("{path}.{key}")
                    };
                    self.insert(
                        internal_path,
                        Object {
                            id: object.id.clone(),
                            value: internal_value,
                        },
                    )?;
                }
            }
            serde_json::Value::Array(array) => {
                let mut array_index = 0u64;
                let mut unique_internal_values: HashSet<serde_json::Value> = HashSet::new();
                for internal_value in array {
                    if unique_internal_values.contains(&internal_value) {
                        continue;
                    }
                    unique_internal_values.insert(internal_value.clone());
                    let key = format!("{:0>10}", array_index);
                    let internal_path = if path.is_empty() {
                        key
                    } else {
                        format!("{path}.{key}")
                    };
                    self.insert(
                        internal_path,
                        Object {
                            id: object.id.clone(),
                            value: internal_value.clone(),
                        },
                    )?;
                    array_index += 1;
                }
            }
            _ => {
                self.index_transaction
                    .database_transaction
                    .object_id_and_path_to_value
                    .insert((object.id, path), object.value.try_into()?);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{Chest, Object, ObjectId};
    use fallible_iterator::FallibleIterator;

    fn new_default_chest(test_name_for_isolation: &str) -> Chest {
        Chest::new(
            serde_saphyr::from_str(
                &std::fs::read_to_string("src/test_chest_config.yml")
                    .unwrap()
                    .replace("TEST_NAME", test_name_for_isolation),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn test_insert() {
        let mut chest = new_default_chest("test_insert");
        let object = Object {
            id: ObjectId::new(),
            value: json!({"key1": "value1", "key2": "value2"}),
        };

        chest
            .lock_all_and_write(|transaction| {
                transaction.insert("".to_string(), object.clone())?;
                assert_eq!(
                    transaction.objects()?.collect::<Vec<_>>()?,
                    vec![object.clone()]
                );
                Ok(())
            })
            .unwrap();
    }
}
