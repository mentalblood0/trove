use std::collections::{HashMap, HashSet};

use anyhow::{Error, Result, anyhow};
use fallible_iterator::FallibleIterator;
use serde::{Deserialize, Serialize};

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

fn pad(path: &str) -> String {
    let re = regex::Regex::new(r"(:?\.|^)(\d{1,9})(:?\.|$)").unwrap();
    re.replace_all(&path, |caps: &regex::Captures| {
        let segment = caps.get(0).unwrap().as_str();
        format!("{:0>10}", segment)
    })
    .to_string()
}

fn insert_into_map(
    map: &mut serde_json::Map<String, serde_json::Value>,
    parts: Vec<String>,
    value: serde_json::Value,
) {
    if parts.len() == 1 {
        map.insert(parts[0].to_string(), value);
    } else {
        let first = parts[0].to_string();
        let rest = parts[1..].to_vec();
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

fn split_on_unescaped_dots(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            if ch != '.' {
                current.push('\\');
            }
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '.' {
            if !current.is_empty() {
                result.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }
    if escaped {
        current.push('\\');
    }
    if !current.is_empty() {
        result.push(current);
    }
    result
}

fn nest(flat_object: FlatObject) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (path, value) in flat_object {
        let parts = split_on_unescaped_dots(&path);
        insert_into_map(
            &mut map,
            parts
                .iter()
                .map(|escaped_key| unescape_key(escaped_key))
                .collect::<Vec<_>>(),
            value,
        );
    }
    serde_json::Value::Object(map)
}

fn process_arrays(nested_object: serde_json::Value) -> serde_json::Value {
    match nested_object {
        serde_json::Value::Object(map) => {
            if let Ok(mut pairs) =
                fallible_iterator::convert(map.iter().map(|keyvalue| Ok::<_, Error>(keyvalue)))
                    .map(|(key, value)| Ok((key.parse::<u64>()?, value.clone())))
                    .collect::<Vec<_>>()
            {
                pairs.sort_by_key(|(key, _)| key.clone());
                serde_json::Value::Array(
                    pairs
                        .into_iter()
                        .map(|(_, value)| process_arrays(value))
                        .collect::<Vec<_>>(),
                )
            } else {
                serde_json::Value::Object(
                    map.into_iter()
                        .map(|(key, value)| (key, process_arrays(value)))
                        .collect::<serde_json::Map<_, _>>(),
                )
            }
        }
        serde_json::Value::Array(vec) => serde_json::Value::Array(
            vec.into_iter()
                .map(|value| process_arrays(value))
                .collect::<Vec<_>>(),
        ),
        _ => nested_object,
    }
}

#[derive(bincode::Encode, bincode::Decode, Clone, Debug)]
enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Bool(bool),
    String(String),
}

impl TryFrom<serde_json::Value> for Value {
    type Error = Error;

    fn try_from(json_value: serde_json::Value) -> Result<Self> {
        match json_value {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Integer(i as i64))
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

pub struct Chest {
    pub index: trove_database::Index,
}

#[derive(Serialize, Deserialize)]
pub struct ChestConfig {
    pub index: trove_database::IndexConfig,
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
    fn of_data(data: &Vec<u8>) -> Self {
        Self {
            value: xxhash_rust::xxh3::xxh3_128(data).to_le_bytes(),
        }
    }

    fn of_pathvalue(path: &str, value: &Value) -> Result<Self> {
        let encoded_value = bincode::encode_to_vec(value, bincode::config::standard())?;
        let mut data: Vec<u8> = Vec::with_capacity(path.len() + 1 + encoded_value.len());
        data.extend_from_slice(path.as_bytes());
        data.push(0u8);
        data.extend_from_slice(&encoded_value);
        Ok(Self::of_data(&data))
    }

    fn of_path_object_id_and_value(
        path: &str,
        object_id: &ObjectId,
        value: &Value,
    ) -> Result<Self> {
        let encoded_value = bincode::encode_to_vec(value, bincode::config::standard())?;
        let mut data: Vec<u8> = Vec::with_capacity(path.len() + 1 + 16 + encoded_value.len());
        data.extend_from_slice(path.as_bytes());
        data.push(0u8);
        data.extend_from_slice(&object_id.value);
        data.extend_from_slice(&encoded_value);
        Ok(Self::of_data(&data))
    }
}

struct PartitionedPath {
    base: String,
    index: Option<u64>,
}

impl PartitionedPath {
    fn from_path(path: String) -> Self {
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

        pub fn get(
            &'a self,
            object_id: &ObjectId,
            path_prefix: &str,
        ) -> Result<Option<serde_json::Value>> {
            let padded_path_prefix = pad(path_prefix);
            // println!("get {object_id:?} {padded_path_prefix:?}");
            let mut flat_object: FlatObject = Vec::new();
            let mut iterator = self
                .index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .iter(Some(&(object_id.clone(), padded_path_prefix.to_string())))?
                .take_while(|entry| {
                    Ok(entry.0.0 == *object_id && entry.0.1.starts_with(&padded_path_prefix))
                });
            loop {
                if let Some(entry) = iterator.next()? {
                    let start_from = if padded_path_prefix.is_empty() {
                        0
                    } else {
                        entry.0.1.len().min(padded_path_prefix.len() + 1)
                    };
                    flat_object.push((
                        entry.0.1.clone()[start_from..].to_string(),
                        serde_json::Value::from(entry.1.clone()),
                    ));
                } else {
                    break;
                }
            }
            // dbg!(&flat_object);
            if flat_object.is_empty() {
                Ok(None)
            } else if flat_object.len() == 1 && flat_object[0].0 == "" {
                Ok(Some(flat_object[0].clone().1))
            } else {
                let nested = nest(flat_object);
                Ok(Some(process_arrays(nested)))
            }
        }

        pub fn select(
            &'a self,
            present_pathvalues: &Vec<(String, serde_json::Value)>,
            absent_pathvalues: &Vec<(String, serde_json::Value)>,
            start_after_object: Option<ObjectId>,
        ) -> Result<Box<dyn FallibleIterator<Item = ObjectId, Error = Error> + '_>> {
            // println!("select {present_pathvalues:?}");
            let present_ids = {
                let mut result = Vec::new();
                for (path, value) in present_pathvalues {
                    result.push(dream::Object::Identified(IndexBatch::get_dream_id(
                        path.clone(),
                        &(*value).clone().try_into()?,
                    )?));
                }
                result
            };
            let absent_ids = {
                let mut result = Vec::new();
                for (path, value) in absent_pathvalues {
                    result.push(dream::Object::Identified(IndexBatch::get_dream_id(
                        path.clone(),
                        &(*value).clone().try_into()?,
                    )?));
                }
                result
            };
            Ok(Box::new(
                self.index_transaction
                    .search(
                        &present_ids,
                        &absent_ids,
                        start_after_object.and_then(|start_after_object| {
                            Some(dream::Id {
                                value: start_after_object.value,
                            })
                        }),
                    )?
                    .map(|dream_id| {
                        Ok(ObjectId {
                            value: dream_id.value,
                        })
                    }),
            ))
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
                    if current_entry.0.0 != object_id {
                        break;
                    }
                    flat_object.push((
                        current_entry.0.1.clone(),
                        serde_json::Value::from(current_entry.1.clone()),
                    ));
                } else {
                    break;
                }
            }
            if flat_object.len() == 1 && flat_object[0].0 == "" {
                Ok(Some(Object {
                    id: object_id,
                    value: flat_object[0].clone().1,
                }))
            } else {
                let nested = nest(flat_object);
                let processed = process_arrays(nested);
                Ok(Some(Object {
                    id: object_id,
                    value: processed,
                }))
            }
        } else {
            Ok(None)
        }
    }
}

struct IndexBatch {
    object_id: ObjectId,
    digests: Vec<dream::Object>,
    array_digests: HashMap<u64, Vec<dream::Object>>,
}

impl IndexBatch {
    fn new(object_id: ObjectId) -> Self {
        Self {
            object_id,
            digests: Vec::new(),
            array_digests: HashMap::new(),
        }
    }

    fn get_dream_id(path: String, value: &Value) -> Result<dream::Id> {
        let partitioned_path = PartitionedPath::from_path(path.clone());
        let result = dream::Id {
            value: Digest::of_pathvalue(&partitioned_path.base, value)?.value,
        };
        Ok(result)
    }

    fn push(&mut self, path: String, value: Value) -> Result<&Self> {
        let partitioned_path = PartitionedPath::from_path(path.clone());
        if let Some(path_index) = partitioned_path.index {
            self.digests
                .push(dream::Object::Identified(IndexBatch::get_dream_id(
                    partitioned_path.base.clone(),
                    &value,
                )?));
            self.array_digests
                .entry(path_index)
                .or_insert(Vec::new())
                .push(dream::Object::Identified(dream::Id {
                    value: Digest::of_path_object_id_and_value(
                        &partitioned_path.base,
                        &self.object_id,
                        &value,
                    )?
                    .value,
                }));
        } else {
            self.digests
                .push(dream::Object::Identified(IndexBatch::get_dream_id(
                    path.clone(),
                    &value,
                )?));
        }
        Ok(self)
    }

    fn u64_to_dream_object(input: u64) -> dream::Object {
        let mut value = [0u8; 16];
        value[8..].copy_from_slice(&input.to_be_bytes());
        dream::Object::Identified(dream::Id { value })
    }

    fn flush_insert(
        &self,
        index_transaction: &mut trove_database::WriteTransaction<'_, '_>,
    ) -> Result<&Self> {
        let id = &dream::Object::Identified(dream::Id {
            value: self.object_id.value,
        });
        index_transaction.insert(id, &self.digests)?;
        for (path_index, path_index_paths_digests) in self.array_digests.iter() {
            index_transaction.insert(
                &Self::u64_to_dream_object(*path_index),
                path_index_paths_digests,
            )?;
        }
        Ok(self)
    }

    fn flush_remove(
        &self,
        index_transaction: &mut trove_database::WriteTransaction<'_, '_>,
    ) -> Result<&Self> {
        index_transaction.remove_object(&dream::Object::Identified(dream::Id {
            value: self.object_id.value,
        }))?;
        for (path_index, path_index_paths_digests) in self.array_digests.iter() {
            index_transaction.remove_tags_from_object(
                &Self::u64_to_dream_object(*path_index),
                path_index_paths_digests,
            )?;
        }
        Ok(self)
    }
}

fn escape_key(unescaped_key: &str) -> String {
    let mut result = unescaped_key.replace(".", "\\.");
    if result.chars().last() == Some('\\') {
        result.push('\\');
    }
    result
}

fn unescape_key(escaped_key: &str) -> String {
    let mut result = escaped_key.replace("\\.", ".");
    if result.chars().last() == Some('\\') {
        result.pop();
    }
    result
}

fn flatten_to(
    path: &str,
    value: &serde_json::Value,
    result: &mut Vec<(String, Value)>,
) -> Result<()> {
    match value {
        serde_json::Value::Object(map) => {
            for (key, internal_value) in map {
                let escaped_key = escape_key(&key);
                let internal_path = if path.is_empty() {
                    escaped_key
                } else {
                    format!("{path}.{escaped_key}")
                };
                flatten_to(&internal_path, &internal_value, result)?;
            }
        }
        serde_json::Value::Array(array) => {
            let mut array_index = 0u64;
            let mut unique_internal_values: HashSet<serde_json::Value> = HashSet::new();
            for internal_value in array {
                match internal_value {
                    serde_json::Value::Array(_) => {}
                    serde_json::Value::Object(_) => {}
                    _ => {
                        if unique_internal_values.contains(&internal_value) {
                            continue;
                        }
                        unique_internal_values.insert(internal_value.clone());
                    }
                }
                let key = format!("{:0>10}", array_index);
                let internal_path = if path.is_empty() {
                    key
                } else {
                    format!("{path}.{key}")
                };
                flatten_to(&internal_path, &internal_value, result)?;
                array_index += 1;
            }
        }
        _ => {
            result.push((path.to_string(), (*value).clone().try_into()?));
        }
    }
    Ok(())
}

fn flatten(path: &str, value: &serde_json::Value) -> Result<Vec<(String, Value)>> {
    let mut result: Vec<(String, Value)> = Vec::new();
    flatten_to(path, value, &mut result)?;
    Ok(result)
}

impl<'a, 'b, 'c> WriteTransaction<'a, 'b, 'c> {
    define_read_methods!();

    fn update_with_index(
        &mut self,
        object_id: ObjectId,
        path: String,
        value: serde_json::Value,
        index_batch: &mut IndexBatch,
    ) -> Result<ObjectId> {
        for (internal_path, internal_value) in flatten(&path, &value)? {
            index_batch.push(internal_path.clone(), internal_value.clone())?;
            self.index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .insert((object_id.clone(), internal_path), internal_value);
        }
        Ok(object_id)
    }

    pub fn update(
        &mut self,
        object_id: ObjectId,
        path: String,
        value: serde_json::Value,
    ) -> Result<ObjectId> {
        let mut index_batch = IndexBatch::new(object_id.clone());
        self.update_with_index(object_id.clone(), path, value, &mut index_batch)?;
        index_batch.flush_insert(self.index_transaction)?;
        Ok(object_id)
    }

    pub fn insert(&mut self, value: serde_json::Value) -> Result<ObjectId> {
        let id = ObjectId::new();
        self.update(id.clone(), "".to_string(), value)
    }
}

#[cfg(test)]
mod tests {
    use nanorand::{Rng, WyRand};
    use serde_json::json;

    use super::*;
    use crate::{Chest, Object};
    use fallible_iterator::FallibleIterator;
    use pretty_assertions::assert_eq;

    struct RandomJsonGenerator {
        rng: WyRand,
        max_array_size: usize,
        max_object_size: usize,
    }

    impl RandomJsonGenerator {
        fn new(seed: u64) -> Self {
            Self {
                rng: WyRand::new_seed(seed),
                max_array_size: 10,
                max_object_size: 10,
            }
        }

        fn generate(&mut self, depth: usize) -> serde_json::Value {
            if depth == 0 {
                self.generate_primitive()
            } else {
                let choice = self.rng.generate_range(0..=7);
                match choice {
                    0 => serde_json::Value::Null,
                    4 => self.generate_array(depth),
                    5 => self.generate_object(depth),
                    _ => self.generate_primitive(),
                }
            }
        }

        fn generate_string(&mut self) -> serde_json::Value {
            serde_json::Value::String(
                (0..self.rng.generate_range(1..50))
                    .map(|_| {
                        let c = self.rng.generate_range(32..127) as u8 as char;
                        c
                    })
                    .collect(),
            )
        }

        fn generate_array(&mut self, depth: usize) -> serde_json::Value {
            let size = self.rng.generate_range(1..self.max_array_size);
            let array: Vec<serde_json::Value> = {
                let mut result = Vec::new();
                let mut unique_primitive_values = Vec::new();
                for _ in 0..size {
                    let value = self.generate(depth - 1);
                    match value {
                        serde_json::Value::Array(_) => {}
                        serde_json::Value::Object(_) => {}
                        _ => {
                            if unique_primitive_values.contains(&value) {
                                continue;
                            }
                            unique_primitive_values.push(value.clone());
                        }
                    }
                    result.push(value);
                }
                result
            };
            serde_json::Value::Array(array)
        }

        fn generate_object(&mut self, depth: usize) -> serde_json::Value {
            let generated_object = serde_json::Value::Object(
                (0..self.rng.generate_range(1..self.max_object_size))
                    .map(|_| {
                        (
                            self.generate_string().as_str().unwrap().to_string(),
                            self.generate(depth - 1),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>(),
            );
            generated_object
        }

        fn generate_primitive(&mut self) -> serde_json::Value {
            match self.rng.generate_range(0..5) {
                0 => serde_json::Value::Null,
                1 => serde_json::Value::Bool(self.rng.generate()),
                2 => json!(self.rng.generate::<i64>()),
                3 => json!(self.rng.generate::<f64>()),
                _ => self.generate_string(),
            }
        }
    }

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
    fn test_generative() {
        let mut chest = new_default_chest("test_generative");
        let mut json_generator = RandomJsonGenerator::new(0);

        chest
            .lock_all_and_write(|transaction| {
                let objects = {
                    let mut result = Vec::new();
                    for _ in 0..4 {
                        let json = json_generator.generate(3);
                        let generated_object = Object {
                            id: transaction.insert(json.clone())?,
                            value: json,
                        };
                        // dbg!(&generated_object);
                        result.push(generated_object);
                    }
                    result
                };
                for object in objects.iter() {
                    assert_eq!(transaction.get(&object.id, "")?.unwrap(), object.value);
                    for (path, value) in flatten(&"", &object.value)? {
                        // println!("{:?} {path:?} = {value:?}", &object.id);
                        let value_as_json: serde_json::Value = value.into();
                        let partitioned_path = PartitionedPath::from_path(path.clone());
                        let select_path = if partitioned_path.index.is_some() {
                            partitioned_path.base
                        } else {
                            path.clone()
                        };
                        let selected = transaction
                            .select(
                                &vec![(select_path.clone(), value_as_json.clone())],
                                &vec![],
                                None,
                            )?
                            .collect::<Vec<ObjectId>>()?;
                        // dbg!(&selected);
                        for object_id in selected.iter() {
                            let got = &transaction.get(&object_id, &select_path)?.unwrap();
                            if partitioned_path.index.is_some() {
                                assert!(got.as_array().unwrap().iter().any(|got_array_element| {
                                    got_array_element == &value_as_json
                                }));
                            } else {
                                assert_eq!(got, &value_as_json);
                            }
                        }
                        assert!(selected.iter().any(|object_id| *object_id == object.id));
                    }
                }
                assert_eq!(transaction.objects()?.collect::<Vec<_>>()?, objects);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_dots_in_keys() {
        let mut chest = new_default_chest("test_simple");
        let object_json = json!({"a.b.c": 1});

        chest
            .lock_all_and_write(|transaction| {
                let object = Object {
                    id: transaction.insert(object_json.clone())?,
                    value: object_json.clone(),
                };
                assert_eq!(
                    transaction.objects()?.collect::<Vec<_>>()?,
                    vec![object.clone()]
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_simple() {
        let mut chest = new_default_chest("test_simple");
        let object_json = json!({
            "dict": {
                "hello": ["number", 42, -4.2, 0.0],
                "boolean": false
            },
            "null": null,
            "array": [1, ["two", false], [null]]
        });

        chest
            .lock_all_and_write(|transaction| {
                let object = Object {
                    id: transaction.insert(object_json.clone())?,
                    value: object_json.clone(),
                };

                assert_eq!(
                    transaction.objects()?.collect::<Vec<_>>()?,
                    vec![object.clone()]
                );

                assert_eq!(
                    &transaction.get(&object.id, "dict")?.unwrap(),
                    object.value.as_object().unwrap().get("dict").unwrap()
                );
                assert_eq!(
                    &transaction.get(&object.id, "dict.hello")?.unwrap(),
                    object
                        .value
                        .as_object()
                        .unwrap()
                        .get("dict")
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get("hello")
                        .unwrap()
                );
                assert_eq!(
                    &transaction.get(&object.id, "dict.boolean")?.unwrap(),
                    object
                        .value
                        .as_object()
                        .unwrap()
                        .get("dict")
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get("boolean")
                        .unwrap()
                );
                assert_eq!(
                    transaction.get(&object.id, "dict.hello.0")?.unwrap(),
                    object
                        .value
                        .as_object()
                        .unwrap()
                        .get("dict")
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get("hello")
                        .unwrap()
                        .as_array()
                        .unwrap()[0]
                );
                assert_eq!(
                    transaction.get(&object.id, "dict.hello.1")?.unwrap(),
                    object
                        .value
                        .as_object()
                        .unwrap()
                        .get("dict")
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get("hello")
                        .unwrap()
                        .as_array()
                        .unwrap()[1]
                );
                assert_eq!(
                    transaction.get(&object.id, "dict.hello.2")?.unwrap(),
                    object
                        .value
                        .as_object()
                        .unwrap()
                        .get("dict")
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get("hello")
                        .unwrap()
                        .as_array()
                        .unwrap()[2]
                );
                assert_eq!(
                    transaction.get(&object.id, "dict.hello.3")?.unwrap(),
                    object
                        .value
                        .as_object()
                        .unwrap()
                        .get("dict")
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get("hello")
                        .unwrap()
                        .as_array()
                        .unwrap()[3]
                );

                Ok(())
            })
            .unwrap();
    }
}
