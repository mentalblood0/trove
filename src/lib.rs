use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::OnceLock;

use anyhow::{Context, Error, Result, anyhow};
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

#[derive(Debug, Clone, bincode::Encode, bincode::Decode, PartialOrd, Ord, PartialEq, Eq)]
pub enum PathSegment {
    JsonObjectKey(String),
    JsonArrayIndex(u32),
}

type Path = Vec<PathSegment>;

type FlatObject = Vec<(Path, Value)>;

pub fn nest(flat_object: &FlatObject) -> Result<Option<serde_json::Value>> {
    if flat_object.is_empty() {
        Ok(None)
    } else if flat_object[0].0.is_empty() {
        Ok(Some(flat_object[0].1.clone().into()))
    } else {
        let mut result = serde_json::Value::Null;
        for (path, value) in flat_object {
            let mut current = &mut result;
            for path_segment in path {
                match path_segment {
                    PathSegment::JsonObjectKey(json_object_key) => {
                        if *current == serde_json::Value::Null {
                            *current = serde_json::json!({});
                        }
                        let current_object = current
                            .as_object_mut()
                            .ok_or_else(|| anyhow!("Can not put value {value:?} at path {path:?} because can not put into array by object key"))?;
                        current_object.insert(json_object_key.clone(), value.clone().into());
                        current = current_object.get_mut(json_object_key).ok_or_else(|| {
                            anyhow!("Can not find value which just put at key {json_object_key:?}")
                        })?;
                    }
                    PathSegment::JsonArrayIndex(_) => {
                        if *current == serde_json::Value::Null {
                            *current = serde_json::json!([]);
                        }
                        let current_array =current 
                            .as_array_mut()
                            .ok_or_else(|| anyhow!("Can not put value {value:?} at path {path:?} because can not put into array by object key"))?;
                        current_array.push(value.clone().into());
                        current = current_array.last_mut().ok_or_else(|| {
                            anyhow!("Can not find value which just pushed")
                        })?;
                    }
                }
            }
        }
        Ok(Some(result))
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
    object_id_and_path_to_value<(ObjectId, Path), super::super::Value>,
} use {
    use crate::ObjectId;
    use crate::Path;
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
            index: trove_database::Index::new(config.index.clone()).with_context(|| {
                format!("Can not create chest with index config {:?}", config.index)
            })?,
        })
    }

    pub fn lock_all_and_write<'a, F>(&'a mut self, mut f: F) -> Result<&'a mut Self>
    where
        F: FnMut(&mut WriteTransaction<'_, '_, '_>) -> Result<()>,
    {
        self.index
            .lock_all_and_write(|index_write_transaction| {
                f(&mut WriteTransaction {
                    index_transaction: index_write_transaction,
                })
            })
            .with_context(|| "Can not lock index and initiate write transaction")?;

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
            })
            .with_context(
                || "Can not lock all write operations on index and initiate read transaction",
            )?;
        Ok(self)
    }
}

struct Digest {
    value: [u8; 16],
}

#[repr(u8)]
#[derive(Clone, Debug)]
pub enum IndexRecordType {
    Direct = 0,
    Array = 1,
}

impl Digest {
    fn of_data(data: &Vec<u8>) -> Self {
        Self {
            value: xxhash_rust::xxh3::xxh3_128(data).to_le_bytes(),
        }
    }

    fn of_pathvalue(
        index_record_type: IndexRecordType,
        path: &Path,
        value: &Value,
    ) -> Result<Self> {
        let encoded_path = bincode::encode_to_vec(path, bincode::config::standard())?;
        let encoded_value = bincode::encode_to_vec(value, bincode::config::standard())
            .with_context(|| format!("Can not binary encode value {value:?}"))?;
        let mut data: Vec<u8> =
            Vec::with_capacity(2 + encoded_path.len() + 1 + encoded_value.len());
        data.push(index_record_type as u8);
        data.push(0u8);
        data.extend_from_slice(&encoded_path);
        data.push(0u8);
        data.extend_from_slice(&encoded_value);
        Ok(Self::of_data(&data))
    }

    fn of_path_object_id_and_value(
        path: &Path,
        object_id: &ObjectId,
        value: &Value,
    ) -> Result<Self> {
        let encoded_path = bincode::encode_to_vec(path, bincode::config::standard())?;
        let encoded_value = bincode::encode_to_vec(value, bincode::config::standard())
            .with_context(|| format!("Can not binary encode value {value:?}"))?;
        let mut data: Vec<u8> =
            Vec::with_capacity(encoded_path.len() + 1 + 16 + encoded_value.len());
        data.extend_from_slice(&encoded_path);
        data.push(0u8);
        data.extend_from_slice(&object_id.value);
        data.extend_from_slice(&encoded_value);
        Ok(Self::of_data(&data))
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
        Box<dyn FallibleIterator<Item = ((ObjectId, Path), Value), Error = Error> + 'a>,
    last_entry: Option<((ObjectId, Path), Value)>,
}

macro_rules! define_read_methods {
    () => {
        pub fn objects(&'a self) -> Result<ObjectsIterator<'a>> {
            Ok(ObjectsIterator {
                data_table_iterator: self
                    .index_transaction
                    .database_transaction
                    .object_id_and_path_to_value
                    .iter(Bound::Unbounded, false)
                    .with_context(
                        || "Can not initiate iteration over object_id_and_path_to_value table",
                    )?,
                last_entry: None,
            })
        }

        pub fn get_flattened(
            &'a self,
            object_id: &ObjectId,
            path_prefix: &Path,
        ) -> Result<FlatObject> {
            let mut flat_object: FlatObject = Vec::new();
            let from_object_id_and_path =
                &(object_id.clone(), path_prefix.clone());
            let mut iterator = self
                .index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .iter(Bound::Included(from_object_id_and_path), false)
                .with_context(
                    || format!("Can not initiate iteration over object_id_and_path_to_value table from key {from_object_id_and_path:?}"),
                )?
                .take_while(|entry| {
                    Ok(entry.0.0 == *object_id
                        && (path_prefix.is_empty()
                            || entry.0.1 == *path_prefix
                            || entry.0.1.starts_with(&path_prefix)))
                });
            loop {
                if let Some(entry) = iterator.next()? {
                    flat_object.push((
                        entry.0.1.clone()[path_prefix.len()..].to_vec(),
                        entry.1,
                    ));
                } else {
                    break;
                }
            }
            Ok(flat_object)
        }

        pub fn get(
            &'a self,
            object_id: &ObjectId,
            path_prefix: &Path,
        ) -> Result<Option<serde_json::Value>> {
            nest(&self.get_flattened(object_id, path_prefix).with_context(|| format!("Can not get part at path prefix {path_prefix:?} of flattened object with id {object_id:?}"))?)
        }

        pub fn select(
            &'a self,
            presention_conditions: &Vec<(IndexRecordType, String, serde_json::Value)>,
            absention_conditions: &Vec<(IndexRecordType, String, serde_json::Value)>,
            start_after_object: Option<ObjectId>,
        ) -> Result<Box<dyn FallibleIterator<Item = ObjectId, Error = Error> + '_>> {
            let present_ids = {
                let mut result = Vec::new();
                for (index_record_type, path, value) in presention_conditions {
                    result.push(dream::Object::Identified(dream::Id {
                        value: Digest::of_pathvalue(
                            index_record_type.clone(),
                            &path,
                            &value.clone().try_into().with_context(|| format!("Can not convert json value {value:?} to database storable value so to select objects where ({index_record_type:?}, {path:?}, {value:?}) is present"))?,
                        ).with_context(|| format!("Can not compute digest of presention condition ({index_record_type:?}), {path:?}, {value:?}"))?
                        .value,
                    }));
                }
                result
            };
            let absent_ids = {
                let mut result = Vec::new();
                for (index_record_type, path, value) in absention_conditions {
                    result.push(dream::Object::Identified(dream::Id {
                        value: Digest::of_pathvalue(
                            index_record_type.clone(),
                            &path,
                            &value.clone().try_into().with_context(|| format!("Can not convert json value {value:?} to database storable value so to select objects where ({index_record_type:?}, {path:?}, {value:?}) is absent"))?,
                        ).with_context(|| format!("Can not compute digest of absention condition ({index_record_type:?}), {path:?}, {value:?}"))?
                        .value,
                    }));
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
                    ).with_context(|| format!("Can not initiate search in index with presention conditions {presention_conditions:?} and absention conditions {absention_conditions:?}"))?
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
            self.last_entry = self
                .data_table_iterator
                .next()
                .with_context(|| "Can not get first data table entry")?;
        }
        if let Some(first_object_entry) = self.last_entry.clone() {
            let object_id = first_object_entry.0.0;
            let mut flat_object: FlatObject = Vec::new();
            flat_object.push((
                first_object_entry.0.1,
                serde_json::Value::from(first_object_entry.1),
            ));
            loop {
                self.last_entry = self.data_table_iterator.next().with_context(|| {
                    format!(
                        "Can not get next data table entry after {:?}",
                        self.last_entry
                    )
                })?;
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
            Ok(nest(flat_object).and_then(|value| {
                Some(Object {
                    id: object_id,
                    value,
                })
            }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
struct IndexBatch {
    object_id: ObjectId,
    digests: Vec<dream::Object>,
    array_digests: HashMap<u32, Vec<dream::Object>>,
}

impl IndexBatch {
    fn new(object_id: ObjectId) -> Self {
        Self {
            object_id,
            digests: Vec::new(),
            array_digests: HashMap::new(),
        }
    }

    fn push(&mut self, path: String, value: Value) -> Result<&Self> {
        let partitioned_path = PartitionedPath::from_path(path.clone());
        if let Some(path_index) = partitioned_path.index {
            self.digests.push(dream::Object::Identified(dream::Id {
                value: Digest::of_pathvalue(
                    IndexRecordType::Array,
                    &partitioned_path.base,
                    &value.clone(),
                )
                .with_context(|| {
                    format!(
                        "Can not compute array type digest for path {:?} and value {:?}",
                        partitioned_path.base, value
                    )
                })?
                .value,
            }));
            self.array_digests
                .entry(path_index)
                .or_insert(Vec::new())
                .push(dream::Object::Identified(dream::Id {
                    value: Digest::of_path_object_id_and_value(
                        &partitioned_path.base,
                        &self.object_id,
                        &value,
                ).with_context(|| {
                    format!(
                        "Can not compute array type digest for path {:?}, object id {:?} and value {:?}",
                        partitioned_path.base, self.object_id, value
                    )
                })?
                    .value,
                }));
        } else {
            self.digests.push(dream::Object::Identified(dream::Id {
                value: Digest::of_pathvalue(IndexRecordType::Direct, &path, &value.clone())
                    .with_context(|| {
                        format!(
                            "Can not compute direct type digest for path {:?} and value {:?}",
                            path, value
                        )
                    })?
                    .value,
            }));
        }
        Ok(self)
    }

    fn u32_to_dream_id(input: u32) -> dream::Id {
        let mut value = [0u8; 16];
        value[12..].copy_from_slice(&input.to_be_bytes());
        dream::Id { value }
    }

    fn iter(&'_ self) -> Box<dyn Iterator<Item = (dream::Id, &Vec<dream::Object>)> + '_> {
        Box::new(
            vec![(
                dream::Id {
                    value: self.object_id.value,
                },
                &self.digests,
            )]
            .into_iter()
            .chain(self.array_digests.iter().map(
                |(path_index, path_index_paths_digests)| {
                    (Self::u32_to_dream_id(*path_index), path_index_paths_digests)
                },
            )),
        )
    }

    fn flush_insert(
        &self,
        index_transaction: &mut trove_database::WriteTransaction<'_, '_>,
    ) -> Result<&Self> {
        for (dream_id, tags) in self.iter() {
            index_transaction
                .insert(&dream::Object::Identified(dream_id.clone()), tags)
                .with_context(|| {
                    format!("Can not insert id-tags pair ({dream_id:?}, {tags:?}) into dream index")
                })?;
        }
        Ok(self)
    }

    fn flush_remove(
        &self,
        index_transaction: &mut trove_database::WriteTransaction<'_, '_>,
    ) -> Result<&Self> {
        for (dream_id, tags) in self.iter() {
            index_transaction
                .remove_tags_from_object(&dream::Object::Identified(dream_id.clone()), tags)
                .with_context(|| format!("Can not remove tags {tags:?} from object with id {dream_id:?} in dream index"))?;
        }
        Ok(self)
    }
}

fn escape_key(unescaped_key: &str) -> String {
    unescaped_key.replace('\\', "\\\\").replace('.', "\\.")
}

fn unescape_key(escaped_key: &str) -> String {
    escaped_key.replace("\\.", ".").replace("\\\\", "\\")
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
                flatten_to(&internal_path, &internal_value, result).with_context(|| {
                    format!("Can not get flat representation of value {internal_value:?} part at path {internal_path:?}")
                })?;
            }
        }
        serde_json::Value::Array(array) => {
            let mut array_index = 0u32;
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
                flatten_to(&internal_path, &internal_value, result).with_context(|| {
                    format!("Can not merge flat representation of value {internal_value:?} part at path {internal_path:?} into {result:?}")
                })?;
                array_index += 1;
            }
        }
        _ => {
            // println!("flatten to {}", path);
            result.push((
                path.to_string(),
                (*value).clone().try_into().with_context(|| {
                    format!("Can not convert json value {value:?} into database storable value")
                })?,
            ));
        }
    }
    Ok(())
}

fn flatten(path: &str, value: &serde_json::Value) -> Result<Vec<(String, Value)>> {
    let mut result: Vec<(String, Value)> = Vec::new();
    flatten_to(path, value, &mut result).with_context(|| {
        format!("Can not merge flat representation of value {value:?} part at path {path:?} into {result:?}")
    })?;
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
        for (internal_path, internal_value) in flatten(&path, &value)
            .with_context(|| format!("Can not flatten value {value:?} part at path {path:?}"))?
        {
            index_batch.push(internal_path.clone(), internal_value.clone()).with_context(|| format!("Can not push path-value pair ({internal_path:?}, {internal_value:?}) into index batch"))?;
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
        self.update_with_index(object_id.clone(), path.clone(), value.clone(), &mut index_batch).with_context(|| format!("Can not update object with id {object_id:?} with path-value pair ({path:?}, {value:?}) also updating index batch {index_batch:?}"))?;
        index_batch
            .flush_insert(self.index_transaction)
            .with_context(|| format!("Can not flush-insert index batch {index_batch:?}"))?;
        Ok(object_id)
    }

    pub fn insert(&mut self, value: serde_json::Value) -> Result<ObjectId> {
        let id = ObjectId::new();
        self.update(id.clone(), "".to_string(), value)
    }

    pub fn insert_with_id(&mut self, object: Object) -> Result<ObjectId> {
        self.update(object.id, "".to_string(), object.value)
    }

    pub fn remove(&mut self, object_id: &ObjectId, path_prefix: &str) -> Result<()> {
        let padded_path_prefix = pad(path_prefix);
        let from_object_id_and_path = &(object_id.clone(), padded_path_prefix.to_string());
        let paths_to_remove = self
            .index_transaction
            .database_transaction
            .object_id_and_path_to_value
            .iter(Bound::Included(from_object_id_and_path), false).with_context(|| format!("Can not initiate iteration over object_id_and_path_to_value table starting from key {from_object_id_and_path:?}"))?
            .take_while(|((current_object_id, current_path), _)| {
                Ok(*current_object_id == *object_id
                    && current_path.starts_with(&padded_path_prefix))
            })
            .collect::<Vec<_>>().with_context(|| format!("Can not collect from iteration over object_id_and_path_to_value table starting from key {from_object_id_and_path:?} taking while object id is {object_id:?} and path starts with {padded_path_prefix:?}"))?;
        let mut index_batch = IndexBatch::new(object_id.clone());
        for ((_, current_path), current_value) in paths_to_remove.into_iter() {
            self.index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .remove(&(object_id.clone(), current_path.clone()));
            index_batch
                .push(current_path.clone(), current_value.clone())
                .with_context(|| {
                    format!("Can not push path-value pair ({current_path:?}, {current_value:?}) into index batch")
                })?;
        }
        index_batch
            .flush_remove(self.index_transaction)
            .with_context(|| format!("Can not flush-remove index batch {index_batch:?}"))?;
        Ok(())
    }

    pub fn last(
        &self,
        object_id: &ObjectId,
        path: &str,
    ) -> Result<Option<(String, serde_json::Value)>> {
        let padded_path = pad(path);
        dbg!(&padded_path);
        let iter_from = Bound::Included(&(
            object_id.clone(),
            if padded_path.is_empty() {
                "9".to_string()
            } else {
                format!("{padded_path}.9")
            },
        ));
        dbg!(&iter_from);
        self.index_transaction
            .database_transaction
            .object_id_and_path_to_value
            .iter(iter_from, true)?
            .take_while(|((current_object_id, current_path), _)| {
                dbg!(&current_path);
                Ok(current_object_id == object_id && current_path.starts_with(&padded_path))
            })
            .map(|((_, current_path), _)| {
                let result_path = current_path[..if padded_path.is_empty() {
                    0
                } else {
                    padded_path.len() + 1
                } + 10]
                    .to_string();
                dbg!(&result_path);
                Ok((
                    result_path.clone(),
                    self.get(object_id, &result_path)?.ok_or_else(|| {
                        anyhow!("Can not get last element of array at path {result_path:?}")
                    })?,
                ))
            })
            .next()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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
            loop {
                let result: String = (0..self.rng.generate_range(1..50))
                    .map(|_| {
                        let c = self.rng.generate_range(32..127) as u8 as char;
                        c
                    })
                    .collect();
                if result.parse::<u32>().is_err() {
                    return serde_json::Value::String(result);
                }
            }
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
        let mut rng = WyRand::new_seed(0);
        let mut json_generator = RandomJsonGenerator::new(0);

        chest
            .lock_all_and_write(|transaction| {
                let mut previously_added_objects: BTreeMap<ObjectId, serde_json::Value> =
                    BTreeMap::new();
                for _ in 0..400 {
                    let action_id = if previously_added_objects.is_empty() {
                        1
                    } else {
                        rng.generate_range(1..=3)
                    };
                    match action_id {
                        1 => {
                            let new_objects = (0..1)
                                .map(|_| {
                                    let json = json_generator.generate(3);
                                    (transaction.insert(json.clone()).unwrap(), json)
                                })
                                .collect::<Vec<_>>();
                            for (object_id, object_value) in new_objects.iter() {
                                let result = transaction.get(&object_id, "")?.unwrap();
                                // println!("result = {}", serde_json::to_string(&result)?);
                                // println!(
                                //     "object_value = {}",
                                //     serde_json::to_string(&object_value)?
                                // );
                                assert_eq!(result, *object_value);
                                let flatten_object = flatten(&"", &object_value)?;
                                for (pathvalue_index, (path, value)) in
                                    flatten_object.iter().enumerate()
                                {
                                    println!("{path:?} = {value:?}");
                                    let value_as_json: serde_json::Value = value.clone().into();
                                    let partitioned_path = PartitionedPath::from_path(path.clone());
                                    let index_record_type = if partitioned_path.index.is_some() {
                                        IndexRecordType::Array
                                    } else {
                                        IndexRecordType::Direct
                                    };
                                    let select_path = if partitioned_path.index.is_some() {
                                        partitioned_path.base.clone()
                                    } else {
                                        path.clone()
                                    };
                                    let selected = transaction
                                        .select(
                                            &vec![(
                                                index_record_type,
                                                select_path.clone(),
                                                value_as_json.clone(),
                                            )],
                                            &vec![],
                                            None,
                                        )?
                                        .collect::<Vec<ObjectId>>()?;
                                    for selected_object_id in selected.iter() {
                                        if let Some(got) =
                                            &transaction.get(&selected_object_id, &select_path)?
                                        {
                                            if partitioned_path.index.is_some() {
                                                if let Some(got_array) = got.as_array() {
                                                    assert!(got_array.iter().any(
                                                        |got_array_element| {
                                                            got_array_element == &value_as_json
                                                        }
                                                    ));
                                                }
                                            } else {
                                                assert_eq!(got, &value_as_json);
                                            }
                                        }
                                    }
                                    assert!(selected.iter().any(|selected_object_id| {
                                        selected_object_id == object_id
                                    }));
                                    if partitioned_path.index.is_some() {
                                        if flatten_object
                                            .get(pathvalue_index + 1)
                                            .and_then(|(next_path, next_value)| {
                                                PartitionedPath::from_path(next_path.clone()).index
                                            })
                                            .is_none()
                                        {
                                            assert_eq!(
                                                transaction
                                                    .last(object_id, &partitioned_path.base)?
                                                    .unwrap()
                                                    .1,
                                                value_as_json
                                            );
                                        }
                                    }
                                }
                            }
                            previously_added_objects.extend(new_objects);
                            assert_eq!(
                                transaction
                                    .objects()?
                                    .map(|current_object| Ok((
                                        current_object.id,
                                        current_object.value
                                    )))
                                    .collect::<BTreeMap<_, _>>()?,
                                previously_added_objects
                            );
                        }
                        2 => {
                            let object_to_remove_id = previously_added_objects
                                .keys()
                                .nth(rng.generate_range(0..previously_added_objects.len()))
                                .unwrap()
                                .clone();
                            transaction.remove(&object_to_remove_id, "")?;
                            previously_added_objects.remove(&object_to_remove_id);
                            assert_eq!(transaction.get(&object_to_remove_id, "")?, None);
                        }
                        3 => {
                            let object_to_remove_from_id = previously_added_objects
                                .keys()
                                .nth(rng.generate_range(0..previously_added_objects.len()))
                                .unwrap()
                                .clone();
                            let flattened_object_to_remove_from =
                                transaction.get_flattened(&object_to_remove_from_id, "")?;
                            let path_to_remove = flattened_object_to_remove_from
                                [rng.generate_range(0..flattened_object_to_remove_from.len())]
                            .0
                            .clone();
                            let correct_result_option = nest(
                                flattened_object_to_remove_from
                                    .into_iter()
                                    .filter(|(path, _)| !path.starts_with(&path_to_remove))
                                    .map(|(path, value)| (path, value.into()))
                                    .collect::<Vec<_>>(),
                            );
                            transaction.remove(&object_to_remove_from_id, &path_to_remove)?;
                            previously_added_objects.remove(&object_to_remove_from_id);
                            if let Some(ref correct_result) = correct_result_option {
                                previously_added_objects.insert(
                                    object_to_remove_from_id.clone(),
                                    correct_result.clone(),
                                );
                            }
                            assert_eq!(
                                transaction.get(&object_to_remove_from_id, &path_to_remove)?,
                                None
                            );
                            assert_eq!(
                                transaction.get(&object_to_remove_from_id, "")?,
                                correct_result_option
                            );
                        }
                        _ => {}
                    }
                }
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
