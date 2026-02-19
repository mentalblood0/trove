//! # Trove
//!
//! A JSON document database with indexed storage built on top of [dream](https://github.com/robamu/dream) index.
//!
//! ## Overview
//!
//! Trove provides a transactional storage for JSON documents with the following features:
//! - Document storage with auto-generated UUID v7 identifiers
//! - Path-based access to nested JSON structures
//! - Indexed queries based on path-value presence/absence
//! - ACID-compliant transactions (write and read)
//! - Flat storage with nested reconstruction
//!
//! ## Architecture
//!
//! The library uses:
//! - **dream** for inverted indexing
//! - **bincode** for efficient binary encoding
//! - **serde_json** for JSON handling
//! - **xxhash** for digest computation

use std::collections::HashMap;
use std::ops::Bound;

use anyhow::{anyhow, Context, Error, Result};
use fallible_iterator::FallibleIterator;
use serde::{Deserialize, Serialize};

/// A globally unique identifier for objects in the database.
///
/// Generated using UUID v7 for time-ordered unique identifiers.
/// Serialized as base64 URL-safe without padding for compact storage.
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
    pub fn to_string(&self) -> String {
        serde_json::to_value(self)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }
}

impl serde::Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(&self.value)
            .serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use base64::Engine;
        let s = String::deserialize(deserializer)?;
        let value = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        let value: [u8; 16] = value.try_into().map_err(|v: Vec<u8>| {
            serde::de::Error::custom(format!("expected 16 bytes, got {}", v.len()))
        })?;
        Ok(ObjectId { value })
    }
}

/// A JSON document stored in the database.
///
/// Contains a unique identifier and its JSON value.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Object {
    /// The unique identifier of this object.
    pub id: ObjectId,
    /// The JSON value stored in this object.
    pub value: serde_json::Value,
}

/// Represents a single segment in a JSON path.
///
/// Paths are constructed from segments that can be either:
/// - `JsonObjectKey`: A string key for accessing object properties
/// - `JsonArrayIndex`: A numeric index for accessing array elements
#[derive(Debug, Clone, bincode::Encode, bincode::Decode, PartialOrd, Ord, PartialEq, Eq)]
pub enum PathSegment {
    /// A key name for accessing a JSON object property.
    JsonObjectKey(String),
    /// A zero-based index for accessing a JSON array element.
    JsonArrayIndex(u32),
}

/// A path through a JSON document.
///
/// Constructed from a vector of [`PathSegment`] values. Use the [`path_segments!`] macro
/// for convenient construction.
type Path = Vec<PathSegment>;

/// A flattened representation of a JSON object.
///
/// Each entry contains a path to a leaf value and the value itself.
/// This format is used internally for storage and is converted to/from
/// nested JSON using [`nest`] and [`flatten`].
type FlatObject = Vec<(Path, Value)>;

/// Reconstructs a nested JSON value from a flat representation.
///
/// Takes a [`FlatObject`] (a list of path-value pairs) and reconstructs
/// the original nested JSON structure.
///
/// # Arguments
///
/// * `flat_object` - A flat representation containing paths and values.
///
/// # Returns
///
/// - `Ok(Some(value))` - The reconstructed JSON value
/// - `Ok(None)` - If the flat object was empty
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
                        current = current
                            .as_object_mut()
                            .unwrap()
                            .entry(json_object_key)
                            .or_insert(serde_json::Value::Null);
                    }
                    PathSegment::JsonArrayIndex(json_array_index) => {
                        if *current == serde_json::Value::Null {
                            *current = serde_json::json!([]);
                        }
                        if current.as_array().unwrap().len() <= *json_array_index as usize {
                            current
                                .as_array_mut()
                                .unwrap()
                                .push(serde_json::Value::Null);
                        }
                        let current_len = current.as_array().unwrap().len();
                        current = current
                            .as_array_mut()
                            .unwrap()
                            .get_mut((current_len - 1).min(*json_array_index as usize))
                            .unwrap();
                    }
                }
            }
            *current = value.clone().into();
        }
        Ok(Some(result))
    }
}

/// A storable value type that can be persisted in the database.
///
/// This enum represents primitive JSON values that can be stored.
/// Arrays and objects are not directly storable - they are flattened
/// into path-value pairs using [`flatten`].
///
/// # Variants
///
/// - `Null` - Represents JSON null
/// - `Integer` - A 64-bit signed integer
/// - `Float` - A 64-bit floating point number
/// - `Bool` - A boolean value
/// - `String` - A UTF-8 string
#[derive(bincode::Encode, bincode::Decode, Clone, Debug)]
pub enum Value {
    /// The null value.
    Null,
    /// A signed 64-bit integer.
    Integer(i64),
    /// A 64-bit floating point number.
    Float(f64),
    /// A boolean value.
    Bool(bool),
    /// A UTF-8 string.
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

/// The main database struct that provides access to the storage.
///
/// A `Chest` is the entry point for all database operations. It wraps
/// the underlying Dream index and provides methods for creating transactions.
pub struct Chest {
    /// The underlying Dream index.
    pub index: trove_database::Index,
}

/// Configuration for a [`Chest`].
///
/// Contains the configuration for the underlying Dream index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChestConfig {
    /// The index configuration.
    pub index: trove_database::IndexConfig,
}

impl Chest {
    /// Creates a new [`Chest`] with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The chest configuration containing index settings.
    ///
    /// # Returns
    ///
    /// A new `Chest` instance, or an error if the index cannot be created.
    pub fn new(config: ChestConfig) -> Result<Self> {
        Ok(Self {
            index: trove_database::Index::new(config.index.clone()).with_context(|| {
                format!("Can not create chest with index config {:?}", config.index)
            })?,
        })
    }

    /// Acquires an exclusive write lock and performs operations in a transaction.
    ///
    /// This method locks all writes to the database and calls the provided
    /// closure with a [`WriteTransaction`]. The closure can perform any number
    /// of read and write operations.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that receives a mutable `WriteTransaction` and returns `Result<()>`.
    ///
    /// # Returns
    ///
    /// `Ok(R)` on success, where `R` is what user-provided closure returns.
    pub fn lock_all_and_write<'a, F, R>(&'a mut self, mut f: F) -> Result<R>
    where
        F: FnMut(&mut WriteTransaction<'_, '_, '_>) -> Result<R>,
    {
        self.index
            .lock_all_and_write(|index_write_transaction| {
                f(&mut WriteTransaction {
                    index_transaction: index_write_transaction,
                })
            })
            .with_context(|| "Can not lock index and initiate write transaction")
    }

    /// Acquires all write locks and performs read-only operations.
    ///
    /// This method prevents any writes from occurring during the read operation
    /// and calls the provided closure with a [`ReadTransaction`].
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that receives a `ReadTransaction` and returns `Result<()>`.
    ///
    /// # Returns
    ///
    /// `Ok(R)` on success, where `R` is what user-provided closure returns.
    pub fn lock_all_writes_and_read<F, R>(&self, mut f: F) -> Result<R>
    where
        F: FnMut(ReadTransaction) -> Result<R>,
    {
        self.index
            .lock_all_writes_and_read(|index_read_transaction| {
                f(ReadTransaction {
                    index_transaction: index_read_transaction,
                })
            })
            .with_context(|| {
                "Can not lock all write operations on index and initiate read transaction"
            })
    }
}

struct Digest {
    value: [u8; 16],
}

/// The type of index record, determining how values are indexed.
///
/// This affects how values are matched during [`select`] queries:
/// - `Direct`: Matches exact path-value pairs
/// - `Array`: Matches values within arrays
#[repr(u8)]
#[derive(Clone, Debug)]
pub enum IndexRecordType {
    /// Direct path-value indexing.
    /// Use this for object properties and scalar values.
    Direct = 0,
    /// Use this for values inside arrays.
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

/// A read-only transaction for querying the database.
///
/// Provides methods to read objects and their values without modification.
/// Created via [`Chest::lock_all_writes_and_read`].
pub struct ReadTransaction<'a> {
    /// The underlying Dream read transaction.
    pub index_transaction: trove_database::ReadTransaction<'a>,
}

/// A write transaction for modifying the database.
///
/// Provides methods to insert, update, and remove objects.
/// Created via [`Chest::lock_all_and_write`].
pub struct WriteTransaction<'a, 'b, 'c> {
    /// The underlying Dream write transaction.
    pub index_transaction: &'a mut trove_database::WriteTransaction<'b, 'c>,
}

/// An iterator over all objects in the database.
///
/// Yields complete [`Object`] instances with nested JSON values.
/// Created via [`ReadTransaction::objects`].
///
/// This iterator groups all path-value pairs belonging to the same object.
pub struct ObjectsIterator<'a> {
    data_table_iterator:
        Box<dyn FallibleIterator<Item = ((ObjectId, Path), Value), Error = Error> + 'a>,
    last_entry: Option<((ObjectId, Path), Value)>,
}

macro_rules! define_read_methods {
    () => {
        /// Returns an iterator over all objects in the database.
        ///
        /// Each yielded object contains the complete nested JSON structure.
        ///
        /// # Returns
        ///
        /// An iterator yielding `Result<Object>`.
        pub fn objects(&'a self) -> Result<ObjectsIterator<'a>> {
            Ok(ObjectsIterator {
                data_table_iterator: self
                    .index_transaction
                    .database_transaction
                    .object_id_and_path_to_value
                    .iter(Bound::Unbounded, false)
                    .with_context(|| {
                        "Can not initiate iteration over object_id_and_path_to_value table"
                    })?,
                last_entry: None,
            })
        }

        /// Gets a flattened representation of an object's value at a path prefix.
        ///
        /// Returns path-value pairs relative to the given path prefix.
        ///
        /// # Arguments
        ///
        /// * `object_id` - The ID of the object to query.
        /// * `path_prefix` - The path prefix to start from (empty vec for entire object).
        ///
        /// # Returns
        ///
        /// A `FlatObject` containing all path-value pairs under the prefix.
        pub fn get_flattened(
            &'a self,
            object_id: &ObjectId,
            path_prefix: &Path,
        ) -> Result<FlatObject> {
            let mut flat_object: FlatObject = Vec::new();
            let from_object_id_and_path = &(object_id.clone(), path_prefix.clone());
            let mut iterator = self
                .index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .iter(Bound::Included(from_object_id_and_path), false)
                .with_context(|| {
                    format!(
                        "Can not initiate iteration over object_id_and_path_to_value table from \
                         key {from_object_id_and_path:?}"
                    )
                })?
                .take_while(|entry| {
                    Ok(entry.0 .0 == *object_id
                        && (path_prefix.is_empty()
                            || entry.0 .1 == *path_prefix
                            || entry.0 .1.starts_with(&path_prefix)))
                });
            loop {
                if let Some(entry) = iterator.next()? {
                    flat_object.push((entry.0 .1.clone()[path_prefix.len()..].to_vec(), entry.1));
                } else {
                    break;
                }
            }
            Ok(flat_object)
        }

        /// Gets a nested JSON value from an object at a given path.
        ///
        /// Retrieves the value at the specified path, reconstructing nested
        /// structures from the flat storage.
        ///
        /// # Arguments
        ///
        /// * `object_id` - The ID of the object to query.
        /// * `path_prefix` - The path to the value.
        ///
        /// # Returns
        ///
        /// - `Ok(Some(value))` - The value at the path.
        /// - `Ok(None)` - If no value exists at the path.
        pub fn get(
            &'a self,
            object_id: &ObjectId,
            path_prefix: &Path,
        ) -> Result<Option<serde_json::Value>> {
            nest(
                &self
                    .get_flattened(object_id, path_prefix)
                    .with_context(|| {
                        format!(
                            "Can not get part at path prefix {path_prefix:?} of flattened object \
                             with id {object_id:?}"
                        )
                    })?,
            )
        }

        /// Selects objects matching specified presence and absence conditions.
        ///
        /// Uses the inverted index to find objects that contain certain values
        /// at certain paths (presention_conditions) and do NOT contain others
        /// (absention_conditions).
        ///
        /// # Arguments
        ///
        /// * `presention_conditions` - Values that must be present.
        /// * `absention_conditions` - Values that must be absent.
        /// * `start_after_object` - Pagination: start after this object ID.
        ///
        /// # Returns
        ///
        /// An iterator yielding matching `ObjectId`s.
        pub fn select(
            &'a self,
            presention_conditions: &Vec<(IndexRecordType, Path, serde_json::Value)>,
            absention_conditions: &Vec<(IndexRecordType, Path, serde_json::Value)>,
            start_after_object: Option<ObjectId>,
        ) -> Result<Box<dyn FallibleIterator<Item = ObjectId, Error = Error> + '_>> {
            let present_ids = {
                let mut result = Vec::new();
                for (index_record_type, path, value) in presention_conditions {
                    result.push(dream::Object::Identified(dream::Id {
                        value: Digest::of_pathvalue(
                            index_record_type.clone(),
                            &path,
                            &value.clone().try_into().with_context(|| {
                                format!(
                                    "Can not convert json value {value:?} to database storable \
                                     value so to select objects where ({index_record_type:?}, \
                                     {path:?}, {value:?}) is present"
                                )
                            })?,
                        )
                        .with_context(|| {
                            format!(
                                "Can not compute digest of presention condition \
                                 ({index_record_type:?}), {path:?}, {value:?}"
                            )
                        })?
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
                            &value.clone().try_into().with_context(|| {
                                format!(
                                    "Can not convert json value {value:?} to database storable \
                                     value so to select objects where ({index_record_type:?}, \
                                     {path:?}, {value:?}) is absent"
                                )
                            })?,
                        )
                        .with_context(|| {
                            format!(
                                "Can not compute digest of absention condition \
                                 ({index_record_type:?}), {path:?}, {value:?}"
                            )
                        })?
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
                    )
                    .with_context(|| {
                        format!(
                            "Can not initiate search in index with presention conditions \
                             {presention_conditions:?} and absention conditions \
                             {absention_conditions:?}"
                        )
                    })?
                    .map(|dream_id| {
                        Ok(ObjectId {
                            value: dream_id.value,
                        })
                    }),
            ))
        }

        /// Gets the length of an array at a given path.
        ///
        /// Returns the number of elements in the array at the specified path.
        ///
        /// # Arguments
        ///
        /// * `object_id` - The ID of the object containing the array.
        /// * `array_path` - The path to the array.
        ///
        /// # Returns
        ///
        /// - `Ok(Some(len))` - The number of elements in the array.
        /// - `Ok(Some(0))` - If the array doesn't exist yet.
        /// - `Ok(None)` - If the path doesn't exist.
        pub fn len(&self, object_id: &ObjectId, array_path: &Path) -> Result<Option<u32>> {
            let iter_from = Bound::Included(&(
                object_id.clone(),
                array_path
                    .iter()
                    .cloned()
                    .chain(vec![PathSegment::JsonArrayIndex(std::u32::MAX)])
                    .collect::<Path>(),
            ));
            let mut found_object = false;
            Ok(self
                .index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .iter(iter_from, true)?
                .take_while(|((current_object_id, current_path), _)| {
                    Ok(current_object_id == object_id
                        && current_path.starts_with(&array_path)
                        && if let Some(PathSegment::JsonArrayIndex(_)) =
                            current_path.get(array_path.len())
                        {
                            true
                        } else {
                            false
                        })
                })
                .map(|((_, result_path), _)| {
                    found_object = true;
                    Ok(match result_path.get(array_path.len()).ok_or_else(|| {
                        anyhow!("Can not get last element of result path {result_path:?}")
                    })? {
                        PathSegment::JsonObjectKey(object_key) => {
                            return Err(anyhow!(
                                "Last element of result path appear to be JSON object string key \
                                 {object_key}, but expected JSON array index number"
                            ))
                        }
                        PathSegment::JsonArrayIndex(array_index) => *array_index,
                    } + 1)
                })
                .next()?
                .or_else(|| if found_object { None } else { Some(0) }))
        }

        /// Gets the last element of an array at a given path.
        ///
        /// Returns the last element of the array at the specified path.
        ///
        /// # Arguments
        ///
        /// * `object_id` - The ID of the object containing the array.
        /// * `array_path` - The path to the array.
        ///
        /// # Returns
        ///
        /// - `Ok(Some(value))` - The last element of the array.
        /// - `Ok(None)` - If the array is empty or doesn't exist.
        pub fn last(
            &self,
            object_id: &ObjectId,
            array_path: &Path,
        ) -> Result<Option<serde_json::Value>> {
            if let Some(array_len) = self.len(object_id, array_path)? {
                let result_path = array_path
                    .iter()
                    .cloned()
                    .chain(vec![PathSegment::JsonArrayIndex(array_len - 1)].into_iter())
                    .collect::<Vec<_>>();
                Ok(Some(self.get(object_id, &result_path)?.ok_or_else(
                    || anyhow!("Can not get last element of array at path {result_path:?}"),
                )?))
            } else {
                Ok(None)
            }
        }

        /// Checks if an object with the given ID exists.
        ///
        /// # Arguments
        ///
        /// * `object_id` - The ID to check for.
        ///
        /// # Returns
        ///
        /// `Ok(true)` if the object exists, `Ok(false)` otherwise.
        pub fn contains_object_with_id(&self, object_id: &ObjectId) -> Result<bool> {
            Ok(self
                .index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .iter(Bound::Included(&(object_id.clone(), vec![])), false)?
                .take_while(|((current_object_id, _), _)| Ok(current_object_id == object_id))
                .next()?
                .is_some())
        }

        /// Checks if any path starting with the given prefix exists.
        ///
        /// Returns `true` if there are any entries at or under the given path.
        ///
        /// # Arguments
        ///
        /// * `object_id` - The ID of the object to check.
        /// * `path` - The path prefix to check.
        pub fn contains_path(&self, object_id: &ObjectId, path: &Path) -> Result<bool> {
            Ok(self
                .index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .iter(Bound::Included(&(object_id.clone(), path.clone())), false)?
                .take_while(|((current_object_id, current_path), _)| {
                    Ok(current_object_id == object_id && current_path.starts_with(path))
                })
                .next()?
                .is_some())
        }

        /// Checks if an exact path exists.
        ///
        /// Returns `true` only if there's a value at the exact specified path.
        ///
        /// # Arguments
        ///
        /// * `object_id` - The ID of the object to check.
        /// * `path` - The exact path to check.
        pub fn contains_exact_path(&self, object_id: &ObjectId, path: &Path) -> Result<bool> {
            Ok(self
                .index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .iter(Bound::Included(&(object_id.clone(), path.clone())), false)?
                .take_while(|((current_object_id, current_path), _)| {
                    Ok(current_object_id == object_id && current_path == path)
                })
                .next()?
                .is_some())
        }

        /// Checks if an array contains a specific element.
        ///
        /// Returns `true` if the value exists in the array at the given path.
        ///
        /// # Arguments
        ///
        /// * `object_id` - The ID of the object containing the array.
        /// * `array_path` - The path to the array.
        /// * `element` - The element value to search for.
        pub fn contains_element(
            &self,
            object_id: &ObjectId,
            array_path: &Path,
            element: &Value,
        ) -> Result<bool> {
            self.index_transaction
                .has_object_with_tag(&dream::Object::Identified(dream::Id {
                    value: Digest::of_path_object_id_and_value(array_path, object_id, element)
                        .with_context(|| {
                            format!(
                                "Can not compute array type digest for path {array_path:?}, \
                                 object id {:?} and value {element:?}",
                                object_id
                            )
                        })?
                        .value,
                }))
        }

        pub fn get_element_index(
            &self,
            object_id: &ObjectId,
            array_path: &Path,
            element: &Value,
        ) -> Result<Option<u32>> {
            Ok(self
                .index_transaction
                .search(
                    &vec![dream::Object::Identified(dream::Id {
                        value: Digest::of_path_object_id_and_value(array_path, object_id, element)
                            .with_context(|| {
                                format!(
                                    "Can not compute array type digest for path {array_path:?}, \
                                     object id {:?} and value {element:?}",
                                    object_id
                                )
                            })?
                            .value,
                    })],
                    &vec![],
                    None,
                )?
                .next()?
                .map(|dream_id| IndexBatch::dream_id_to_u32(dream_id)))
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
            let object_id = first_object_entry.0 .0;
            let mut flat_object: FlatObject = Vec::new();
            flat_object.push((first_object_entry.0 .1, first_object_entry.1));
            loop {
                self.last_entry = self.data_table_iterator.next().with_context(|| {
                    format!(
                        "Can not get next data table entry after {:?}",
                        self.last_entry
                    )
                })?;
                if let Some(current_entry) = &self.last_entry {
                    if current_entry.0 .0 != object_id {
                        break;
                    }
                    flat_object.push((current_entry.0 .1.clone(), current_entry.1.clone()));
                } else {
                    break;
                }
            }
            Ok(nest(&flat_object)?.and_then(|value| {
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

    fn push(&mut self, path: Path, value: Value) -> Result<&Self> {
        let path_index_option = path.last().and_then(|last_segment| {
            if let PathSegment::JsonArrayIndex(path_index) = last_segment {
                Some(path_index)
            } else {
                None
            }
        });
        if let Some(path_index) = path_index_option {
            let path_base = path[..path.len() - 1].to_vec();
            self.digests.push(dream::Object::Identified(dream::Id {
                value: Digest::of_pathvalue(IndexRecordType::Array, &path_base, &value.clone())
                    .with_context(|| {
                        format!(
                            "Can not compute array type digest for path {path_base:?} and value \
                             {value:?}",
                        )
                    })?
                    .value,
            }));
            self.array_digests
                .entry(*path_index)
                .or_insert(Vec::new())
                .push(dream::Object::Identified(dream::Id {
                    value: Digest::of_path_object_id_and_value(&path_base, &self.object_id, &value)
                        .with_context(|| {
                            format!(
                                "Can not compute array type digest for path {path_base:?}, object \
                                 id {:?} and value {value:?}",
                                self.object_id
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

    fn dream_id_to_u32(id: dream::Id) -> u32 {
        u32::from_be_bytes(id.value[12..16].try_into().unwrap())
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
                .with_context(|| {
                    format!(
                        "Can not remove tags {tags:?} from object with id {dream_id:?} in dream \
                         index"
                    )
                })?;
        }
        Ok(self)
    }
}

fn flatten_to(
    path: Path,
    value: &serde_json::Value,
    result: &mut Vec<(Path, Value)>,
) -> Result<()> {
    match value {
        serde_json::Value::Object(map) => {
            for (key, internal_value) in map {
                let internal_path = path
                    .iter()
                    .cloned()
                    .chain(vec![PathSegment::JsonObjectKey(key.clone())].into_iter())
                    .collect::<Path>();
                flatten_to(internal_path, &internal_value, result).with_context(|| {
                    format!("Can not get flat representation of value {internal_value:?} part")
                })?;
            }
        }
        serde_json::Value::Array(array) => {
            // let mut unique_internal_values: HashSet<serde_json::Value> = HashSet::new();
            for (internal_value_index, internal_value) in array.iter().enumerate() {
                // match internal_value {
                //     serde_json::Value::Array(_) => {}
                //     serde_json::Value::Object(_) => {}
                //     _ => {
                //         if unique_internal_values.contains(&internal_value) {
                //             continue;
                //         }
                //         unique_internal_values.insert(internal_value.clone());
                //     }
                // }
                let internal_path = path
                    .iter()
                    .cloned()
                    .chain(
                        vec![PathSegment::JsonArrayIndex(internal_value_index as u32)].into_iter(),
                    )
                    .collect::<Path>();
                flatten_to(internal_path, &internal_value, result).with_context(|| {
                    format!(
                        "Can not merge flat representation of value {internal_value:?} part into \
                         {result:?}"
                    )
                })?;
            }
        }
        _ => {
            result.push((
                path,
                (*value).clone().try_into().with_context(|| {
                    format!("Can not convert json value {value:?} into database storable value")
                })?,
            ));
        }
    }
    Ok(())
}

/// Flattens a JSON value into path-value pairs.
///
/// Recursively traverses the JSON structure and produces a list of
/// (path, value) pairs representing all leaf values.
///
/// - Object properties become path segments with their key names
/// - Array elements become path segments with their indices
/// - Primitive values (null, bool, number, string) become the final values
///
/// # Arguments
///
/// * `path` - The current path prefix
/// * `value` - The JSON value to flatten
///
/// # Returns
///
/// A vector of (path, value) pairs representing all leaf values.
fn flatten(path: &Path, value: &serde_json::Value) -> Result<Vec<(Path, Value)>> {
    let mut result: Vec<(Path, Value)> = Vec::new();
    flatten_to(path.clone(), value, &mut result).with_context(|| {
        format!(
            "Can not merge flat representation of value {value:?} part at path {path:?} into \
             {result:?}"
        )
    })?;
    Ok(result)
}

impl<'a, 'b, 'c> WriteTransaction<'a, 'b, 'c> {
    define_read_methods!();

    fn update_with_index(
        &mut self,
        object_id: ObjectId,
        path: Path,
        value: serde_json::Value,
        index_batch: &mut IndexBatch,
    ) -> Result<ObjectId> {
        for (internal_path, internal_value) in flatten(&path, &value)
            .with_context(|| format!("Can not flatten value {value:?} part at path {path:?}"))?
        {
            index_batch
                .push(internal_path.clone(), internal_value.clone())
                .with_context(|| {
                    format!(
                        "Can not push path-value pair ({internal_path:?}, {internal_value:?}) \
                         into index batch"
                    )
                })?;
            self.index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .insert((object_id.clone(), internal_path), internal_value);
        }
        Ok(object_id)
    }

    /// Updates or inserts a value at a specific path within an object.
    ///
    /// If the object doesn't exist, it will be created. If the path doesn't exist,
    /// intermediate structures will be created.
    ///
    /// # Arguments
    ///
    /// * `object_id` - The ID of the object to update (or create).
    /// * `path` - The path where the value should be stored (empty vec for root).
    /// * `value` - The JSON value to store.
    ///
    /// # Returns
    ///
    /// The object ID.
    pub fn update(
        &mut self,
        object_id: ObjectId,
        path: Path,
        value: serde_json::Value,
    ) -> Result<ObjectId> {
        let mut index_batch = IndexBatch::new(object_id.clone());
        self.update_with_index(
            object_id.clone(),
            path.clone(),
            value.clone(),
            &mut index_batch,
        )
        .with_context(|| {
            format!(
                "Can not update object with id {object_id:?} with path-value pair ({path:?}, \
                 {value:?}) also updating index batch {index_batch:?}"
            )
        })?;
        index_batch
            .flush_insert(self.index_transaction)
            .with_context(|| format!("Can not flush-insert index batch {index_batch:?}"))?;
        Ok(object_id)
    }

    /// Inserts a new object with an auto-generated ID.
    ///
    /// A new [`ObjectId`] is generated and the value is stored at the root path.
    ///
    /// # Arguments
    ///
    /// * `value` - The JSON value to store as a new object.
    ///
    /// # Returns
    ///
    /// The newly generated `ObjectId`.
    pub fn insert(&mut self, value: serde_json::Value) -> Result<ObjectId> {
        let id = ObjectId::new();
        self.update(id.clone(), vec![], value)
    }

    /// Inserts an object with a specific ID.
    ///
    /// The provided object's ID is used instead of generating a new one.
    ///
    /// # Arguments
    ///
    /// * `object` - The object to insert.
    ///
    /// # Returns
    ///
    /// The object's ID.
    pub fn insert_with_id(&mut self, object: Object) -> Result<ObjectId> {
        self.update(object.id, vec![], object.value)
    }

    /// Removes a value at a given path prefix.
    ///
    /// Removes all entries at or under the specified path from the object.
    ///
    /// # Arguments
    ///
    /// * `object_id` - The ID of the object to modify.
    /// * `path_prefix` - The path prefix to remove (empty vec removes entire object).
    pub fn remove(&mut self, object_id: &ObjectId, path_prefix: &Path) -> Result<()> {
        let from_object_id_and_path = &(object_id.clone(), path_prefix.clone());
        let paths_to_remove = self
            .index_transaction
            .database_transaction
            .object_id_and_path_to_value
            .iter(Bound::Included(from_object_id_and_path), false)
            .with_context(|| {
                format!(
                    "Can not initiate iteration over object_id_and_path_to_value table starting \
                     from key {from_object_id_and_path:?}"
                )
            })?
            .take_while(|((current_object_id, current_path), _)| {
                Ok(*current_object_id == *object_id && current_path.starts_with(&path_prefix))
            })
            .collect::<Vec<_>>()
            .with_context(|| {
                format!(
                    "Can not collect from iteration over object_id_and_path_to_value table \
                     starting from key {from_object_id_and_path:?} taking while object id is \
                     {object_id:?} and path starts with {path_prefix:?}"
                )
            })?;
        let mut index_batch = IndexBatch::new(object_id.clone());
        for ((_, current_path), current_value) in paths_to_remove.into_iter() {
            self.index_transaction
                .database_transaction
                .object_id_and_path_to_value
                .remove(&(object_id.clone(), current_path.clone()));
            index_batch
                .push(current_path.clone(), current_value.clone())
                .with_context(|| {
                    format!(
                        "Can not push path-value pair ({current_path:?}, {current_value:?}) into \
                         index batch"
                    )
                })?;
        }
        index_batch
            .flush_remove(self.index_transaction)
            .with_context(|| format!("Can not flush-remove index batch {index_batch:?}"))?;
        Ok(())
    }

    /// Pushes a value onto the end of an array.
    ///
    /// Appends a new element to the array at the specified path.
    ///
    /// # Arguments
    ///
    /// * `object_id` - The ID of the object containing the array.
    /// * `array_path` - The path to the array.
    /// * `value` - The value to append.
    ///
    /// # Returns
    ///
    /// The index at which the value was inserted.
    pub fn push(
        &mut self,
        object_id: &ObjectId,
        array_path: &Path,
        value: serde_json::Value,
    ) -> Result<u32> {
        let array_len = self
            .len(object_id, array_path)?
            .ok_or_else(|| anyhow!("Can not get length of array at path {array_path:?}"))?;
        let push_path = array_path
            .iter()
            .cloned()
            .chain(vec![PathSegment::JsonArrayIndex(array_len)].into_iter())
            .collect::<Vec<_>>();
        self.update(object_id.clone(), push_path, value)?;
        Ok(array_len)
    }
}

impl From<&str> for PathSegment {
    fn from(s: &str) -> Self {
        PathSegment::JsonObjectKey(s.to_string())
    }
}

impl From<String> for PathSegment {
    fn from(s: String) -> Self {
        PathSegment::JsonObjectKey(s)
    }
}

impl From<i32> for PathSegment {
    fn from(i: i32) -> Self {
        PathSegment::JsonArrayIndex(i as u32)
    }
}

impl From<u32> for PathSegment {
    fn from(i: u32) -> Self {
        PathSegment::JsonArrayIndex(i)
    }
}

impl From<usize> for PathSegment {
    fn from(i: usize) -> Self {
        PathSegment::JsonArrayIndex(i as u32)
    }
}

/// Constructs a [`Path`] from a list of segments.
///
/// This macro provides a convenient way to create paths using a
/// comma-separated list of segment values.
///
/// # Arguments
///
/// A list of segments that can be:
/// - `&str` or `String` - Creates a [`PathSegment::JsonObjectKey`]
/// - Integer types (`i32`, `u32`, `usize`) - Creates a [`PathSegment::JsonArrayIndex`]
#[macro_export]
macro_rules! path_segments {
    ( $( $seg:expr ),+ $(,)? ) => {
        vec![
            $(
                crate::PathSegment::from($seg)
            ),+
        ]
    };
}

/// Tests for the trove database.
///
/// These tests verify the core functionality including:
/// - Basic CRUD operations
/// - Path-based access to nested structures
/// - Array operations (push, len, last)
/// - Select queries with presence/absence conditions
/// - Edge cases like dots in object keys
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
                                assert_eq!(transaction.contains_object_with_id(object_id)?, true);
                                let result = transaction.get(&object_id, &vec![])?.unwrap();
                                assert_eq!(result, *object_value);
                                let flatten_object = flatten(&vec![], &object_value)?;
                                for (pathvalue_index, (path, value)) in
                                    flatten_object.iter().enumerate()
                                {
                                    transaction.contains_exact_path(object_id, path)?;
                                    for segments_count in 0..path.len() {
                                        assert_eq!(
                                            transaction.contains_path(
                                                object_id,
                                                &path[..=segments_count].to_vec()
                                            )?,
                                            true
                                        );
                                    }
                                    let value_as_json: serde_json::Value = value.clone().into();
                                    if let Some(last_path_segment) = path.last() {
                                        let base_path = path[..path.len() - 1].to_vec();
                                        match last_path_segment {
                                            PathSegment::JsonObjectKey(_) => {
                                                assert_eq!(
                                                    transaction.contains_element(
                                                        object_id, &base_path, value
                                                    )?,
                                                    false
                                                );
                                                assert_eq!(
                                                    transaction.len(object_id, &base_path).unwrap(),
                                                    Some(0)
                                                );
                                                let selected = transaction
                                                    .select(
                                                        &vec![(
                                                            IndexRecordType::Direct,
                                                            path.clone(),
                                                            value_as_json.clone(),
                                                        )],
                                                        &vec![],
                                                        None,
                                                    )?
                                                    .collect::<Vec<ObjectId>>()?;
                                                for selected_object_id in selected.iter() {
                                                    assert_eq!(
                                                        transaction
                                                            .get(&selected_object_id, &path)?
                                                            .unwrap(),
                                                        value_as_json
                                                    );
                                                }
                                                assert!(selected.iter().any(
                                                    |selected_object_id| {
                                                        selected_object_id == object_id
                                                    }
                                                ));
                                            }
                                            PathSegment::JsonArrayIndex(current_array_index) => {
                                                assert_eq!(
                                                    transaction.contains_element(
                                                        object_id, &base_path, value
                                                    )?,
                                                    true
                                                );
                                                assert_eq!(
                                                    transaction
                                                        .get_element_index(
                                                            object_id, &base_path, value
                                                        )
                                                        .unwrap(),
                                                    Some(*current_array_index)
                                                );
                                                let selected = transaction
                                                    .select(
                                                        &vec![(
                                                            IndexRecordType::Array,
                                                            base_path.clone(),
                                                            value_as_json.clone(),
                                                        )],
                                                        &vec![],
                                                        None,
                                                    )?
                                                    .collect::<Vec<ObjectId>>()?;
                                                for selected_object_id in selected.iter() {
                                                    assert!(transaction
                                                        .get(&selected_object_id, &base_path)?
                                                        .unwrap()
                                                        .as_array()
                                                        .unwrap()
                                                        .iter()
                                                        .any(|got_array_element| {
                                                            got_array_element == &value_as_json
                                                        }));
                                                }
                                                assert!(selected.iter().any(
                                                    |selected_object_id| {
                                                        selected_object_id == object_id
                                                    }
                                                ));
                                                if flatten_object
                                                    .get(pathvalue_index + 1)
                                                    .is_none_or(|(next_path, _)| {
                                                        next_path.starts_with(&base_path)
                                                            && next_path.get(base_path.len())
                                                                != Some(
                                                                    &PathSegment::JsonArrayIndex(
                                                                        current_array_index + 1,
                                                                    ),
                                                                )
                                                    })
                                                {
                                                    transaction.remove(object_id, path).unwrap();
                                                    transaction
                                                        .push(
                                                            object_id,
                                                            &base_path,
                                                            value_as_json.clone(),
                                                        )
                                                        .unwrap();
                                                    assert_eq!(
                                                        transaction
                                                            .len(object_id, &base_path)
                                                            .unwrap(),
                                                        Some(*current_array_index + 1),
                                                    );
                                                    assert_eq!(
                                                        transaction
                                                            .last(object_id, &base_path)
                                                            .unwrap(),
                                                        Some(value_as_json.clone())
                                                    );
                                                }
                                            }
                                        }
                                    } else {
                                        let selected = transaction
                                            .select(
                                                &vec![(
                                                    IndexRecordType::Direct,
                                                    vec![],
                                                    value_as_json.clone(),
                                                )],
                                                &vec![],
                                                None,
                                            )?
                                            .collect::<Vec<ObjectId>>()?;
                                        for selected_object_id in selected.iter() {
                                            assert_eq!(
                                                transaction
                                                    .get(&selected_object_id, &vec![])?
                                                    .unwrap(),
                                                value_as_json
                                            );
                                        }
                                        assert!(selected.iter().any(|selected_object_id| {
                                            selected_object_id == object_id
                                        }));
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
                            transaction.remove(&object_to_remove_id, &vec![])?;
                            previously_added_objects.remove(&object_to_remove_id);
                            assert_eq!(transaction.get(&object_to_remove_id, &vec![])?, None);
                            assert_eq!(transaction.get(&object_to_remove_id, &vec![])?, None);
                        }
                        3 => {
                            let object_to_remove_from_id = previously_added_objects
                                .keys()
                                .nth(rng.generate_range(0..previously_added_objects.len()))
                                .unwrap()
                                .clone();
                            let flattened_object_to_remove_from =
                                transaction.get_flattened(&object_to_remove_from_id, &vec![])?;
                            let path_to_remove = flattened_object_to_remove_from
                                [rng.generate_range(0..flattened_object_to_remove_from.len())]
                            .0
                            .clone();
                            let correct_result_option = nest(
                                &flattened_object_to_remove_from
                                    .into_iter()
                                    .filter(|(path, _)| !path.starts_with(&path_to_remove))
                                    .map(|(path, value)| (path, value.into()))
                                    .collect::<Vec<_>>(),
                            )?;
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
                                transaction.get(&object_to_remove_from_id, &vec![])?,
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
        let mut chest = new_default_chest("test_dots_in_keys");
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
                    &transaction
                        .get(&object.id, &path_segments!("dict"))?
                        .unwrap(),
                    object.value.as_object().unwrap().get("dict").unwrap()
                );
                assert_eq!(
                    &transaction
                        .get(&object.id, &path_segments!("dict", "hello"))?
                        .unwrap(),
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
                    &transaction
                        .get(&object.id, &path_segments!("dict", "boolean"))?
                        .unwrap(),
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
                    transaction
                        .get(&object.id, &path_segments!("dict", "hello", 0))?
                        .unwrap(),
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
                    transaction
                        .get(&object.id, &path_segments!("dict", "hello", 1))?
                        .unwrap(),
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
                    transaction
                        .get(&object.id, &path_segments!("dict", "hello", 2))?
                        .unwrap(),
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
                    transaction
                        .get(&object.id, &path_segments!("dict", "hello", 3))?
                        .unwrap(),
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
