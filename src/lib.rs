use anyhow::{anyhow, Context, Error, Result};
use fallible_iterator::FallibleIterator;

pub extern crate anyhow;
pub extern crate dream;
pub extern crate fallible_iterator;
pub extern crate paste;
pub extern crate serde;

pub use bincode;
pub use dream::xxhash_rust;

#[derive(
    Clone, Default, PartialEq, PartialOrd, Debug, bincode::Encode, bincode::Decode, Eq, Ord, Hash,
)]
#[bincode(crate = "bincode")]
pub struct DocumentId {
    pub value: [u8; 16],
}

impl DocumentId {
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

impl From<i64> for DocumentId {
    fn from(i: i64) -> Self {
        let mut result_raw = [0u8; 16];
        result_raw[..8].copy_from_slice(&i.to_le_bytes());
        Self { value: result_raw }
    }
}

impl From<DocumentId> for i64 {
    fn from(document_id: DocumentId) -> Self {
        let mut document_id_bytes = [0u8; 8];
        document_id_bytes.copy_from_slice(&document_id.value[..8]);
        i64::from_le_bytes(document_id_bytes)
    }
}

impl serde::Serialize for DocumentId {
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

impl<'de> serde::Deserialize<'de> for DocumentId {
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
        Ok(DocumentId { value })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Document {
    pub id: DocumentId,
    pub value: serde_json::Value,
}

#[derive(
    Debug, Clone, bincode::Encode, bincode::Decode, PartialOrd, Ord, PartialEq, Eq, serde::Serialize,
)]
#[bincode(crate = "bincode")]
pub enum PathSegment {
    JsonObjectKey(String),
    JsonArrayIndex(u32),
}

pub type Path = Vec<PathSegment>;

pub type FlatDocument = Vec<(Path, Value)>;

pub fn nest(flat_document: &FlatDocument) -> Result<Option<serde_json::Value>> {
    if flat_document.is_empty() {
        Ok(None)
    } else if flat_document[0].0.is_empty() {
        Ok(Some(flat_document[0].1.clone().into()))
    } else {
        let mut result = serde_json::Value::Null;
        for (path, value) in flat_document {
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

#[derive(bincode::Encode, bincode::Decode, Clone, Debug, serde::Serialize)]
#[bincode(crate = "bincode")]
pub enum Value {
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

pub struct Digest {
    pub value: [u8; 16],
}

#[repr(u8)]
#[derive(Clone, Debug, serde::Serialize)]
pub enum IndexRecordType {
    Direct = 0,
    Array = 1,
}

impl Digest {
    pub fn of_data(data: &Vec<u8>) -> Self {
        Self {
            value: xxhash_rust::xxh3::xxh3_128(data).to_le_bytes(),
        }
    }

    pub fn of_serializable<S>(serializable: &S) -> Self
    where
        S: serde::Serialize + std::fmt::Debug,
    {
        Self::of_data(&serde_json::to_vec(&serializable).unwrap())
    }
}

pub struct DocumentsIterator<'a> {
    pub data_table_iterator:
        Box<dyn FallibleIterator<Item = ((DocumentId, Path), Value), Error = Error> + 'a>,
    pub last_entry: Option<((DocumentId, Path), Value)>,
}

#[macro_export]
macro_rules! define_chest {
    ($chest_name:ident(
        $(
            $bucket_name:ident
        )*
    ) {
        $(
            $additional_schema_name:ident {
                $(
                    $table_name:ident<$key_type:ty, $value_type:ty>
                )*
            }
        )*
    } use {
        $($use_item:tt)*
    }) => {
        #[allow(dead_code)]
        pub mod $chest_name {
            use std::{ops::Bound, collections::HashMap};

            use $crate::{
                serde::{Serialize, Deserialize},
                anyhow::{Result, Error, Context, anyhow},
                dream, paste::paste, fallible_iterator::FallibleIterator,
                DocumentId, Document, Path, DocumentsIterator, FlatDocument, Digest, PathSegment, IndexRecordType, nest, flatten
            };

            paste! {
                dream::define_index!(trove_database(
                    $( $bucket_name ),*
                ) {
                    data {
                        $(
                            [<$bucket_name _document_id_and_path_to_value>]<(DocumentId, Path), JsonValue>
                        )+
                    }
                    $(
                        $additional_schema_name {
                            $( $table_name<$key_type, $value_type> )*
                        }
                    )*
                } use {
                    use $crate::DocumentId;
                    use $crate::Path;
                    use $crate::Value as JsonValue;
                    $($use_item)*
                });
            }

            pub struct Chest {
                pub index: trove_database::Index,
            }

            #[derive(Debug, Clone, Serialize, Deserialize)]
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

            pub struct ReadTransaction<'a> {
                pub index_transaction: trove_database::ReadTransaction<'a>,
            }

            pub struct WriteTransaction<'a, 'b, 'c> {
                pub index_transaction: &'a mut trove_database::WriteTransaction<'b, 'c>,
            }

            $(
                paste! {
                    #[derive(Debug, Clone)]
                    struct [<$bucket_name:camel IndexBatch>] {
                        document_id: DocumentId,
                        digests: Vec<dream::Object>,
                        array_digests: HashMap<u32, Vec<dream::Object>>,
                    }

                    impl [<$bucket_name:camel IndexBatch>] {
                        fn new(document_id: DocumentId) -> Self {
                            Self {
                                document_id,
                                digests: Vec::new(),
                                array_digests: HashMap::new(),
                            }
                        }

                        fn push(&mut self, path: Path, json_value: &serde_json::Value) -> Result<&Self> {
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
                                    value: Digest::of_serializable(&(IndexRecordType::Array, &path_base, &json_value)).value,
                                }));
                                self.array_digests
                                    .entry(*path_index)
                                    .or_insert(Vec::new())
                                    .push(dream::Object::Identified(dream::Id {
                                        value: Digest::of_serializable(&(&path_base, &self.document_id, &json_value)).value,
                                    }));
                            } else {
                                self.digests.push(dream::Object::Identified(dream::Id {
                                    value: Digest::of_serializable(&(IndexRecordType::Direct, path, &json_value)).value,
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
                                        value: self.document_id.value,
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
                                    .[<$bucket_name _insert>](&dream::Object::Identified(dream_id.clone()), tags)
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
                                    .[<$bucket_name _remove_tags_from_object>](&dream::Object::Identified(dream_id.clone()), tags)
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
                }
            )*

            macro_rules! define_read_methods {
                () => {
                    $(
                        paste! {
                            pub fn [<$bucket_name _documents>](&'a self) -> Result<DocumentsIterator<'a>> {
                                Ok(DocumentsIterator {
                                    data_table_iterator: self
                                        .index_transaction
                                        .database_transaction
                                        .data
                                        .[<$bucket_name _document_id_and_path_to_value>]
                                        .iter(Bound::Unbounded, false)
                                        .with_context(|| {
                                            "Can not initiate iteration over document_id_and_path_to_value table"
                                        })?,
                                    last_entry: None,
                                })
                            }

                            pub fn [<$bucket_name _get_flattened>](
                                &'a self,
                                document_id: &DocumentId,
                                path_prefix: &Path,
                            ) -> Result<FlatDocument> {
                                let mut flat_document: FlatDocument = Vec::new();
                                let from_document_id_and_path = &(document_id.clone(), path_prefix.clone());
                                let mut iterator = self
                                    .index_transaction
                                    .database_transaction
                                    .data
                                    .[<$bucket_name _document_id_and_path_to_value>]
                                    .iter(Bound::Included(from_document_id_and_path), false)
                                    .with_context(|| {
                                        format!(
                                            "Can not initiate iteration over document_id_and_path_to_value table from \
                                             key {from_document_id_and_path:?}"
                                        )
                                    })?
                                    .take_while(|entry| {
                                        Ok(entry.0 .0 == *document_id
                                            && (path_prefix.is_empty()
                                                || entry.0 .1 == *path_prefix
                                                || entry.0 .1.starts_with(&path_prefix)))
                                    });
                                loop {
                                    if let Some(entry) = iterator.next()? {
                                        flat_document.push((entry.0 .1.clone()[path_prefix.len()..].to_vec(), entry.1));
                                    } else {
                                        break;
                                    }
                                }
                                Ok(flat_document)
                            }

                            pub fn [<$bucket_name _get>](
                                &'a self,
                                document_id: &DocumentId,
                                path_prefix: &Path,
                            ) -> Result<Option<serde_json::Value>> {
                                nest(
                                    &self
                                        .[<$bucket_name _get_flattened>](document_id, path_prefix)
                                        .with_context(|| {
                                            format!(
                                                "Can not get part at path prefix {path_prefix:?} of flattened document \
                                                 with id {document_id:?}"
                                            )
                                        })?,
                                )
                            }

                            pub fn [<$bucket_name _select>](
                                &'a self,
                                presention_conditions: &Vec<(IndexRecordType, Path, serde_json::Value)>,
                                absention_conditions: &Vec<(IndexRecordType, Path, serde_json::Value)>,
                                start_after_document: Option<DocumentId>,
                            ) -> Result<Box<dyn FallibleIterator<Item = DocumentId, Error = Error> + '_>> {
                                let present_ids = {
                                    let mut result = Vec::new();
                                    for (index_record_type, path, value) in presention_conditions {
                                        result.push(dream::Object::Identified(dream::Id {
                                            value: Digest::of_serializable(&(index_record_type, path, &value)).value,
                                        }));
                                    }
                                    result
                                };
                                let absent_ids = {
                                    let mut result = Vec::new();
                                    for (index_record_type, path, value) in absention_conditions {
                                        result.push(dream::Object::Identified(dream::Id {
                                            value: Digest::of_serializable(&(index_record_type, path, &value)).value,
                                        }));
                                    }
                                    result
                                };
                                Ok(Box::new(
                                    self.index_transaction
                                        .[<$bucket_name _search>](
                                            &present_ids,
                                            &absent_ids,
                                            start_after_document.and_then(|start_after_document| {
                                                Some(dream::Id {
                                                    value: start_after_document.value,
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
                                            Ok(DocumentId {
                                                value: dream_id.value,
                                            })
                                        }),
                                ))
                            }

                            pub fn [<$bucket_name _last_element_index>](&self, document_id: &DocumentId, array_path: &Path) -> Result<Option<u32>> {
                                let iter_from = Bound::Included(&(
                                    document_id.clone(),
                                    array_path
                                        .iter()
                                        .cloned()
                                        .chain(vec![PathSegment::JsonArrayIndex(std::u32::MAX)])
                                        .collect::<Path>(),
                                ));
                                let mut found_document = false;
                                Ok(self
                                    .index_transaction
                                    .database_transaction
                                    .data
                                    .[<$bucket_name _document_id_and_path_to_value>]
                                    .iter(iter_from, true)?
                                    .take_while(|((current_document_id, current_path), _)| {
                                        Ok(current_document_id == document_id
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
                                        found_document = true;
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
                                        })
                                    })
                                    .next()?
                                    .or_else(|| if found_document { None } else { Some(0) }))
                            }

                            pub fn [<$bucket_name _last>](
                                &self,
                                document_id: &DocumentId,
                                array_path: &Path,
                            ) -> Result<Option<serde_json::Value>> {
                                if let Some(last_element_index) = self.[<$bucket_name _last_element_index>](document_id, array_path)? {
                                    let result_path = array_path
                                        .iter()
                                        .cloned()
                                        .chain(vec![PathSegment::JsonArrayIndex(last_element_index)].into_iter())
                                        .collect::<Vec<_>>();
                                    Ok(Some(self.[<$bucket_name _get>](document_id, &result_path)?.ok_or_else(
                                        || anyhow!("Can not get last element of array at path {result_path:?}"),
                                    )?))
                                } else {
                                    Ok(None)
                                }
                            }

                            pub fn [<$bucket_name _contains_document_with_id>](&self, document_id: &DocumentId) -> Result<bool> {
                                Ok(self
                                    .index_transaction
                                    .database_transaction
                                    .data
                                    .[<$bucket_name _document_id_and_path_to_value>]
                                    .iter(Bound::Included(&(document_id.clone(), vec![])), false)?
                                    .take_while(|((current_document_id, _), _)| Ok(current_document_id == document_id))
                                    .next()?
                                    .is_some())
                            }

                            pub fn [<$bucket_name _contains_path>](&self, document_id: &DocumentId, path: &Path) -> Result<bool> {
                                Ok(self
                                    .index_transaction
                                    .database_transaction
                                    .data
                                    .[<$bucket_name _document_id_and_path_to_value>]
                                    .iter(Bound::Included(&(document_id.clone(), path.clone())), false)?
                                    .take_while(|((current_document_id, current_path), _)| {
                                        Ok(current_document_id == document_id && current_path.starts_with(path))
                                    })
                                    .next()?
                                    .is_some())
                            }

                            pub fn [<$bucket_name _contains_exact_path>](&self, document_id: &DocumentId, path: &Path) -> Result<bool> {
                                Ok(self
                                    .index_transaction
                                    .database_transaction
                                    .data
                                    .[<$bucket_name _document_id_and_path_to_value>]
                                    .iter(Bound::Included(&(document_id.clone(), path.clone())), false)?
                                    .take_while(|((current_document_id, current_path), _)| {
                                        Ok(current_document_id == document_id && current_path == path)
                                    })
                                    .next()?
                                    .is_some())
                            }

                            pub fn [<$bucket_name _contains_element>](
                                &self,
                                document_id: &DocumentId,
                                array_path: &Path,
                                element: &serde_json::Value,
                            ) -> Result<bool> {
                                self.index_transaction
                                    .[<$bucket_name _has_object_with_tag>](&dream::Object::Identified(dream::Id {
                                        value: Digest::of_serializable(&(array_path, document_id, element)).value,
                                    }))
                            }

                            pub fn [<$bucket_name _get_element_index>](
                                &self,
                                document_id: &DocumentId,
                                array_path: &Path,
                                element: &serde_json::Value,
                            ) -> Result<Option<u32>> {
                                Ok(self
                                    .index_transaction
                                    .[<$bucket_name _search>](
                                        &vec![dream::Object::Identified(dream::Id {
                                            value: Digest::of_serializable(&(array_path, document_id, element)).value,
                                        })],
                                        &vec![],
                                        None,
                                    )?
                                    .next()?
                                    .map(|dream_id| [<$bucket_name:camel IndexBatch>]::dream_id_to_u32(dream_id)))
                            }
                        }
                    )*
                };
            }

            impl<'a> ReadTransaction<'a> {
                define_read_methods!();
            }

            impl<'a, 'b, 'c> WriteTransaction<'a, 'b, 'c> {
                define_read_methods!();
                $(
                    paste! {
                        fn [<$bucket_name _update_with_index>](
                            &mut self,
                            document_id: DocumentId,
                            path: Path,
                            value: serde_json::Value,
                            index_batch: &mut [<$bucket_name:camel IndexBatch>],
                        ) -> Result<DocumentId> {
                            for (internal_path, internal_value) in flatten(&path, &value, true)
                                .with_context(|| format!("Can not flatten value {value:?} part at path {path:?}"))?
                            {
                                index_batch
                                    .push(internal_path.clone(), &internal_value.clone().into())
                                    .with_context(|| {
                                        format!(
                                            "Can not push path-value pair ({internal_path:?}, {internal_value:?}) \
                                             into index batch"
                                        )
                                    })?;
                                let simple_internal_value_result: Result<crate::Value> = internal_value.try_into();
                                if let Ok(simple_internal_value) = simple_internal_value_result {
                                    self.index_transaction
                                        .database_transaction
                                        .data
                                        .[<$bucket_name _document_id_and_path_to_value>]
                                        .insert((document_id.clone(), internal_path), simple_internal_value);
                                }
                            }
                            Ok(document_id)
                        }

                        pub fn [<$bucket_name _update>](
                            &mut self,
                            document_id: DocumentId,
                            path: Path,
                            value: serde_json::Value,
                        ) -> Result<DocumentId> {
                            let mut index_batch = [<$bucket_name:camel IndexBatch>]::new(document_id.clone());
                            self.[<$bucket_name _update_with_index>](
                                document_id.clone(),
                                path.clone(),
                                value.clone(),
                                &mut index_batch,
                            )
                            .with_context(|| {
                                format!(
                                    "Can not update document with id {document_id:?} with path-value pair ({path:?}, \
                                     {value:?}) also updating index batch {index_batch:?}"
                                )
                            })?;
                            index_batch
                                .flush_insert(self.index_transaction)
                                .with_context(|| format!("Can not flush-insert index batch {index_batch:?}"))?;
                            Ok(document_id)
                        }

                        pub fn [<$bucket_name _insert>](&mut self, value: serde_json::Value) -> Result<DocumentId> {
                            let id = DocumentId::new();
                            self.[<$bucket_name _update>](id.clone(), vec![], value)
                        }

                        pub fn [<$bucket_name _insert_with_id>](&mut self, document: Document) -> Result<DocumentId> {
                            self.[<$bucket_name _update>](document.id, vec![], document.value)
                        }

                        pub fn [<$bucket_name _remove>](&mut self, document_id: &DocumentId, path_prefix: &Path) -> Result<()> {
                            let from_document_id_and_path = &(document_id.clone(), path_prefix.clone());
                            let paths_to_remove = self
                                .index_transaction
                                .database_transaction
                                .data
                                .[<$bucket_name _document_id_and_path_to_value>]
                                .iter(Bound::Included(from_document_id_and_path), false)
                                .with_context(|| {
                                    format!(
                                        "Can not initiate iteration over document_id_and_path_to_value table starting \
                                         from key {from_document_id_and_path:?}"
                                    )
                                })?
                                .take_while(|((current_document_id, current_path), _)| {
                                    Ok(*current_document_id == *document_id && current_path.starts_with(&path_prefix))
                                })
                                .collect::<Vec<_>>()
                                .with_context(|| {
                                    format!(
                                        "Can not collect from iteration over document_id_and_path_to_value table \
                                         starting from key {from_document_id_and_path:?} taking while document id is \
                                         {document_id:?} and path starts with {path_prefix:?}"
                                    )
                                })?;
                            let mut index_batch = [<$bucket_name:camel IndexBatch>]::new(document_id.clone());
                            for ((_, current_path), current_value) in paths_to_remove.into_iter() {
                                self.index_transaction
                                    .database_transaction
                                    .data
                                    .[<$bucket_name _document_id_and_path_to_value>]
                                    .remove(&(document_id.clone(), current_path.clone()));
                                index_batch
                                    .push(current_path.clone(), &current_value.clone().into())
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

                        pub fn [<$bucket_name _push>](
                            &mut self,
                            document_id: &DocumentId,
                            array_path: &Path,
                            value: serde_json::Value,
                        ) -> Result<u32> {
                            let last_element_index= self
                                .[<$bucket_name _last_element_index>](document_id, array_path)?
                                .ok_or_else(|| anyhow!("Can not get length of array at path {array_path:?}"))?;
                            dbg!(&last_element_index);
                            let push_path = array_path
                                .iter()
                                .cloned()
                                .chain(vec![PathSegment::JsonArrayIndex(last_element_index + 1)].into_iter())
                                .collect::<Vec<_>>();
                            self.[<$bucket_name _update>](document_id.clone(), push_path, value)?;
                            Ok(last_element_index)
                        }
                    }
                )*
            }
        }
    };
}

impl<'a> FallibleIterator for DocumentsIterator<'a> {
    type Item = Document;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        if self.last_entry.is_none() {
            self.last_entry = self
                .data_table_iterator
                .next()
                .with_context(|| "Can not get first data table entry")?;
        }
        if let Some(first_document_entry) = self.last_entry.clone() {
            let document_id = first_document_entry.0 .0;
            let mut flat_document: FlatDocument = Vec::new();
            flat_document.push((first_document_entry.0 .1, first_document_entry.1));
            loop {
                self.last_entry = self.data_table_iterator.next().with_context(|| {
                    format!(
                        "Can not get next data table entry after {:?}",
                        self.last_entry
                    )
                })?;
                if let Some(current_entry) = &self.last_entry {
                    if current_entry.0 .0 != document_id {
                        break;
                    }
                    flat_document.push((current_entry.0 .1.clone(), current_entry.1.clone()));
                } else {
                    break;
                }
            }
            Ok(nest(&flat_document)?.and_then(|value| {
                Some(Document {
                    id: document_id,
                    value,
                })
            }))
        } else {
            Ok(None)
        }
    }
}

pub fn flatten_to(
    path: Path,
    value: &serde_json::Value,
    result: &mut Vec<(Path, serde_json::Value)>,
    emit_complex_objects_too: bool,
) -> Result<()> {
    match value {
        serde_json::Value::Object(map) => {
            if emit_complex_objects_too {
                result.push((path.clone(), value.clone()));
            }
            for (key, internal_value) in map {
                let internal_path = path
                    .iter()
                    .cloned()
                    .chain(vec![PathSegment::JsonObjectKey(key.clone())].into_iter())
                    .collect::<Path>();
                flatten_to(
                    internal_path,
                    &internal_value,
                    result,
                    emit_complex_objects_too,
                )
                .with_context(|| {
                    format!("Can not get flat representation of value {internal_value:?} part")
                })?;
            }
        }
        serde_json::Value::Array(array) => {
            if emit_complex_objects_too {
                result.push((path.clone(), value.clone()));
            }
            for (internal_value_index, internal_value) in array.iter().enumerate() {
                let internal_path = path
                    .iter()
                    .cloned()
                    .chain(
                        vec![PathSegment::JsonArrayIndex(internal_value_index as u32)].into_iter(),
                    )
                    .collect::<Path>();
                flatten_to(
                    internal_path,
                    &internal_value,
                    result,
                    emit_complex_objects_too,
                )
                .with_context(|| {
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

pub fn flatten(
    path: &Path,
    value: &serde_json::Value,
    emit_complex_objects_too: bool,
) -> Result<Vec<(Path, serde_json::Value)>> {
    let mut result: Vec<(Path, serde_json::Value)> = Vec::new();
    flatten_to(path.clone(), value, &mut result, emit_complex_objects_too).with_context(|| {
        format!(
            "Can not merge flat representation of value {value:?} part at path {path:?} into \
             {result:?}"
        )
    })?;
    Ok(result)
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

#[macro_export]
macro_rules! path_segments {
    ( $( $seg:expr ),+ $(,)? ) => {
        vec![
            $(
                $crate::PathSegment::from($seg)
            ),+
        ]
    };
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nanorand::{Rng, WyRand};
    use serde_json::json;

    use super::*;
    use crate::Document;
    use fallible_iterator::FallibleIterator;
    use pretty_assertions::assert_eq;

    define_chest!(test_chest(
        main_bucket
        another_bucket
    ) {
    } use {
    });

    use test_chest::Chest;

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
                let mut previously_added_documents: BTreeMap<DocumentId, serde_json::Value> =
                    BTreeMap::new();
                for _ in 0..400 {
                    let action_id = if previously_added_documents.is_empty() {
                        1
                    } else {
                        rng.generate_range(1..=3)
                    };
                    match action_id {
                        1 => {
                            let new_documents = (0..1)
                                .map(|_| {
                                    let json = json_generator.generate(3);
                                    (transaction.main_bucket_insert(json.clone()).unwrap(), json)
                                })
                                .collect::<Vec<_>>();
                            dbg!(&new_documents);
                            for (document_id, document_value) in new_documents.iter() {
                                assert_eq!(
                                    transaction
                                        .main_bucket_contains_document_with_id(document_id)?,
                                    true
                                );
                                let result =
                                    transaction.main_bucket_get(&document_id, &vec![])?.unwrap();
                                assert_eq!(result, *document_value);
                                let flatten_document = flatten(&vec![], &document_value, false)?;
                                for (pathvalue_index, (path, value)) in
                                    flatten_document.iter().enumerate()
                                {
                                    transaction
                                        .main_bucket_contains_exact_path(document_id, path)?;
                                    for segments_count in 0..path.len() {
                                        assert_eq!(
                                            transaction.main_bucket_contains_path(
                                                document_id,
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
                                                    transaction.main_bucket_contains_element(
                                                        document_id,
                                                        &base_path,
                                                        &value_as_json
                                                    )?,
                                                    false
                                                );
                                                assert_eq!(
                                                    transaction
                                                        .main_bucket_last_element_index(
                                                            document_id,
                                                            &base_path
                                                        )
                                                        .unwrap(),
                                                    Some(0)
                                                );
                                                let selected = transaction
                                                    .main_bucket_select(
                                                        &vec![(
                                                            IndexRecordType::Direct,
                                                            path.clone(),
                                                            value_as_json.clone(),
                                                        )],
                                                        &vec![],
                                                        None,
                                                    )?
                                                    .collect::<Vec<DocumentId>>()?;
                                                for selected_document_id in selected.iter() {
                                                    assert_eq!(
                                                        transaction
                                                            .main_bucket_get(
                                                                &selected_document_id,
                                                                &path
                                                            )?
                                                            .unwrap(),
                                                        value_as_json
                                                    );
                                                }
                                                assert!(selected.iter().any(
                                                    |selected_document_id| {
                                                        selected_document_id == document_id
                                                    }
                                                ));
                                            }
                                            PathSegment::JsonArrayIndex(current_array_index) => {
                                                assert_eq!(
                                                    transaction.main_bucket_contains_element(
                                                        document_id,
                                                        &base_path,
                                                        &value_as_json
                                                    )?,
                                                    true
                                                );
                                                assert_eq!(
                                                    transaction
                                                        .main_bucket_get_element_index(
                                                            document_id,
                                                            &base_path,
                                                            &value_as_json
                                                        )
                                                        .unwrap(),
                                                    Some(*current_array_index)
                                                );
                                                let selected = transaction
                                                    .main_bucket_select(
                                                        &vec![(
                                                            IndexRecordType::Array,
                                                            base_path.clone(),
                                                            value_as_json.clone(),
                                                        )],
                                                        &vec![],
                                                        None,
                                                    )?
                                                    .collect::<Vec<DocumentId>>()?;
                                                for selected_document_id in selected.iter() {
                                                    assert!(transaction
                                                        .main_bucket_get(
                                                            &selected_document_id,
                                                            &base_path
                                                        )?
                                                        .unwrap()
                                                        .as_array()
                                                        .unwrap()
                                                        .iter()
                                                        .any(|got_array_element| {
                                                            got_array_element == &value_as_json
                                                        }));
                                                }
                                                assert!(selected.iter().any(
                                                    |selected_document_id| {
                                                        selected_document_id == document_id
                                                    }
                                                ));
                                                if flatten_document
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
                                                    transaction
                                                        .main_bucket_remove(document_id, path)
                                                        .unwrap();
                                                    transaction
                                                        .main_bucket_push(
                                                            document_id,
                                                            &base_path,
                                                            value_as_json.clone(),
                                                        )
                                                        .unwrap();
                                                    dbg!(&document_id, &base_path);
                                                    assert_eq!(
                                                        transaction
                                                            .main_bucket_last_element_index(
                                                                document_id,
                                                                &base_path
                                                            )
                                                            .unwrap(),
                                                        Some(*current_array_index),
                                                    );
                                                    assert_eq!(
                                                        transaction
                                                            .main_bucket_last(
                                                                document_id,
                                                                &base_path
                                                            )
                                                            .unwrap(),
                                                        Some(value_as_json.clone())
                                                    );
                                                }
                                            }
                                        }
                                    } else {
                                        dbg!(&value_as_json);
                                        let selected = transaction
                                            .main_bucket_select(
                                                &vec![(
                                                    IndexRecordType::Direct,
                                                    vec![],
                                                    value_as_json.clone(),
                                                )],
                                                &vec![],
                                                None,
                                            )?
                                            .collect::<Vec<DocumentId>>()?;
                                        for selected_document_id in selected.iter() {
                                            assert_eq!(
                                                transaction
                                                    .main_bucket_get(
                                                        &selected_document_id,
                                                        &vec![]
                                                    )?
                                                    .unwrap(),
                                                value_as_json
                                            );
                                        }
                                        assert!(selected.iter().any(|selected_document_id| {
                                            selected_document_id == document_id
                                        }));
                                    }
                                }
                            }
                            previously_added_documents.extend(new_documents);
                            assert_eq!(
                                transaction
                                    .main_bucket_documents()?
                                    .map(|current_document| Ok((
                                        current_document.id,
                                        current_document.value
                                    )))
                                    .collect::<BTreeMap<_, _>>()?,
                                previously_added_documents
                            );
                        }
                        2 => {
                            let document_to_remove_id = previously_added_documents
                                .keys()
                                .nth(rng.generate_range(0..previously_added_documents.len()))
                                .unwrap()
                                .clone();
                            transaction.main_bucket_remove(&document_to_remove_id, &vec![])?;
                            previously_added_documents.remove(&document_to_remove_id);
                            assert_eq!(
                                transaction.main_bucket_get(&document_to_remove_id, &vec![])?,
                                None
                            );
                            assert_eq!(
                                transaction.main_bucket_get(&document_to_remove_id, &vec![])?,
                                None
                            );
                        }
                        3 => {
                            let document_to_remove_from_id = previously_added_documents
                                .keys()
                                .nth(rng.generate_range(0..previously_added_documents.len()))
                                .unwrap()
                                .clone();
                            let flattened_document_to_remove_from = transaction
                                .main_bucket_get_flattened(&document_to_remove_from_id, &vec![])?;
                            let path_to_remove = flattened_document_to_remove_from
                                [rng.generate_range(0..flattened_document_to_remove_from.len())]
                            .0
                            .clone();
                            let correct_result_option = nest(
                                &flattened_document_to_remove_from
                                    .into_iter()
                                    .filter(|(path, _)| !path.starts_with(&path_to_remove))
                                    .map(|(path, value)| (path, value.into()))
                                    .collect::<Vec<_>>(),
                            )?;
                            transaction
                                .main_bucket_remove(&document_to_remove_from_id, &path_to_remove)?;
                            previously_added_documents.remove(&document_to_remove_from_id);
                            if let Some(ref correct_result) = correct_result_option {
                                previously_added_documents.insert(
                                    document_to_remove_from_id.clone(),
                                    correct_result.clone(),
                                );
                            }
                            assert_eq!(
                                transaction.main_bucket_get(
                                    &document_to_remove_from_id,
                                    &path_to_remove
                                )?,
                                None
                            );
                            assert_eq!(
                                transaction
                                    .main_bucket_get(&document_to_remove_from_id, &vec![])?,
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
        let document_json = json!({"a.b.c": 1});

        chest
            .lock_all_and_write(|transaction| {
                let document = Document {
                    id: transaction.main_bucket_insert(document_json.clone())?,
                    value: document_json.clone(),
                };
                assert_eq!(
                    transaction.main_bucket_documents()?.collect::<Vec<_>>()?,
                    vec![document.clone()]
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_simple() {
        let mut chest = new_default_chest("test_simple");
        let document_json = json!({
            "dict": {
                "hello": ["number", 42, -4.2, 0.0],
                "boolean": false
            },
            "null": null,
            "array": [1, ["two", false], [null]]
        });

        chest
            .lock_all_and_write(|transaction| {
                let document = Document {
                    id: transaction.main_bucket_insert(document_json.clone())?,
                    value: document_json.clone(),
                };

                assert_eq!(
                    transaction.main_bucket_documents()?.collect::<Vec<_>>()?,
                    vec![document.clone()]
                );

                assert_eq!(
                    &transaction
                        .main_bucket_get(&document.id, &path_segments!("dict"))?
                        .unwrap(),
                    document.value.as_object().unwrap().get("dict").unwrap()
                );
                assert_eq!(
                    &transaction
                        .main_bucket_get(&document.id, &path_segments!("dict", "hello"))?
                        .unwrap(),
                    document
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
                        .main_bucket_get(&document.id, &path_segments!("dict", "boolean"))?
                        .unwrap(),
                    document
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
                        .main_bucket_get(&document.id, &path_segments!("dict", "hello", 0))?
                        .unwrap(),
                    document
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
                        .main_bucket_get(&document.id, &path_segments!("dict", "hello", 1))?
                        .unwrap(),
                    document
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
                        .main_bucket_get(&document.id, &path_segments!("dict", "hello", 2))?
                        .unwrap(),
                    document
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
                        .main_bucket_get(&document.id, &path_segments!("dict", "hello", 3))?
                        .unwrap(),
                    document
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
