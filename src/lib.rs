use anyhow::{Error, Result, anyhow};
use fallible_iterator::FallibleIterator;
use xxhash_rust::xxh3::xxh3_128;

use serde::{Deserialize, Serialize};

#[derive(
    Clone, Default, PartialEq, PartialOrd, Debug, bincode::Encode, bincode::Decode, Eq, Ord, Hash,
)]
pub struct ObjectId {
    pub value: [u8; 16],
}

impl ObjectId {
    pub fn new() -> Result<Self> {
        Ok(Self {
            value: *uuid::Uuid::now_v7().as_bytes(),
        })
    }
}

dream::define_index!(trove_database {
    object_id_and_path_to_value<(ObjectId, String), Vec<u8>>,
} use {
    use crate::ObjectId;
});

struct Chest {
    index: trove_database::Index,
}
