use crate::{
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError, Tree as LsmTree, UserValue,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Cursor, Read, Write},
    sync::Arc,
};
use value_log::ValueHandle;

#[derive(Debug)]
pub enum MaybeInlineValue {
    Inline(UserValue),
    Indirect(ValueHandle),
}

// Implement Serializable trait for ValueHandle
impl Serializable for ValueHandle {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.offset)?;
        writer.write_u64::<BigEndian>(self.segment_id)?;
        Ok(())
    }
}

// Implement Deserializable trait for ValueHandle
impl Deserializable for ValueHandle {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let offset = reader.read_u64::<BigEndian>()?;
        let segment_id = reader.read_u64::<BigEndian>()?;

        Ok(Self { segment_id, offset })
    }
}

// Implement Serializable trait for MaybeInlineValue
impl Serializable for MaybeInlineValue {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        match self {
            Self::Inline(bytes) => {
                writer.write_u8(0)?;
                writer.write_u64::<BigEndian>(bytes.len() as u64)?;
                writer.write_all(bytes)?;
            }
            Self::Indirect(value_handle) => {
                writer.write_u8(1)?;
                value_handle.serialize(writer)?;
            }
        }
        Ok(())
    }
}

// Implement Deserializable trait for MaybeInlineValue
impl Deserializable for MaybeInlineValue {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let tag = reader.read_u8()?;

        match tag {
            0 => {
                let len = reader.read_u64::<BigEndian>()? as usize;
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes)?;

                Ok(Self::Inline(Arc::from(bytes)))
            }
            1 => {
                let handle = ValueHandle::deserialize(reader)?;
                Ok(Self::Indirect(handle))
            }
            _ => {
                panic!("Invalid tag");
            }
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct IndexTree(pub(crate) LsmTree);

impl IndexTree {
    pub fn get_internal(&self, key: &[u8]) -> crate::Result<Option<MaybeInlineValue>> {
        let Some(item) = self.0.get(key).expect("oh no") else {
            return Ok(None);
        };

        let mut cursor = Cursor::new(item);
        let item = MaybeInlineValue::deserialize(&mut cursor).expect("should deserialize");

        Ok(Some(item))
    }
}

impl value_log::ExternalIndex for IndexTree {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        let Some(item) = self.get_internal(key).expect("should get value") else {
            return Ok(None);
        };

        match item {
            MaybeInlineValue::Inline(_) => Ok(None),
            MaybeInlineValue::Indirect(handle) => Ok(Some(handle)),
        }
    }
}

impl From<LsmTree> for IndexTree {
    fn from(value: LsmTree) -> Self {
        Self(value)
    }
}
