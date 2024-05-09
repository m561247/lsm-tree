use super::value::MaybeInlineValue;
use crate::{serde::Deserializable, Tree as LsmTree};
use std::io::Cursor;
use value_log::ValueHandle;

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
