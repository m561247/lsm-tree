pub mod index;
mod value;

use crate::{r#abstract::AbstractTree, serde::Serializable, SeqNo};
use index::IndexTree;
use std::{path::Path, sync::Arc};
use value::MaybeInlineValue;
use value_log::ValueLog;

/// A key-value separated log-structured merge tree
///
/// The tree consists of an index tree (LSM-tree) and a log-structured value log
/// to reduce write amplification.
/// See <https://docs.rs/value-log> for more information.
pub struct BlobTree {
    index: IndexTree,
    blobs: ValueLog,
}

impl BlobTree {
    pub fn open<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        let tree_path = path.join("index");
        let vlog_path = path.join("blobs");

        let vlog_cfg = value_log::Config::default();

        let index: IndexTree = crate::Config::new(tree_path).open()?.into();

        Ok(Self {
            index: index.clone(),
            blobs: ValueLog::open(vlog_path, vlog_cfg, Arc::new(index))?,
        })
    }
}

impl AbstractTree for BlobTree {
    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, seqno: SeqNo) -> (u32, u32) {
        // NOTE: Initially, we always write an inline value
        // On memtable flush, depending on the values' sizes, they will be separated
        // into inline or indirect values
        let item = MaybeInlineValue::Inline(value.as_ref().into());

        let mut value = vec![];
        item.serialize(&mut value).expect("should serialize");

        self.index.0.insert(key, value, seqno)
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<Arc<[u8]>>> {
        use MaybeInlineValue::{Indirect, Inline};

        let Some(value) = self.index.get_internal(key.as_ref())? else {
            return Ok(None);
        };

        match value {
            Inline(bytes) => Ok(Some(bytes)),
            Indirect(handle) => {
                // Resolve indirection using value log
                self.blobs.get(&handle).map_err(Into::into)
            }
        }
    }

    fn remove<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        self.index.0.remove(key, seqno)
    }
}
