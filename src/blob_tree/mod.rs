pub mod index;
mod value;

use crate::{
    r#abstract::AbstractTree,
    range::{Mapper, Range},
    serde::{Deserializable, Serializable},
    SeqNo,
};
use index::IndexTree;
use std::{io::Cursor, ops::RangeBounds, path::Path, sync::Arc};
use value_log::{ValueHandle, ValueLog};

/// A key-value separated log-structured merge tree
///
/// The tree consists of an index tree (LSM-tree) and a log-structured value log
/// to reduce write amplification.
/// See <https://docs.rs/value-log> for more information.
pub struct BlobTree {
    index: IndexTree,
    blobs: ValueLog<IndexTree>,
}

/* struct IndexWriter {
    batch: Vec<(UserKey, ValueHandle)>,
    tree: crate::Tree,
} */

/* impl IndexWriter {
    pub fn new(tree: crate::Tree) -> Self {
        Self {
            batch: Vec::default(),
            tree,
        }
    }
} */

/* impl value_log::IndexWriter for IndexWriter {
    fn insert_indirection(&mut self, key: &[u8], value: ValueHandle) -> std::io::Result<()> {
        self.batch.push((key.into(), value));
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        for (key, handle) in self.batch.drain(..) {
            let mut value = vec![];
            handle.serialize(&mut value).expect("should serialize");

            self.tree
                .insert(key, value, 0 /* where to get seqno from D: */);
        }
        Ok(())
    }
} */

impl BlobTree {
    pub fn open<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        let vlog_path = path.join("blobs");

        let vlog_cfg = value_log::Config::default();

        let index: IndexTree = crate::Config::new(path).open()?.into();

        Ok(Self {
            index: index.clone(),
            blobs: ValueLog::open(vlog_path, vlog_cfg, index)?,
        })
    }

    pub fn flush_active_memtable(&self) -> crate::Result<Option<()>> {
        use crate::{
            file::SEGMENTS_FOLDER,
            segment::writer::{Options, Writer as SegmentWriter},
        };
        use value::MaybeInlineValue;

        log::debug!("flushing active memtable & performing key-value separation");

        let Some((segment_id, yanked_memtable)) = self.index.0.rotate_memtable() else {
            return Ok(None);
        };

        let lsm_segment_folder = self
            .index
            .0
            .path
            .join(SEGMENTS_FOLDER)
            .join(segment_id.to_string());

        let mut segment_writer = SegmentWriter::new(Options {
            block_size: self.index.0.config.block_size,
            evict_tombstones: false,
            folder: lsm_segment_folder,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.0001,
        })?;
        let mut blob_writer = self.blobs.get_writer()?;

        let blob_id = blob_writer.segment_id();

        for entry in &yanked_memtable.items {
            let key = entry.key();

            let value = entry.value();
            let mut cursor = Cursor::new(value);
            let value = MaybeInlineValue::deserialize(&mut cursor).expect("oops");
            let MaybeInlineValue::Inline(value) = value else {
                panic!("values are initially always inlined");
            };

            let size = value.len();

            if size >= 4_096 {
                let offset = blob_writer.offset(&key.user_key);
                let value_handle = ValueHandle {
                    offset,
                    segment_id: blob_id,
                };

                let mut serialized_handle = vec![];
                value_handle
                    .serialize(&mut serialized_handle)
                    .expect("should serialize");

                blob_writer.write(&key.user_key, &value)?;
                segment_writer.write(crate::Value::new(
                    key.user_key.clone(),
                    serialized_handle,
                    key.seqno,
                    crate::ValueType::Value,
                ))?;
            } else {
                segment_writer.write(crate::Value::from(((key.clone()), value.clone())))?;
            }
        }

        self.blobs.register(blob_writer)?;
        segment_writer.finish()?;
        self.index.0.consume_writer(segment_id, segment_writer)?;

        Ok(None)
    }
}

struct VlogMapper {
    blobs: ValueLog<IndexTree>,
}

impl Mapper for VlogMapper {
    fn map(
        &self,
        item: crate::r#abstract::RangeItem,
        _seqno: Option<SeqNo>,
    ) -> Option<crate::r#abstract::RangeItem> {
        use value::MaybeInlineValue;

        match item {
            Ok((key, value)) => {
                let mut cursor = Cursor::new(value);
                let item = MaybeInlineValue::deserialize(&mut cursor).expect("should deserialize");

                match item {
                    MaybeInlineValue::Inline(bytes) => Some(Ok((key, bytes))),
                    MaybeInlineValue::Indirect(handle) => match self.blobs.get(&handle) {
                        Ok(Some(bytes)) => Some(Ok((key, bytes))),
                        Ok(None) => None,
                        Err(e) => Some(Err(e.into())),
                    },
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl AbstractTree for BlobTree {
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> Range {
        let mapper = VlogMapper {
            blobs: self.blobs.clone(),
        };
        self.index.0.create_range(range, None, Box::new(mapper))
    }

    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, seqno: SeqNo) -> (u32, u32) {
        use value::MaybeInlineValue;

        // NOTE: Initially, we always write an inline value
        // On memtable flush, depending on the values' sizes, they will be separated
        // into inline or indirect values
        let item = MaybeInlineValue::Inline(value.as_ref().into());

        let mut value = vec![];
        item.serialize(&mut value).expect("should serialize");

        self.index.0.insert(key, value, seqno)
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<Arc<[u8]>>> {
        use value::MaybeInlineValue::{Indirect, Inline};

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
