use lsm_tree::AbstractTree;
use std::path::Path;
use test_log::test;

#[test]
#[ignore]
fn blob_simple() -> lsm_tree::Result<()> {
    /* let folder = tempfile::tempdir()?;
    let path = folder.path(); */

    let path = Path::new(".blobby");

    if path.try_exists()? {
        std::fs::remove_dir_all(&path)?;
    }

    std::fs::create_dir_all(&path)?;

    let tree = lsm_tree::BlobTree::open(path)?;

    let big_value = b"neptune!".repeat(128_000);

    assert!(tree.get("big")?.is_none());
    tree.insert("big", &big_value, 0);
    tree.insert("smol", "small value", 0);

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, big_value);

    assert!(tree.get("xyz")?.is_none());

    tree.flush_active_memtable()?;

    let value = tree.get("big")?.expect("should exist");
    assert_eq!(&*value, big_value);

    let value = tree.get("smol")?.expect("should exist");
    assert_eq!(&*value, b"small value");

    assert!(tree.get("xyz")?.is_none());

    Ok(())
}
