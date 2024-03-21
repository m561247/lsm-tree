use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn blob_simple() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::BlobTree::open(path)?;

    assert!(tree.get("abc")?.is_none());
    tree.insert("abc", "asd", 0);

    let value = tree.get("abc")?.expect("should exist");
    assert_eq!(&*value, b"asd");

    assert!(tree.get("asd")?.is_none());

    Ok(())
}
