use tempfile::tempdir;
use terminus_store_10::store::sync as sync_10;
use terminus_store_10 as store_10;
use terminus_store_11::store::sync as sync_11;

#[test]
fn foo() {
    let dir = tempdir().unwrap();
    let s10 = sync_10::open_sync_directory_store(dir.path());
    let graph = s10.create("hello").unwrap();
    let builder = s10.create_base_layer().unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", "\"c\"^^'http://www.w3.org/2001/XMLSchema#string'")).unwrap();
    let layer = builder.commit().unwrap();
    graph.set_head(&layer).unwrap();

    assert!(false);
}
