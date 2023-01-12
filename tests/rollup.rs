use tempfile::tempdir;
use terminus_store_10 as store_10;
use terminus_store_11 as store_11;

use tokio;

use terminusdb_10_to_11;

fn num_val(num: u64) -> String {
    format!("{num}^^'http://www.w3.org/2001/XMLSchema#unsignedLong'")
}

#[tokio::test]
async fn foo() {
    let dir = tempdir().unwrap();
    let s10 = store_10::open_directory_store(dir.path());
    let graph = s10.create("hello").await.unwrap();
    let builder = s10.create_base_layer().await.unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(2))).unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(20))).unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(4))).unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(40))).unwrap();
    let layer = builder.commit().await.unwrap();
    let builder = layer.open_write().await.unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(1))).unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(10))).unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(3))).unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(30))).unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(5))).unwrap();
    builder.add_string_triple(store_10::StringTriple::new_value("a", "b", &num_val(50))).unwrap();
    let layer2 = builder.commit().await.unwrap();
    graph.set_head(&layer2).await.unwrap();

    layer2.rollup().await.unwrap();

    let destination_dir = tempdir().unwrap();
    let work_dir = tempdir().unwrap();

    terminusdb_10_to_11::convert_store::convert_store(dir.path().to_str().unwrap(), destination_dir.path().to_str().unwrap(), work_dir.path().to_str().unwrap(), false, false).await.unwrap();

    let s11 = store_11::open_archive_store(destination_dir.path());
    let new_layer = s11.get_layer_from_id(terminus_store_10::Layer::name(&layer2)).await.unwrap().unwrap();
    //new_layer.rollup().await.unwrap();
    //std::mem::drop(new_layer);
    //let new_layer = s11.get_layer_from_id(terminus_store_10::Layer::name(&layer2)).await.unwrap().unwrap();

    let result: Vec<_> = store_11::Layer::triples(&new_layer).map(|t| store_11::Layer::id_object(&new_layer, t.object).unwrap()).map(|o| o.value().unwrap().as_val::<u64, u64>()).collect();
    dbg!(&result);

    assert_eq!(vec![2,4,20,40,1,3,5,10,30,50], result);
}
