use terminus_store_10::storage as storage_10;
use terminus_store_10::storage::directory as directory_10;
use terminus_store_11::storage as storage_11;
use terminus_store_11::storage::archive as archive_11;
use terminus_store_11::storage::directory as directory_11;

use crate::convert_layer::*;
use crate::reachable::*;

use std::io;

pub async fn convert_store(from: &str, to: &str, work: &str) -> io::Result<()> {
    let v10_layer_store = directory_10::DirectoryLayerStore::new(from);
    let v10_label_store = directory_10::DirectoryLabelStore::new(from);
    let v11_layer_store = archive_11::ArchiveLayerStore::new(to);
    let v11_label_store = directory_11::DirectoryLabelStore::new(to);

    let reachable = find_reachable_layers(&v10_layer_store, &v10_label_store).await?;

    let mut visit_queue = Vec::new();
    visit_queue.extend(reachable[&None].clone());

    while let Some(layer) = visit_queue.pop() {
        convert_layer_with_stores(&v10_layer_store, &v11_layer_store, work, layer).await?;
        if let Some(children) = reachable.get(&Some(layer)) {
            visit_queue.extend(children.clone());
        }
    }

    Ok(())
}
