use terminus_store_10::storage::directory as directory_10;
use terminus_store_11::storage::archive as archive_11;

use crate::convert_layer::*;
use crate::reachable::*;

use std::io;
use std::path::PathBuf;

use tokio::fs;

pub async fn convert_store(from: &str, to: &str, work: &str, naive: bool) -> io::Result<()> {
    let v10_layer_store = directory_10::DirectoryLayerStore::new(from);
    let v10_label_store = directory_10::DirectoryLabelStore::new(from);
    let v11_layer_store = archive_11::ArchiveLayerStore::new(to);

    let reachable = find_reachable_layers(&v10_layer_store, &v10_label_store).await?;

    let mut visit_queue = Vec::new();
    visit_queue.extend(reachable[&None].clone());

    while let Some(layer) = visit_queue.pop() {
        convert_layer_with_stores(&v10_layer_store, &v11_layer_store, work, naive, layer).await?;
        if let Some(children) = reachable.get(&Some(layer)) {
            visit_queue.extend(children.clone());
        }
    }

    convert_labels(from, to).await
}

pub async fn convert_labels(from: &str, to: &str) -> io::Result<()> {
    let v11_store_path = PathBuf::from(to);
    let mut stream = fs::read_dir(from).await?;
    while let Some(direntry) = stream.next_entry().await? {
        if direntry.file_type().await?.is_file() {
            let os_name = direntry.file_name();
            let name = os_name.to_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected non-utf8 directory name",
                )
            })?;
            if name.ends_with(".label") {
                let mut to_path = v11_store_path.clone();
                to_path.push(name);
                fs::copy(direntry.path(), to_path).await?;
            }
        }
    }

    Ok(())
}
