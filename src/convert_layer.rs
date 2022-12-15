use terminus_store_10::storage as storage_10;
use terminus_store_10::storage::directory as directory_10;
use terminus_store_11::storage as storage_11;
use terminus_store_11::storage::archive as archive_11;
use terminus_store_11::storage::string_to_name;

use crate::consts::*;
use crate::convert_dict::*;

use std::collections::HashMap;

use bytes::Buf;
use tokio::io::AsyncWriteExt;

use std::io;
pub async fn convert_layer(from: &str, to: &str, id: &str) -> io::Result<()> {
    let id = string_to_name(id).unwrap();
    let v10_store = directory_10::DirectoryLayerStore::new(from);
    let is_child = storage_10::PersistentLayerStore::layer_has_parent(&v10_store, id).await?;
    let v11_store = archive_11::ArchiveLayerStore::new(to);

    todo!();
}

async fn convert_dictionaries(
    v10_store: &directory_10::DirectoryLayerStore,
    v11_store: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
) -> io::Result<HashMap<u64, u64>> {
    let node_dict_pfc = storage_10::PersistentLayerStore::get_file(
        v10_store,
        id,
        V10_FILENAMES.node_dictionary_blocks,
    )
    .await?;
    let UntypedDictionaryOutput { offsets, data } =
        convert_untyped_dictionary(node_dict_pfc).await?;

    write_buf_to_file(v11_store, id, V11_FILENAMES.node_dictionary_blocks, data).await?;
    write_buf_to_file(
        v11_store,
        id,
        V11_FILENAMES.node_dictionary_offsets,
        offsets,
    )
    .await?;

    let predicate_dict_pfc = storage_10::PersistentLayerStore::get_file(
        v10_store,
        id,
        V10_FILENAMES.predicate_dictionary_blocks,
    )
    .await?;
    let UntypedDictionaryOutput { offsets, data } =
        convert_untyped_dictionary(predicate_dict_pfc).await?;

    write_buf_to_file(
        v11_store,
        id,
        V11_FILENAMES.predicate_dictionary_blocks,
        data,
    )
    .await?;
    write_buf_to_file(
        v11_store,
        id,
        V11_FILENAMES.predicate_dictionary_offsets,
        offsets,
    )
    .await?;

    let value_dict_pfc = storage_10::PersistentLayerStore::get_file(
        v10_store,
        id,
        V10_FILENAMES.value_dictionary_blocks,
    )
    .await?;
    let TypedDictionaryOutput {
        types_present,
        type_offsets,
        offsets,
        data,
        mapping,
    } = convert_typed_dictionary(value_dict_pfc).await?;

    write_buf_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_types_present,
        types_present,
    )
    .await?;
    write_buf_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_type_offsets,
        type_offsets,
    )
    .await?;
    write_buf_to_file(v11_store, id, V11_FILENAMES.value_dictionary_blocks, data).await?;
    write_buf_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_offsets,
        offsets,
    )
    .await?;

    Ok(mapping)
}

async fn write_buf_to_file<B: Buf>(
    store: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    file: &str,
    mut buf: B,
) -> io::Result<()> {
    let file = storage_11::PersistentLayerStore::get_file(store, id, file).await?;
    let mut writer = storage_11::FileStore::open_write(&file).await?;

    while buf.has_remaining() {
        writer.write_buf(&mut buf).await?;
    }

    writer.flush().await?;
    storage_11::SyncableFile::sync_all(writer).await
}
