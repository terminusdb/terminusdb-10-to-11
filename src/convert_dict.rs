use bytes::{Buf, Bytes, BytesMut};
use futures::stream::TryStreamExt;
use std::collections::HashMap;
use std::io;
use terminus_store_10::storage::{self as storage_10, directory as directory_10};
use terminus_store_10::structure::pfc as pfc_10;
use terminus_store_11::structure::tfc as tfc_11;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

pub struct UntypedDictionaryOutput {
    pub offsets: Bytes,
    pub data: Bytes,
}

pub async fn convert_untyped_dictionary_to_files(
    from: &str,
    to_offsets: &str,
    to_data: &str,
) -> io::Result<()> {
    let UntypedDictionaryOutput {
        mut offsets,
        mut data,
    } = convert_untyped_dictionary(directory_10::FileBackedStore::new(from)).await?;

    let mut options = OpenOptions::new();
    options.create_new(true);
    options.write(true);

    let mut to_offsets_file = options.open(to_offsets).await?;
    let mut to_data_file = options.open(to_data).await?;

    while offsets.has_remaining() {
        to_offsets_file.write_buf(&mut offsets).await?;
    }

    while data.has_remaining() {
        to_data_file.write_buf(&mut data).await?;
    }

    to_offsets_file.flush().await?;
    to_data_file.flush().await?;

    Ok(())
}

pub async fn convert_untyped_dictionary<F: storage_10::FileLoad + 'static>(
    from: F,
) -> io::Result<UntypedDictionaryOutput> {
    eprintln!("time to convert untyped dict");
    let open_file = from.open_read().await?;
    eprintln!("file opened");
    let mut stream = pfc_10::dict_reader_to_indexed_stream(open_file, 0);
    eprintln!("opened stream");

    let mut builder = tfc_11::StringDictBufBuilder::new(BytesMut::new(), BytesMut::new());
    while let Some((_ix, val)) = stream.try_next().await? {
        builder.add(Bytes::copy_from_slice(val.as_bytes()));
    }

    let (offsets_buf, data_buf) = builder.finalize();

    Ok(UntypedDictionaryOutput {
        offsets: offsets_buf.freeze(),
        data: data_buf.freeze(),
    })
}

pub struct TypedDictionaryOutput {
    pub types_present: Bytes,
    pub type_offsets: Bytes,
    pub offsets: Bytes,
    pub data: Bytes,
    pub mapping: HashMap<u64, u64>,
}

pub async fn convert_typed_dictionary<F: storage_10::FileLoad + 'static>(
    node_dict: F,
    val_dict: F,
) -> io::Result<TypedDictionaryOutput> {
    let node_count = pfc_10::dict_file_get_count(node_dict).await?;
    let val_count = pfc_10::dict_file_get_count(val_dict.clone()).await?;
    let mut stream = pfc_10::dict_reader_to_indexed_stream(val_dict.open_read().await?, 0);

    let mut converted_vals: Vec<(tfc_11::TypedDictEntry, u64)> = Vec::with_capacity(val_count as usize);
    while let Some((ix, val)) = stream.try_next().await? {
        converted_vals.push((convert_value_string_to_dict_entry(&val), ix));
    }

    converted_vals.sort();
    dbg!(converted_vals.len());

    let mut builder = tfc_11::TypedDictBufBuilder::new(
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
    );
    let mut mapping: HashMap<u64, u64> = HashMap::with_capacity(converted_vals.len());

    for (new_index, (entry, old_index)) in converted_vals.into_iter().enumerate() {
        builder.add(entry);
        let new_index = new_index as u64 + 1;
        mapping.insert(old_index + node_count, new_index + node_count);
    }

    let (types_present_buf, type_offsets_buf, offsets_buf, data_buf) = builder.finalize();

    Ok(TypedDictionaryOutput {
        types_present: types_present_buf.freeze(),
        type_offsets: type_offsets_buf.freeze(),
        offsets: offsets_buf.freeze(),
        data: data_buf.freeze(),
        mapping,
    })
}

fn convert_value_string_to_dict_entry(value: &str) -> tfc_11::TypedDictEntry {
    <String as tfc_11::TdbDataType>::make_entry(&value)
}
