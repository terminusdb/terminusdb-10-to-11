use bytes::{Bytes, BytesMut};
use futures::stream::TryStreamExt;
use std::collections::HashMap;
use std::io;
use terminus_store_10::storage as storage_10;
use terminus_store_10::structure::pfc as pfc_10;
use terminus_store_11::structure::tfc as tfc_11;

use thiserror::*;

use crate::dataconversion::{convert_value_string_to_dict_entry, DataConversionError};

pub struct UntypedDictionaryOutput {
    pub offsets: Bytes,
    pub data: Bytes,
}

pub async fn convert_untyped_dictionary<F: storage_10::FileLoad + 'static>(
    from: F,
) -> io::Result<UntypedDictionaryOutput> {
    eprintln!("time to convert untyped dict");
    let mut stream = pfc_10::dict_file_to_indexed_stream(from, 0).await?;
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

pub struct NaiveTypedDictionaryOutput {
    pub types_present: Bytes,
    pub type_offsets: Bytes,
    pub offsets: Bytes,
    pub data: Bytes,
}

pub async fn convert_naive_typed_dictionary<F: storage_10::FileLoad + 'static>(
    val_dict: F,
) -> io::Result<NaiveTypedDictionaryOutput> {
    let mut stream = pfc_10::dict_file_to_stream(val_dict).await?;

    let mut builder = tfc_11::TypedDictBufBuilder::new(
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
    );

    while let Some(val) = stream.try_next().await? {
        let entry = <String as tfc_11::TdbDataType>::make_entry(&val);
        builder.add(entry);
    }

    let (types_present_buf, type_offsets_buf, offsets_buf, data_buf) = builder.finalize();

    Ok(NaiveTypedDictionaryOutput {
        types_present: types_present_buf.freeze(),
        type_offsets: type_offsets_buf.freeze(),
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
    pub offset: u64,
}

#[derive(Error, Debug)]
pub enum DictionaryConversionError {
    #[error("dictionary failed to convert id {id}: {error}")]
    DataConversion { id: u64, error: DataConversionError },
    #[error("io error: {0}")]
    Io(io::Error),
}

impl From<io::Error> for DictionaryConversionError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

pub async fn convert_typed_dictionary<F: storage_10::FileLoad + 'static>(
    node_dict: F,
    val_dict: F,
    offset: u64,
) -> Result<TypedDictionaryOutput, DictionaryConversionError> {
    let node_count = pfc_10::dict_file_get_count(node_dict).await?;
    let val_count = pfc_10::dict_file_get_count(val_dict.clone()).await?;
    let mut stream = pfc_10::dict_file_to_indexed_stream(val_dict, node_count + offset).await?;

    let mut converted_vals: Vec<(tfc_11::TypedDictEntry, u64)> =
        Vec::with_capacity(val_count as usize);
    while let Some((ix, val)) = stream.try_next().await? {
        converted_vals.push((
            convert_value_string_to_dict_entry(&val)
                .map_err(|e| DictionaryConversionError::DataConversion { id: ix, error: e })?,
            ix,
        ));
    }

    converted_vals.sort();

    let mut builder = tfc_11::TypedDictBufBuilder::new(
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
    );
    let mut mapping: HashMap<u64, u64> = HashMap::with_capacity(converted_vals.len());

    for (new_index, (entry, old_index)) in converted_vals.into_iter().enumerate() {
        builder.add(entry);
        let new_index = new_index as u64 + offset + node_count + 1;
        mapping.insert(old_index, new_index);
    }

    let (types_present_buf, type_offsets_buf, offsets_buf, data_buf) = builder.finalize();

    Ok(TypedDictionaryOutput {
        types_present: types_present_buf.freeze(),
        type_offsets: type_offsets_buf.freeze(),
        offsets: offsets_buf.freeze(),
        data: data_buf.freeze(),
        mapping,
        offset: offset + node_count + val_count,
    })
}
