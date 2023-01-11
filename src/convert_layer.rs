use terminus_store_10::storage as storage_10;
use terminus_store_10::storage::directory as directory_10;
use terminus_store_10::structure as structure_10;
use terminus_store_10::structure::util as util_10;
use terminus_store_11::layer::builder as builder_11;
use terminus_store_11::storage as storage_11;
use terminus_store_11::storage::archive as archive_11;
use terminus_store_11::storage::consts as consts_11;
use terminus_store_11::storage::{name_to_string, string_to_name};
use terminus_store_11::structure as structure_11;
use tokio::io::AsyncReadExt;

use crate::consts::*;
use crate::convert_dict::*;
use crate::convert_triples::*;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use bytes::Bytes;

use serde::{Deserialize, Serialize};

use tokio::io::AsyncWriteExt;

use thiserror::Error;

pub async fn convert_layer(
    from: &str,
    to: &str,
    work: &str,
    naive: bool,
    id_string: &str,
) -> Result<(), LayerConversionError> {
    let v10_store = directory_10::DirectoryLayerStore::new(from);
    let v11_store = archive_11::ArchiveLayerStore::new(to);
    let id = string_to_name(id_string).unwrap();

    convert_layer_with_stores(&v10_store, &v11_store, work, naive, id).await
}

#[derive(Debug, Error)]
pub enum InnerLayerConversionError {
    #[error(transparent)]
    DictionaryConversion(#[from] DictionaryConversionError),
    #[error("layer was already converted")]
    LayerAlreadyConverted,

    #[error("failed to copy {name}: {source}")]
    FileCopyError { name: String, source: io::Error },

    #[error(transparent)]
    ParentMapError(#[from] ParentMapError),

    #[error("failed to convert triple map: {0}")]
    TripleConversionError(io::Error),

    #[error("failed to convert node value idmap: {0}")]
    NodeValueIdmapConversionError(io::Error),

    #[error("failed to rebuild indexes: {0}")]
    RebuildIndexError(io::Error),

    #[error("failed to finalize layer: {0}")]
    FinalizationError(io::Error),

    #[error("failed to copy rollup file: {0}")]
    RollupFileCopyError(io::Error),

    #[error("failed to write the parent map: {0}")]
    ParentMapWriteError(io::Error),

    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
#[error("Failed to convert layer {}: {source}", name_to_string(self.layer))]
pub struct LayerConversionError {
    layer: [u32; 5],
    source: InnerLayerConversionError,
}

impl LayerConversionError {
    fn new<E: Into<InnerLayerConversionError>>(layer: [u32; 5], source: E) -> Self {
        Self {
            layer,
            source: source.into(),
        }
    }
}

pub async fn convert_layer_with_stores(
    v10_store: &directory_10::DirectoryLayerStore,
    v11_store: &archive_11::ArchiveLayerStore,
    work: &str,
    naive: bool,
    id: [u32; 5],
) -> Result<(), LayerConversionError> {
    eprintln!("converting layer {}", name_to_string(id));
    let is_child = storage_10::PersistentLayerStore::layer_has_parent(v10_store, id)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?;

    if storage_11::PersistentLayerStore::directory_exists(v11_store, id)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?
    {
        return Err(LayerConversionError::new(
            id,
            InnerLayerConversionError::LayerAlreadyConverted,
        ));
    }

    eprintln!("initial setup done");

    storage_11::PersistentLayerStore::create_named_directory(v11_store, id)
        .await
        .map_err(|e| LayerConversionError::new(id, e))?;

    let map;
    let offset;
    if naive {
        naive_convert_dictionaries(v10_store, v11_store, id)
            .await
            .map_err(|e| LayerConversionError::new(id, e))?;
        eprintln!("dictionaries converted");
        copy_unchanged_files(v10_store, v11_store, id).await?;
        copy_naive_unchanged_files(v10_store, v11_store, id, is_child).await?;
        map = None;
        offset = None;
    } else {
        let (mut mapping, offset_1) = get_mapping_and_offset(work, v10_store, id)
            .await
            .map_err(|e| LayerConversionError::new(id, e))?;
        eprintln!("parent mappings retrieved");
        let (mapping_addition, new_offset, num_elements) =
            convert_dictionaries(v10_store, v11_store, id, offset_1)
                .await
                .map_err(|e| LayerConversionError::new(id, e))?;
        mapping.extend(mapping_addition.clone());
        eprintln!("dictionaries converted");
        convert_triples(v10_store, v11_store, id, is_child, &mapping)
            .await
            .map_err(|e| {
                LayerConversionError::new(id, InnerLayerConversionError::TripleConversionError(e))
            })?;
        eprintln!("triples converted");
        convert_node_value_idmap(
            v10_store,
            v11_store,
            id,
            offset_1,
            num_elements,
            &mapping_addition,
        )
        .await
        .map_err(|e| {
            LayerConversionError::new(
                id,
                InnerLayerConversionError::NodeValueIdmapConversionError(e),
            )
        })?;
        copy_unchanged_files(v10_store, v11_store, id).await?;
        eprintln!("files copied");
        rebuild_indexes(v11_store, id, is_child)
            .await
            .map_err(|e| {
                LayerConversionError::new(id, InnerLayerConversionError::RebuildIndexError(e))
            })?;
        eprintln!("indexes rebuilt");
        map = Some(mapping);
        offset = Some(new_offset);
    }

    storage_11::PersistentLayerStore::finalize(v11_store, id)
        .await
        .map_err(|e| {
            LayerConversionError::new(id, InnerLayerConversionError::FinalizationError(e))
        })?;
    eprintln!("finalized!");

    // we copy the rollup only after finalizing, as rollups are not
    // part of a layer under construction
    copy_rollup_file(v10_store, v11_store, id)
        .await
        .map_err(|e| {
            LayerConversionError::new(id, InnerLayerConversionError::RollupFileCopyError(e))
        })?;

    if !naive {
        write_parent_map(&work, id, map.unwrap(), offset.unwrap())
            .await
            .map_err(|e| {
                LayerConversionError::new(id, InnerLayerConversionError::ParentMapWriteError(e))
            })?;
        eprintln!("written parent map to workdir");
    }

    Ok(())
}

#[derive(Error, Debug)]
pub enum InnerParentMapError {
    #[error("not found")]
    ParentMapNotFound,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Deserialization(#[from] postcard::Error),
}

#[derive(Error, Debug)]
pub enum ParentMapError {
    #[error(transparent)]
    Io(io::Error),
    #[error("couldn't load parent map {}: {source}", name_to_string(*parent))]
    Other {
        parent: [u32; 5],
        source: InnerParentMapError,
    },
}

impl ParentMapError {
    fn new<E: Into<InnerParentMapError>>(parent: [u32; 5], source: E) -> Self {
        Self::Other {
            parent,
            source: source.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ParentMap {
    offset: u64,
    mapping: Vec<(u64, u64)>,
}

fn path_for_parent_map(workdir: &str, parent: [u32; 5]) -> PathBuf {
    let parent_string = name_to_string(parent);
    let prefix = &parent_string[..3];
    let mut pathbuf = PathBuf::from(workdir);
    pathbuf.push(prefix);
    pathbuf.push(format!("{parent_string}.postcard"));

    pathbuf
}

async fn get_mapping_and_offset_from_parent(
    workdir: &str,
    parent: [u32; 5],
) -> Result<(HashMap<u64, u64>, u64), ParentMapError> {
    let pathbuf = path_for_parent_map(workdir, parent);
    let file = tokio::fs::File::open(pathbuf).await;
    if file.is_err() && file.as_ref().unwrap_err().kind() == io::ErrorKind::NotFound {
        return Err(ParentMapError::new(
            parent,
            InnerParentMapError::ParentMapNotFound,
        ));
    }
    let mut file = file.map_err(|e| ParentMapError::new(parent, e))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .await
        .map_err(|e| ParentMapError::new(parent, e))?;
    let ParentMap {
        offset,
        mapping: mapping_vec,
    } = postcard::from_bytes(&bytes).map_err(|e| ParentMapError::new(parent, e))?;
    let mut mapping = HashMap::with_capacity(mapping_vec.len());
    mapping.extend(mapping_vec);

    Ok((mapping, offset))
}

async fn get_mapping_and_offset(
    workdir: &str,
    store: &directory_10::DirectoryLayerStore,
    id: [u32; 5],
) -> Result<(HashMap<u64, u64>, u64), ParentMapError> {
    // look up parent id if applicable
    if let Some(parent) = storage_10::LayerStore::get_layer_parent_name(store, id)
        .await
        .map_err(|e| ParentMapError::Io(e))?
    {
        get_mapping_and_offset_from_parent(workdir, parent).await
    } else {
        Ok((HashMap::with_capacity(0), 0))
    }
}

async fn naive_convert_dictionaries(
    v10_store: &directory_10::DirectoryLayerStore,
    v11_store: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
) -> io::Result<()> {
    let node_dict_pfc = storage_10::PersistentLayerStore::get_file(
        v10_store,
        id,
        V10_FILENAMES.node_dictionary_blocks,
    )
    .await?;
    let UntypedDictionaryOutput { offsets, data } =
        convert_untyped_dictionary(node_dict_pfc.clone()).await?;

    write_bytes_to_file(v11_store, id, V11_FILENAMES.node_dictionary_blocks, data);
    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.node_dictionary_offsets,
        offsets,
    );

    let predicate_dict_pfc = storage_10::PersistentLayerStore::get_file(
        v10_store,
        id,
        V10_FILENAMES.predicate_dictionary_blocks,
    )
    .await?;
    let UntypedDictionaryOutput { offsets, data } =
        convert_untyped_dictionary(predicate_dict_pfc).await?;

    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.predicate_dictionary_blocks,
        data,
    );
    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.predicate_dictionary_offsets,
        offsets,
    );

    let value_dict_pfc = storage_10::PersistentLayerStore::get_file(
        v10_store,
        id,
        V10_FILENAMES.value_dictionary_blocks,
    )
    .await?;
    let NaiveTypedDictionaryOutput {
        types_present,
        type_offsets,
        offsets,
        data,
    } = convert_naive_typed_dictionary(value_dict_pfc).await?;

    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_types_present,
        types_present,
    );
    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_type_offsets,
        type_offsets,
    );
    write_bytes_to_file(v11_store, id, V11_FILENAMES.value_dictionary_blocks, data);
    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_offsets,
        offsets,
    );

    Ok(())
}

async fn convert_dictionaries(
    v10_store: &directory_10::DirectoryLayerStore,
    v11_store: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    offset: u64,
) -> Result<(HashMap<u64, u64>, u64, u64), DictionaryConversionError> {
    let node_dict_pfc = storage_10::PersistentLayerStore::get_file(
        v10_store,
        id,
        V10_FILENAMES.node_dictionary_blocks,
    )
    .await?;
    let UntypedDictionaryOutput { offsets, data } =
        convert_untyped_dictionary(node_dict_pfc.clone()).await?;

    write_bytes_to_file(v11_store, id, V11_FILENAMES.node_dictionary_blocks, data);
    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.node_dictionary_offsets,
        offsets,
    );

    let predicate_dict_pfc = storage_10::PersistentLayerStore::get_file(
        v10_store,
        id,
        V10_FILENAMES.predicate_dictionary_blocks,
    )
    .await?;
    let UntypedDictionaryOutput { offsets, data } =
        convert_untyped_dictionary(predicate_dict_pfc).await?;

    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.predicate_dictionary_blocks,
        data,
    );
    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.predicate_dictionary_offsets,
        offsets,
    );

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
        offset: new_offset,
    } = convert_typed_dictionary(node_dict_pfc, value_dict_pfc, offset).await?;

    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_types_present,
        types_present,
    );
    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_type_offsets,
        type_offsets,
    );
    write_bytes_to_file(v11_store, id, V11_FILENAMES.value_dictionary_blocks, data);
    write_bytes_to_file(
        v11_store,
        id,
        V11_FILENAMES.value_dictionary_offsets,
        offsets,
    );

    Ok((mapping, new_offset, new_offset - offset))
}

async fn convert_triples(
    v10_store: &directory_10::DirectoryLayerStore,
    v11_store: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    is_child: bool,
    mapping: &HashMap<u64, u64>,
) -> io::Result<()> {
    if is_child {
        let pos_bits = storage_10::PersistentLayerStore::get_file(
            v10_store,
            id,
            V10_FILENAMES.pos_sp_o_adjacency_list_bits,
        )
        .await?;
        let pos_nums = storage_10::PersistentLayerStore::get_file(
            v10_store,
            id,
            V10_FILENAMES.pos_sp_o_adjacency_list_nums,
        )
        .await?;

        let output_nums = convert_sp_o_nums(pos_bits, pos_nums, mapping).await?;
        write_bytes_to_file(
            v11_store,
            id,
            V11_FILENAMES.pos_sp_o_adjacency_list_nums,
            output_nums,
        );

        let neg_bits = storage_10::PersistentLayerStore::get_file(
            v10_store,
            id,
            V10_FILENAMES.neg_sp_o_adjacency_list_bits,
        )
        .await?;
        let neg_nums = storage_10::PersistentLayerStore::get_file(
            v10_store,
            id,
            V10_FILENAMES.neg_sp_o_adjacency_list_nums,
        )
        .await?;

        let output_nums = convert_sp_o_nums(neg_bits, neg_nums, mapping).await?;
        write_bytes_to_file(
            v11_store,
            id,
            V11_FILENAMES.neg_sp_o_adjacency_list_nums,
            output_nums,
        );
    } else {
        let base_bits = storage_10::PersistentLayerStore::get_file(
            v10_store,
            id,
            V10_FILENAMES.base_sp_o_adjacency_list_bits,
        )
        .await?;
        let base_nums = storage_10::PersistentLayerStore::get_file(
            v10_store,
            id,
            V10_FILENAMES.base_sp_o_adjacency_list_nums,
        )
        .await?;

        let output_nums = convert_sp_o_nums(base_bits, base_nums, mapping).await?;
        write_bytes_to_file(
            v11_store,
            id,
            V11_FILENAMES.base_sp_o_adjacency_list_nums,
            output_nums,
        );
    }

    Ok(())
}

async fn copy_unchanged_files(
    from: &directory_10::DirectoryLayerStore,
    to: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
) -> Result<(), LayerConversionError> {
    for filename in UNCHANGED_FILES.iter() {
        copy_file(from, to, id, filename).await?;
    }

    Ok(())
}

async fn copy_naive_unchanged_files(
    from: &directory_10::DirectoryLayerStore,
    to: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    is_child: bool,
) -> Result<(), LayerConversionError> {
    let iter = if is_child {
        CHILD_NAIVE_UNCHANGED_FILES.iter()
    } else {
        BASE_NAIVE_UNCHANGED_FILES.iter()
    };
    for filename in iter {
        copy_file(from, to, id, filename).await?;
    }

    Ok(())
}

async fn rebuild_indexes(
    store: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    is_child: bool,
) -> io::Result<()> {
    let pos_objects_file = if is_child {
        Some(
            storage_11::PersistentLayerStore::get_file(store, id, V11_FILENAMES.pos_objects)
                .await?,
        )
    } else {
        None
    };

    let pos_sp_o_nums = storage_11::PersistentLayerStore::get_file(
        store,
        id,
        V11_FILENAMES.pos_sp_o_adjacency_list_nums,
    )
    .await?;
    let pos_sp_o_bits = storage_11::PersistentLayerStore::get_file(
        store,
        id,
        V11_FILENAMES.pos_sp_o_adjacency_list_bits,
    )
    .await?;
    let pos_sp_o_bit_index_blocks = storage_11::PersistentLayerStore::get_file(
        store,
        id,
        V11_FILENAMES.pos_sp_o_adjacency_list_bit_index_blocks,
    )
    .await?;
    let pos_sp_o_bit_index_sblocks = storage_11::PersistentLayerStore::get_file(
        store,
        id,
        V11_FILENAMES.pos_sp_o_adjacency_list_bit_index_sblocks,
    )
    .await?;

    let pos_sp_o_files = storage_11::AdjacencyListFiles {
        bitindex_files: storage_11::BitIndexFiles {
            bits_file: pos_sp_o_bits,
            blocks_file: pos_sp_o_bit_index_blocks,
            sblocks_file: pos_sp_o_bit_index_sblocks,
        },
        nums_file: pos_sp_o_nums,
    };

    let pos_o_ps_nums = storage_11::PersistentLayerStore::get_file(
        store,
        id,
        V11_FILENAMES.pos_o_ps_adjacency_list_nums,
    )
    .await?;
    let pos_o_ps_bits = storage_11::PersistentLayerStore::get_file(
        store,
        id,
        V11_FILENAMES.pos_o_ps_adjacency_list_bits,
    )
    .await?;
    let pos_o_ps_bit_index_blocks = storage_11::PersistentLayerStore::get_file(
        store,
        id,
        V11_FILENAMES.pos_o_ps_adjacency_list_bit_index_blocks,
    )
    .await?;
    let pos_o_ps_bit_index_sblocks = storage_11::PersistentLayerStore::get_file(
        store,
        id,
        V11_FILENAMES.pos_o_ps_adjacency_list_bit_index_sblocks,
    )
    .await?;

    let pos_o_ps_files = storage_11::AdjacencyListFiles {
        bitindex_files: storage_11::BitIndexFiles {
            bits_file: pos_o_ps_bits,
            blocks_file: pos_o_ps_bit_index_blocks,
            sblocks_file: pos_o_ps_bit_index_sblocks,
        },
        nums_file: pos_o_ps_nums,
    };

    builder_11::build_object_index(pos_sp_o_files, pos_o_ps_files, pos_objects_file).await?;

    if is_child {
        let neg_objects_file = Some(
            storage_11::PersistentLayerStore::get_file(store, id, V11_FILENAMES.neg_objects)
                .await?,
        );

        let neg_sp_o_nums = storage_11::PersistentLayerStore::get_file(
            store,
            id,
            V11_FILENAMES.neg_sp_o_adjacency_list_nums,
        )
        .await?;
        let neg_sp_o_bits = storage_11::PersistentLayerStore::get_file(
            store,
            id,
            V11_FILENAMES.neg_sp_o_adjacency_list_bits,
        )
        .await?;
        let neg_sp_o_bit_index_blocks = storage_11::PersistentLayerStore::get_file(
            store,
            id,
            V11_FILENAMES.neg_sp_o_adjacency_list_bit_index_blocks,
        )
        .await?;
        let neg_sp_o_bit_index_sblocks = storage_11::PersistentLayerStore::get_file(
            store,
            id,
            V11_FILENAMES.neg_sp_o_adjacency_list_bit_index_sblocks,
        )
        .await?;

        let neg_sp_o_files = storage_11::AdjacencyListFiles {
            bitindex_files: storage_11::BitIndexFiles {
                bits_file: neg_sp_o_bits,
                blocks_file: neg_sp_o_bit_index_blocks,
                sblocks_file: neg_sp_o_bit_index_sblocks,
            },
            nums_file: neg_sp_o_nums,
        };

        let neg_o_ps_nums = storage_11::PersistentLayerStore::get_file(
            store,
            id,
            V11_FILENAMES.neg_o_ps_adjacency_list_nums,
        )
        .await?;
        let neg_o_ps_bits = storage_11::PersistentLayerStore::get_file(
            store,
            id,
            V11_FILENAMES.neg_o_ps_adjacency_list_bits,
        )
        .await?;
        let neg_o_ps_bit_index_blocks = storage_11::PersistentLayerStore::get_file(
            store,
            id,
            V11_FILENAMES.neg_o_ps_adjacency_list_bit_index_blocks,
        )
        .await?;
        let neg_o_ps_bit_index_sblocks = storage_11::PersistentLayerStore::get_file(
            store,
            id,
            V11_FILENAMES.neg_o_ps_adjacency_list_bit_index_sblocks,
        )
        .await?;

        let neg_o_ps_files = storage_11::AdjacencyListFiles {
            bitindex_files: storage_11::BitIndexFiles {
                bits_file: neg_o_ps_bits,
                blocks_file: neg_o_ps_bit_index_blocks,
                sblocks_file: neg_o_ps_bit_index_sblocks,
            },
            nums_file: neg_o_ps_nums,
        };

        builder_11::build_object_index(neg_sp_o_files, neg_o_ps_files, neg_objects_file).await?;
    }

    Ok(())
}

async fn write_parent_map(
    workdir: &str,
    id: [u32; 5],
    mapping: HashMap<u64, u64>,
    offset: u64,
) -> io::Result<()> {
    let pathbuf = path_for_parent_map(workdir, id);
    tokio::fs::create_dir_all(pathbuf.parent().unwrap()).await?;

    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).write(true);

    let mut file = options.open(pathbuf).await?;

    let mut map_vec: Vec<_> = mapping.into_iter().collect();
    map_vec.sort();

    let parent_map = ParentMap {
        mapping: map_vec,
        offset,
    };

    let v = postcard::to_allocvec(&parent_map).unwrap();
    file.write_all(&v).await?;
    file.flush().await
}

fn write_bytes_to_file(
    store: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    file: &str,
    bytes: Bytes,
) {
    let file = consts_11::FILENAME_ENUM_MAP[file];
    store.write_bytes(id, file, bytes);
}

async fn copy_file(
    from: &directory_10::DirectoryLayerStore,
    to: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    file: &str,
) -> Result<(), LayerConversionError> {
    inner_copy_file(from, to, id, file).await.map_err(|e| {
        LayerConversionError::new(
            id,
            InnerLayerConversionError::FileCopyError {
                name: file.to_string(),
                source: e,
            },
        )
    })
}
async fn inner_copy_file(
    from: &directory_10::DirectoryLayerStore,
    to: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    file: &str,
) -> io::Result<()> {
    // this assumes that the file name is the same in from and to,
    // which should be correct for everythning that is not a
    // dictionary. At this point, we've already copied over the
    // dictionaries.
    let input = storage_10::PersistentLayerStore::get_file(from, id, file).await?;
    if let Some(map) = storage_10::FileLoad::map_if_exists(&input).await? {
        write_bytes_to_file(to, id, file, map);
    }

    Ok(())
}

async fn copy_rollup_file(
    from: &directory_10::DirectoryLayerStore,
    to: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
) -> io::Result<()> {
    let input = storage_10::PersistentLayerStore::get_file(from, id, V10_FILENAMES.rollup).await?;
    if let Some(map) = storage_10::FileLoad::map_if_exists(&input).await? {
        let output =
            storage_11::PersistentLayerStore::get_file(to, id, V11_FILENAMES.rollup).await?;
        let mut output_writer = storage_11::FileStore::open_write(&output).await?;
        output_writer.write_all(&map).await?;
        output_writer.flush().await?;
        storage_11::SyncableFile::sync_all(output_writer).await?;
        eprintln!("copied rollup file");
    }

    Ok(())
}

async fn convert_node_value_idmap(
    from: &directory_10::DirectoryLayerStore,
    to: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    offset: u64,
    num_entries: u64,
    mapping: &HashMap<u64, u64>,
) -> io::Result<()> {
    if storage_10::PersistentLayerStore::file_exists(
        from,
        id,
        consts_11::FILENAMES.node_value_idmap_bits,
    )
    .await?
    {
        // we have a conversion task to do
        let bits_file = storage_10::PersistentLayerStore::get_file(
            from,
            id,
            consts_11::FILENAMES.node_value_idmap_bits,
        )
        .await?;
        let blocks_file = storage_10::PersistentLayerStore::get_file(
            from,
            id,
            consts_11::FILENAMES.node_value_idmap_bit_index_blocks,
        )
        .await?;
        let sblocks_file = storage_10::PersistentLayerStore::get_file(
            from,
            id,
            consts_11::FILENAMES.node_value_idmap_bit_index_sblocks,
        )
        .await?;

        let bitindex = structure_10::BitIndex::from_maps(
            storage_10::FileLoad::map(&bits_file).await?,
            storage_10::FileLoad::map(&blocks_file).await?,
            storage_10::FileLoad::map(&sblocks_file).await?,
        );
        let width = util_10::calculate_width(num_entries);
        let wtree = structure_10::WaveletTree::from_parts(bitindex, width);

        let iter = wtree.decode().map(|elt| {
            let num = elt + offset + 1;
            mapping.get(&num).map(|n| *n - offset - 1).unwrap_or(elt)
        });

        let output_bits_file = storage_11::PersistentLayerStore::get_file(
            to,
            id,
            consts_11::FILENAMES.node_value_idmap_bits,
        )
        .await?;
        let output_blocks_file = storage_11::PersistentLayerStore::get_file(
            to,
            id,
            consts_11::FILENAMES.node_value_idmap_bit_index_blocks,
        )
        .await?;
        let output_sblocks_file = storage_11::PersistentLayerStore::get_file(
            to,
            id,
            consts_11::FILENAMES.node_value_idmap_bit_index_sblocks,
        )
        .await?;

        structure_11::build_wavelet_tree_from_iter(
            width,
            iter,
            output_bits_file,
            output_blocks_file,
            output_sblocks_file,
        )
        .await?;
        eprintln!("node value map transformed");
    }
    Ok(())
}
