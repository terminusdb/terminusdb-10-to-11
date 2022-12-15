use terminus_store_10::storage as storage_10;
use terminus_store_10::storage::directory as directory_10;
use terminus_store_11::layer::builder as builder_11;
use terminus_store_11::storage as storage_11;
use terminus_store_11::storage::archive as archive_11;
use terminus_store_11::storage::consts as consts_11;
use terminus_store_11::storage::{name_to_string, string_to_name};

use crate::consts::*;
use crate::convert_dict::*;
use crate::convert_triples::*;

use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::path::PathBuf;

use bytes::Bytes;

use serde::{Deserialize, Serialize};

pub async fn convert_layer(from: &str, to: &str, work: &str, id: &str) -> io::Result<()> {
    let id = string_to_name(id).unwrap();
    let v10_store = directory_10::DirectoryLayerStore::new(from);
    let is_child = storage_10::PersistentLayerStore::layer_has_parent(&v10_store, id).await?;
    let v11_store = archive_11::ArchiveLayerStore::new(to);

    eprintln!("initial setup done");
    let (mut mapping, offset) = get_mapping_and_offset(work, &v10_store, id).await?;

    eprintln!("parent mappings retrieved");

    storage_11::PersistentLayerStore::create_named_directory(&v11_store, id).await?;

    let (mapping_addition, offset) =
        convert_dictionaries(&v10_store, &v11_store, id, offset).await?;
    mapping.extend(mapping_addition);
    eprintln!("dictionaries converted");
    convert_triples(&v10_store, &v11_store, id, is_child, &mapping).await?;
    eprintln!("triples converted");
    copy_unchanged_files(&v10_store, &v11_store, id).await?;
    eprintln!("files copied");

    rebuild_indexes(&v11_store, id, is_child).await?;
    eprintln!("indexes rebuilt");

    storage_11::PersistentLayerStore::finalize(&v11_store, id).await?;
    eprintln!("finalized!");

    write_parent_map(&work, id, mapping, offset)?;
    eprintln!("written parent map to workdir");

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct ParentMap {
    offset: u64,
    mapping: Vec<(u64, u64)>,
}

async fn get_mapping_and_offset(
    workdir: &str,
    store: &directory_10::DirectoryLayerStore,
    id: [u32; 5],
) -> io::Result<(HashMap<u64, u64>, u64)> {
    // look up parent id if applicable
    if let Some(parent) = storage_10::LayerStore::get_layer_parent_name(store, id).await? {
        let parent_string = name_to_string(parent);
        let mut pathbuf = PathBuf::from(workdir);
        pathbuf.push(format!("{parent_string}.json"));
        let file = std::fs::File::open(pathbuf);
        if file.is_err() && file.as_ref().unwrap_err().kind() == io::ErrorKind::NotFound {
            let id_string = name_to_string(id);
            panic!("couldn't find parent map for {parent_string} while converting {id_string}");
        }
        let mut file = file?;
        let ParentMap {
            offset,
            mapping: mapping_vec,
        } = serde_json::from_reader(&mut file)?;
        let mut mapping = HashMap::with_capacity(mapping_vec.len());
        mapping.extend(mapping_vec);

        Ok((mapping, offset))
    } else {
        Ok((HashMap::with_capacity(0), 0))
    }
}

async fn convert_dictionaries(
    v10_store: &directory_10::DirectoryLayerStore,
    v11_store: &archive_11::ArchiveLayerStore,
    id: [u32; 5],
    offset: u64,
) -> io::Result<(HashMap<u64, u64>, u64)> {
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
        offset,
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

    Ok((mapping, offset))
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
) -> io::Result<()> {
    for filename in UNCHANGED_FILES.iter() {
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

fn write_parent_map(
    workdir: &str,
    id: [u32; 5],
    mapping: HashMap<u64, u64>,
    offset: u64,
) -> io::Result<()> {
    let id_string = name_to_string(id);
    let mut pathbuf = PathBuf::from(workdir);
    std::fs::create_dir_all(&pathbuf)?;
    pathbuf.push(format!("{id_string}.json"));

    let mut options = std::fs::OpenOptions::new();
    options.create(true).write(true);

    let mut file = options.open(pathbuf)?;

    let mut map_vec: Vec<_> = mapping.into_iter().collect();
    map_vec.sort();

    let parent_map = ParentMap {
        mapping: map_vec,
        offset,
    };

    serde_json::to_writer(&mut file, &parent_map)?;
    file.flush()
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
