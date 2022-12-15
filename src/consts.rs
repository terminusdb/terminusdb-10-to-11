pub struct Filenames {
    pub node_dictionary_blocks: &'static str,
    pub node_dictionary_offsets: &'static str,

    pub predicate_dictionary_blocks: &'static str,
    pub predicate_dictionary_offsets: &'static str,

    pub value_dictionary_types_present: &'static str,
    pub value_dictionary_type_offsets: &'static str,
    pub value_dictionary_blocks: &'static str,
    pub value_dictionary_offsets: &'static str,

    pub node_value_idmap_bits: &'static str,
    pub node_value_idmap_bit_index_blocks: &'static str,
    pub node_value_idmap_bit_index_sblocks: &'static str,

    pub predicate_idmap_bits: &'static str,
    pub predicate_idmap_bit_index_blocks: &'static str,
    pub predicate_idmap_bit_index_sblocks: &'static str,

    pub base_subjects: &'static str,
    pub base_objects: &'static str,

    pub base_s_p_adjacency_list_nums: &'static str,
    pub base_s_p_adjacency_list_bits: &'static str,
    pub base_s_p_adjacency_list_bit_index_blocks: &'static str,
    pub base_s_p_adjacency_list_bit_index_sblocks: &'static str,

    pub base_sp_o_adjacency_list_nums: &'static str,
    pub base_sp_o_adjacency_list_bits: &'static str,
    pub base_sp_o_adjacency_list_bit_index_blocks: &'static str,
    pub base_sp_o_adjacency_list_bit_index_sblocks: &'static str,

    pub base_o_ps_adjacency_list_nums: &'static str,
    pub base_o_ps_adjacency_list_bits: &'static str,
    pub base_o_ps_adjacency_list_bit_index_blocks: &'static str,
    pub base_o_ps_adjacency_list_bit_index_sblocks: &'static str,

    pub pos_subjects: &'static str,
    pub pos_objects: &'static str,

    pub pos_s_p_adjacency_list_nums: &'static str,
    pub pos_s_p_adjacency_list_bits: &'static str,
    pub pos_s_p_adjacency_list_bit_index_blocks: &'static str,
    pub pos_s_p_adjacency_list_bit_index_sblocks: &'static str,

    pub pos_sp_o_adjacency_list_nums: &'static str,
    pub pos_sp_o_adjacency_list_bits: &'static str,
    pub pos_sp_o_adjacency_list_bit_index_blocks: &'static str,
    pub pos_sp_o_adjacency_list_bit_index_sblocks: &'static str,

    pub pos_o_ps_adjacency_list_nums: &'static str,
    pub pos_o_ps_adjacency_list_bits: &'static str,
    pub pos_o_ps_adjacency_list_bit_index_blocks: &'static str,
    pub pos_o_ps_adjacency_list_bit_index_sblocks: &'static str,

    pub neg_subjects: &'static str,
    pub neg_objects: &'static str,

    pub neg_s_p_adjacency_list_nums: &'static str,
    pub neg_s_p_adjacency_list_bits: &'static str,
    pub neg_s_p_adjacency_list_bit_index_blocks: &'static str,
    pub neg_s_p_adjacency_list_bit_index_sblocks: &'static str,

    pub neg_sp_o_adjacency_list_nums: &'static str,
    pub neg_sp_o_adjacency_list_bits: &'static str,
    pub neg_sp_o_adjacency_list_bit_index_blocks: &'static str,
    pub neg_sp_o_adjacency_list_bit_index_sblocks: &'static str,

    pub neg_o_ps_adjacency_list_nums: &'static str,
    pub neg_o_ps_adjacency_list_bits: &'static str,
    pub neg_o_ps_adjacency_list_bit_index_blocks: &'static str,
    pub neg_o_ps_adjacency_list_bit_index_sblocks: &'static str,

    pub base_predicate_wavelet_tree_bits: &'static str,
    pub base_predicate_wavelet_tree_bit_index_blocks: &'static str,
    pub base_predicate_wavelet_tree_bit_index_sblocks: &'static str,

    pub pos_predicate_wavelet_tree_bits: &'static str,
    pub pos_predicate_wavelet_tree_bit_index_blocks: &'static str,
    pub pos_predicate_wavelet_tree_bit_index_sblocks: &'static str,

    pub neg_predicate_wavelet_tree_bits: &'static str,
    pub neg_predicate_wavelet_tree_bit_index_blocks: &'static str,
    pub neg_predicate_wavelet_tree_bit_index_sblocks: &'static str,

    pub parent: &'static str,
    pub rollup: &'static str,
}

pub const V10_FILENAMES: Filenames = Filenames {
    node_dictionary_blocks: "node_dictionary_blocks.pfc",
    node_dictionary_offsets: "node_dictionary_offsets.logarray",

    predicate_dictionary_blocks: "predicate_dictionary_blocks.pfc",
    predicate_dictionary_offsets: "predicate_dictionary_offsets.logarray",

    value_dictionary_types_present: "value_dictionary_types.logarray",
    value_dictionary_type_offsets: "value_dictionary_type_offsets.logarray",
    value_dictionary_blocks: "value_dictionary_blocks.pfc",
    value_dictionary_offsets: "value_dictionary_offsets.logarray",

    node_value_idmap_bits: "node_value_idmap_bits.bitarray",
    node_value_idmap_bit_index_blocks: "node_value_idmap_bit_index_blocks.bitarray",
    node_value_idmap_bit_index_sblocks: "node_value_idmap_bit_index_sblocks.bitarray",

    predicate_idmap_bits: "predicate_idmap_bits.bitarray",
    predicate_idmap_bit_index_blocks: "predicate_idmap_bit_index_blocks.bitarray",
    predicate_idmap_bit_index_sblocks: "predicate_idmap_bit_index_sblocks.bitarray",

    base_subjects: "base_subjects.logarray",
    base_objects: "base_objects.logarray",

    base_s_p_adjacency_list_nums: "base_s_p_adjacency_list_nums.logarray",
    base_s_p_adjacency_list_bits: "base_s_p_adjacency_list_bits.bitarray",
    base_s_p_adjacency_list_bit_index_blocks: "base_s_p_adjacency_list_bit_index_blocks.logarray",
    base_s_p_adjacency_list_bit_index_sblocks: "base_s_p_adjacency_list_bit_index_sblocks.logarray",

    base_sp_o_adjacency_list_nums: "base_sp_o_adjacency_list_nums.logarray",
    base_sp_o_adjacency_list_bits: "base_sp_o_adjacency_list_bits.bitarray",
    base_sp_o_adjacency_list_bit_index_blocks: "base_sp_o_adjacency_list_bit_index_blocks.logarray",
    base_sp_o_adjacency_list_bit_index_sblocks:
        "base_sp_o_adjacency_list_bit_index_sblocks.logarray",

    base_o_ps_adjacency_list_nums: "base_o_ps_adjacency_list_nums.logarray",
    base_o_ps_adjacency_list_bits: "base_o_ps_adjacency_list_bits.bitarray",
    base_o_ps_adjacency_list_bit_index_blocks: "base_o_ps_adjacency_list_bit_index_blocks.logarray",
    base_o_ps_adjacency_list_bit_index_sblocks:
        "base_o_ps_adjacency_list_bit_index_sblocks.logarray",

    pos_subjects: "child_pos_subjects.logarray",
    pos_objects: "child_pos_objects.logarray",

    pos_s_p_adjacency_list_nums: "pos_s_p_adjacency_list_nums.logarray",
    pos_s_p_adjacency_list_bits: "pos_s_p_adjacency_list_bits.bitarray",
    pos_s_p_adjacency_list_bit_index_blocks: "pos_s_p_adjacency_list_bit_index_blocks.logarray",
    pos_s_p_adjacency_list_bit_index_sblocks: "pos_s_p_adjacency_list_bit_index_sblocks.logarray",

    pos_sp_o_adjacency_list_nums: "pos_sp_o_adjacency_list_nums.logarray",
    pos_sp_o_adjacency_list_bits: "pos_sp_o_adjacency_list_bits.bitarray",
    pos_sp_o_adjacency_list_bit_index_blocks: "pos_sp_o_adjacency_list_bit_index_blocks.logarray",
    pos_sp_o_adjacency_list_bit_index_sblocks: "pos_sp_o_adjacency_list_bit_index_sblocks.logarray",

    pos_o_ps_adjacency_list_nums: "pos_o_ps_adjacency_list_nums.logarray",
    pos_o_ps_adjacency_list_bits: "pos_o_ps_adjacency_list_bits.bitarray",
    pos_o_ps_adjacency_list_bit_index_blocks: "pos_o_ps_adjacency_list_bit_index_blocks.logarray",
    pos_o_ps_adjacency_list_bit_index_sblocks: "pos_o_ps_adjacency_list_bit_index_sblocks.logarray",

    neg_subjects: "child_neg_subjects.logarray",
    neg_objects: "child_neg_objects.logarray",

    neg_s_p_adjacency_list_nums: "neg_s_p_adjacency_list_nums.logarray",
    neg_s_p_adjacency_list_bits: "neg_s_p_adjacency_list_bits.bitarray",
    neg_s_p_adjacency_list_bit_index_blocks: "neg_s_p_adjacency_list_bit_index_blocks.logarray",
    neg_s_p_adjacency_list_bit_index_sblocks: "neg_s_p_adjacency_list_bit_index_sblocks.logarray",

    neg_sp_o_adjacency_list_nums: "neg_sp_o_adjacency_list_nums.logarray",
    neg_sp_o_adjacency_list_bits: "neg_sp_o_adjacency_list_bits.bitarray",
    neg_sp_o_adjacency_list_bit_index_blocks: "neg_sp_o_adjacency_list_bit_index_blocks.logarray",
    neg_sp_o_adjacency_list_bit_index_sblocks: "neg_sp_o_adjacency_list_bit_index_sblocks.logarray",

    neg_o_ps_adjacency_list_nums: "neg_o_ps_adjacency_list_nums.logarray",
    neg_o_ps_adjacency_list_bits: "neg_o_ps_adjacency_list_bits.bitarray",
    neg_o_ps_adjacency_list_bit_index_blocks: "neg_o_ps_adjacency_list_bit_index_blocks.logarray",
    neg_o_ps_adjacency_list_bit_index_sblocks: "neg_o_ps_adjacency_list_bit_index_sblocks.logarray",

    base_predicate_wavelet_tree_bits: "base_predicate_wavelet_tree_bits.bitarray",
    base_predicate_wavelet_tree_bit_index_blocks:
        "base_predicate_wavelet_tree_bit_index_blocks.logarray",
    base_predicate_wavelet_tree_bit_index_sblocks:
        "base_predicate_wavelet_tree_bit_index_sblocks.logarray",

    pos_predicate_wavelet_tree_bits: "pos_predicate_wavelet_tree_bits.bitarray",
    pos_predicate_wavelet_tree_bit_index_blocks:
        "pos_predicate_wavelet_tree_bit_index_blocks.logarray",
    pos_predicate_wavelet_tree_bit_index_sblocks:
        "pos_predicate_wavelet_tree_bit_index_sblocks.logarray",

    neg_predicate_wavelet_tree_bits: "neg_predicate_wavelet_tree_bits.bitarray",
    neg_predicate_wavelet_tree_bit_index_blocks:
        "neg_predicate_wavelet_tree_bit_index_blocks.logarray",
    neg_predicate_wavelet_tree_bit_index_sblocks:
        "neg_predicate_wavelet_tree_bit_index_sblocks.logarray",

    parent: "parent.hex",
    rollup: "rollup.hex",
};

pub const V11_FILENAMES: Filenames = Filenames {
    node_dictionary_blocks: "node_dictionary_blocks.tfc",
    node_dictionary_offsets: "node_dictionary_offsets.logarray",

    predicate_dictionary_blocks: "predicate_dictionary_blocks.tfc",
    predicate_dictionary_offsets: "predicate_dictionary_offsets.logarray",

    value_dictionary_types_present: "value_dictionary_types.logarray",
    value_dictionary_type_offsets: "value_dictionary_type_offsets.logarray",
    value_dictionary_blocks: "value_dictionary_blocks.tfc",
    value_dictionary_offsets: "value_dictionary_offsets.logarray",

    node_value_idmap_bits: "node_value_idmap_bits.bitarray",
    node_value_idmap_bit_index_blocks: "node_value_idmap_bit_index_blocks.bitarray",
    node_value_idmap_bit_index_sblocks: "node_value_idmap_bit_index_sblocks.bitarray",

    predicate_idmap_bits: "predicate_idmap_bits.bitarray",
    predicate_idmap_bit_index_blocks: "predicate_idmap_bit_index_blocks.bitarray",
    predicate_idmap_bit_index_sblocks: "predicate_idmap_bit_index_sblocks.bitarray",

    base_subjects: "base_subjects.logarray",
    base_objects: "base_objects.logarray",

    base_s_p_adjacency_list_nums: "base_s_p_adjacency_list_nums.logarray",
    base_s_p_adjacency_list_bits: "base_s_p_adjacency_list_bits.bitarray",
    base_s_p_adjacency_list_bit_index_blocks: "base_s_p_adjacency_list_bit_index_blocks.logarray",
    base_s_p_adjacency_list_bit_index_sblocks: "base_s_p_adjacency_list_bit_index_sblocks.logarray",

    base_sp_o_adjacency_list_nums: "base_sp_o_adjacency_list_nums.logarray",
    base_sp_o_adjacency_list_bits: "base_sp_o_adjacency_list_bits.bitarray",
    base_sp_o_adjacency_list_bit_index_blocks: "base_sp_o_adjacency_list_bit_index_blocks.logarray",
    base_sp_o_adjacency_list_bit_index_sblocks:
        "base_sp_o_adjacency_list_bit_index_sblocks.logarray",

    base_o_ps_adjacency_list_nums: "base_o_ps_adjacency_list_nums.logarray",
    base_o_ps_adjacency_list_bits: "base_o_ps_adjacency_list_bits.bitarray",
    base_o_ps_adjacency_list_bit_index_blocks: "base_o_ps_adjacency_list_bit_index_blocks.logarray",
    base_o_ps_adjacency_list_bit_index_sblocks:
        "base_o_ps_adjacency_list_bit_index_sblocks.logarray",

    pos_subjects: "child_pos_subjects.logarray",
    pos_objects: "child_pos_objects.logarray",

    pos_s_p_adjacency_list_nums: "pos_s_p_adjacency_list_nums.logarray",
    pos_s_p_adjacency_list_bits: "pos_s_p_adjacency_list_bits.bitarray",
    pos_s_p_adjacency_list_bit_index_blocks: "pos_s_p_adjacency_list_bit_index_blocks.logarray",
    pos_s_p_adjacency_list_bit_index_sblocks: "pos_s_p_adjacency_list_bit_index_sblocks.logarray",

    pos_sp_o_adjacency_list_nums: "pos_sp_o_adjacency_list_nums.logarray",
    pos_sp_o_adjacency_list_bits: "pos_sp_o_adjacency_list_bits.bitarray",
    pos_sp_o_adjacency_list_bit_index_blocks: "pos_sp_o_adjacency_list_bit_index_blocks.logarray",
    pos_sp_o_adjacency_list_bit_index_sblocks: "pos_sp_o_adjacency_list_bit_index_sblocks.logarray",

    pos_o_ps_adjacency_list_nums: "pos_o_ps_adjacency_list_nums.logarray",
    pos_o_ps_adjacency_list_bits: "pos_o_ps_adjacency_list_bits.bitarray",
    pos_o_ps_adjacency_list_bit_index_blocks: "pos_o_ps_adjacency_list_bit_index_blocks.logarray",
    pos_o_ps_adjacency_list_bit_index_sblocks: "pos_o_ps_adjacency_list_bit_index_sblocks.logarray",

    neg_subjects: "child_neg_subjects.logarray",
    neg_objects: "child_neg_objects.logarray",

    neg_s_p_adjacency_list_nums: "neg_s_p_adjacency_list_nums.logarray",
    neg_s_p_adjacency_list_bits: "neg_s_p_adjacency_list_bits.bitarray",
    neg_s_p_adjacency_list_bit_index_blocks: "neg_s_p_adjacency_list_bit_index_blocks.logarray",
    neg_s_p_adjacency_list_bit_index_sblocks: "neg_s_p_adjacency_list_bit_index_sblocks.logarray",

    neg_sp_o_adjacency_list_nums: "neg_sp_o_adjacency_list_nums.logarray",
    neg_sp_o_adjacency_list_bits: "neg_sp_o_adjacency_list_bits.bitarray",
    neg_sp_o_adjacency_list_bit_index_blocks: "neg_sp_o_adjacency_list_bit_index_blocks.logarray",
    neg_sp_o_adjacency_list_bit_index_sblocks: "neg_sp_o_adjacency_list_bit_index_sblocks.logarray",

    neg_o_ps_adjacency_list_nums: "neg_o_ps_adjacency_list_nums.logarray",
    neg_o_ps_adjacency_list_bits: "neg_o_ps_adjacency_list_bits.bitarray",
    neg_o_ps_adjacency_list_bit_index_blocks: "neg_o_ps_adjacency_list_bit_index_blocks.logarray",
    neg_o_ps_adjacency_list_bit_index_sblocks: "neg_o_ps_adjacency_list_bit_index_sblocks.logarray",

    base_predicate_wavelet_tree_bits: "base_predicate_wavelet_tree_bits.bitarray",
    base_predicate_wavelet_tree_bit_index_blocks:
        "base_predicate_wavelet_tree_bit_index_blocks.logarray",
    base_predicate_wavelet_tree_bit_index_sblocks:
        "base_predicate_wavelet_tree_bit_index_sblocks.logarray",

    pos_predicate_wavelet_tree_bits: "pos_predicate_wavelet_tree_bits.bitarray",
    pos_predicate_wavelet_tree_bit_index_blocks:
        "pos_predicate_wavelet_tree_bit_index_blocks.logarray",
    pos_predicate_wavelet_tree_bit_index_sblocks:
        "pos_predicate_wavelet_tree_bit_index_sblocks.logarray",

    neg_predicate_wavelet_tree_bits: "neg_predicate_wavelet_tree_bits.bitarray",
    neg_predicate_wavelet_tree_bit_index_blocks:
        "neg_predicate_wavelet_tree_bit_index_blocks.logarray",
    neg_predicate_wavelet_tree_bit_index_sblocks:
        "neg_predicate_wavelet_tree_bit_index_sblocks.logarray",

    parent: "parent.hex",
    rollup: "rollup.hex",
};

pub const UNCHANGED_FILES: [&'static str; 39] = [
    V10_FILENAMES.predicate_idmap_bits,
    V10_FILENAMES.predicate_idmap_bit_index_blocks,
    V10_FILENAMES.predicate_idmap_bit_index_sblocks,
    V10_FILENAMES.rollup,

    V10_FILENAMES.base_s_p_adjacency_list_nums,
    V10_FILENAMES.base_s_p_adjacency_list_bits,
    V10_FILENAMES.base_s_p_adjacency_list_bit_index_blocks,
    V10_FILENAMES.base_s_p_adjacency_list_bit_index_sblocks,
    V10_FILENAMES.base_sp_o_adjacency_list_bits,
    V10_FILENAMES.base_sp_o_adjacency_list_bit_index_blocks,
    V10_FILENAMES.base_sp_o_adjacency_list_bit_index_sblocks,
    V10_FILENAMES.base_predicate_wavelet_tree_bits,
    V10_FILENAMES.base_predicate_wavelet_tree_bit_index_blocks,
    V10_FILENAMES.base_predicate_wavelet_tree_bit_index_sblocks,

    V10_FILENAMES.base_subjects,
    V10_FILENAMES.base_objects,

    V10_FILENAMES.parent,
    V10_FILENAMES.pos_s_p_adjacency_list_nums,
    V10_FILENAMES.pos_s_p_adjacency_list_bits,
    V10_FILENAMES.pos_s_p_adjacency_list_bit_index_blocks,
    V10_FILENAMES.pos_s_p_adjacency_list_bit_index_sblocks,
    V10_FILENAMES.pos_sp_o_adjacency_list_bits,
    V10_FILENAMES.pos_sp_o_adjacency_list_bit_index_blocks,
    V10_FILENAMES.pos_sp_o_adjacency_list_bit_index_sblocks,
    V10_FILENAMES.pos_predicate_wavelet_tree_bits,
    V10_FILENAMES.pos_predicate_wavelet_tree_bit_index_blocks,
    V10_FILENAMES.pos_predicate_wavelet_tree_bit_index_sblocks,
    V10_FILENAMES.neg_s_p_adjacency_list_nums,
    V10_FILENAMES.neg_s_p_adjacency_list_bits,
    V10_FILENAMES.neg_s_p_adjacency_list_bit_index_blocks,
    V10_FILENAMES.neg_s_p_adjacency_list_bit_index_sblocks,
    V10_FILENAMES.neg_sp_o_adjacency_list_bits,
    V10_FILENAMES.neg_sp_o_adjacency_list_bit_index_blocks,
    V10_FILENAMES.neg_sp_o_adjacency_list_bit_index_sblocks,
    V10_FILENAMES.neg_predicate_wavelet_tree_bits,
    V10_FILENAMES.neg_predicate_wavelet_tree_bit_index_blocks,
    V10_FILENAMES.neg_predicate_wavelet_tree_bit_index_sblocks,

    V10_FILENAMES.pos_subjects,
    V10_FILENAMES.neg_subjects,
];
