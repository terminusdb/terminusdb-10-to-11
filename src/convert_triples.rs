use std::collections::HashMap;
use terminus_store_11::storage::directory::FileBackedStore;
use terminus_store_11::structure::{
    bitarray_stream_bits, logarray_file_get_length_and_width, logarray_stream_entries,
    LogArrayBufBuilder,
};

use bytes::{Bytes, BytesMut};
use futures::stream::TryStreamExt;
use tokio::fs::File;

use std::io;

pub async fn convert_sp_o_nums(
    bits_path: &str,
    nums_path: &str,
    mapping: HashMap<u64, u64>,
) -> io::Result<Bytes> {
    let bits_file = FileBackedStore::new(bits_path);
    let nums_file = FileBackedStore::new(nums_path);

    let (_len, width) = logarray_file_get_length_and_width(nums_file.clone()).await?;
    let mut bits_stream = bitarray_stream_bits(bits_file).await?;
    let mut nums_stream = logarray_stream_entries(nums_file).await?;

    let mut buf = BytesMut::new();
    let mut builder = LogArrayBufBuilder::new(&mut buf, width);

    let mut tally = 0;
    while let Some(b) = bits_stream.try_next().await? {
        tally += 1;
        if b {
            // we hit a boundary, read just as many nums and that is our group slice
            let mut v = Vec::with_capacity(tally);
            for _ in 0..tally {
                v.push(mapping[&nums_stream.try_next().await?.unwrap()]);
            }
            v.sort();

            builder.push_vec(v);
        }
    }

    builder.finalize();

    Ok(buf.freeze())
}
