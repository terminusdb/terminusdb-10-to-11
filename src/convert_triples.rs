use std::collections::HashMap;
use terminus_store_10::storage as storage_10;
use terminus_store_10::structure as structure_10;
use terminus_store_11::structure::LogArrayBufBuilder;

use bytes::{Bytes, BytesMut};
use futures::stream::TryStreamExt;

use std::io;

pub async fn convert_sp_o_nums<F: storage_10::FileLoad + 'static>(
    bits: F,
    nums: F,
    mapping: &HashMap<u64, u64>,
) -> io::Result<Bytes> {
    let (_len, width) = structure_10::logarray_file_get_length_and_width(nums.clone()).await?;
    let mut bits_stream = structure_10::bitarray_stream_bits(bits).await?;
    let mut nums_stream = structure_10::logarray_stream_entries(nums).await?;

    let mut buf = BytesMut::new();
    let mut builder = LogArrayBufBuilder::new(&mut buf, width);

    let mut tally = 0;
    while let Some(b) = bits_stream.try_next().await? {
        tally += 1;
        if b {
            // we hit a boundary, read just as many nums and that is our group slice
            let mut v = Vec::with_capacity(tally);
            for _ in 0..tally {
                let unmapped = nums_stream.try_next().await?.unwrap();
                let mapped = mapping.get(&unmapped).cloned().unwrap_or(unmapped);
                v.push(mapped);
            }
            v.sort();

            builder.push_vec(v);
            tally = 0;
        }
    }

    builder.finalize();

    Ok(buf.freeze())
}
