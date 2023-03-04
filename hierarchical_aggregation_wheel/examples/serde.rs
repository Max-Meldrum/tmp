use std::time::Instant;

use hierarchical_aggregation_wheel::aggregator::AllAggregator;
use hierarchical_aggregation_wheel::time::NumericalDuration;
use hierarchical_aggregation_wheel::*;

fn main() {
    // Initial start time
    let time = 0;
    let mut wheel = Wheel::<AllAggregator>::new(time);
    println!(
        "Memory size Wheel {} bytes",
        std::mem::size_of::<Wheel<AllAggregator>>()
    );

    println!(
        "Memory Seconds Wheel {} bytes",
        std::mem::size_of::<SecondsWheel<AllAggregator>>()
    );

    let now = Instant::now();
    let raw_wheel = wheel.as_bytes();
    println!(
        "Serialised empty wheel size {} bytes in {:?}",
        raw_wheel.len(),
        now.elapsed()
    );

    let now = Instant::now();
    let lz4_compressed = lz4_flex::compress_prepend_size(&raw_wheel);
    println!(
        "Empty lz4 serialised wheel size {} bytes in {:?}",
        lz4_compressed.len(),
        now.elapsed(),
    );

    let now = Instant::now();
    let lz4_decompressed = lz4_flex::decompress_size_prepended(&lz4_compressed).unwrap();
    let lz4_wheel = Wheel::<AllAggregator>::from_bytes(&lz4_decompressed);
    println!(
        "Empty lz4 decompress and deserialise wheel in {:?}",
        now.elapsed(),
    );
    assert!(lz4_wheel.is_empty());

    let now = Instant::now();
    let zstd_compressed = zstd::encode_all(&*raw_wheel, 3).unwrap();
    println!(
        "Empty zstd serialised wheel size {} bytes in {:?}",
        zstd_compressed.len(),
        now.elapsed(),
    );

    let total_ticks = wheel.remaining_ticks();

    for _ in 0..total_ticks - 1 {
        wheel.advance(1.seconds());
        wheel
            .insert(Entry::new(1.0, wheel.watermark() + 1))
            .unwrap();
    }
    println!("wheel total {:?}", wheel.range(..));

    let raw_seconds_wheel = wheel.seconds_wheel().as_bytes();
    println!(
        "Serialised Seconds wheel size {} bytes",
        raw_seconds_wheel.len()
    );

    let now = Instant::now();
    let raw_wheel = wheel.as_bytes();
    println!(
        "Full serialised wheel size {} bytes in {:?}",
        raw_wheel.len(),
        now.elapsed()
    );

    let now = Instant::now();
    let lz4_compressed = lz4_flex::compress_prepend_size(&raw_wheel);
    println!(
        "Full lz4 serialised wheel size bytes {} in {:?}",
        lz4_compressed.len(),
        now.elapsed(),
    );

    let now = Instant::now();
    let lz4_decompressed = lz4_flex::decompress_size_prepended(&lz4_compressed).unwrap();
    let lz4_wheel = Wheel::<AllAggregator>::from_bytes(&lz4_decompressed);
    println!(
        "Full lz4 decompress and deserialise wheel in {:?}",
        now.elapsed(),
    );
    println!("deserialised wheel total {:?}", lz4_wheel.range(..));

    let now = Instant::now();
    let zstd_compressed = zstd::encode_all(&*raw_wheel, 3).unwrap();
    println!(
        "full zstd serialised wheel size {} bytes in {:?}",
        zstd_compressed.len(),
        now.elapsed(),
    );
}
