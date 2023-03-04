use std::time::Instant;

use benchmarks::*;
use clap::Parser;
use duckdb::Result;
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1000000)]
    keys: usize,
    #[clap(short, long, value_parser, default_value_t = 10)]
    num_batches: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000)]
    batch_size: usize,
    #[clap(short, long, action)]
    disk: bool,
    #[clap(arg_enum, value_parser, default_value_t = DistributionMode::Zipf)]
    mode: DistributionMode,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let batch_size = args.batch_size;
    let total_batches = args.num_batches;
    println!("Running with {:#?}", args);
    println!("Ingestion records {}", batch_size * total_batches);

    let ride_batches = DataGenerator::generate_batches_random(total_batches, batch_size);
    let wheeldb_batches = ride_batches.clone();
    let no_wheeldb_batches = ride_batches.clone();

    // Streaming ingestion throughput

    let (mut db, id) = duckdb_setup(args.disk);
    measure(id, batch_size, ride_batches, move |batch| {
        duckdb_append_streaming(batch, &mut db).unwrap();
    });

    let (mut db, id) = wheeldb_rocks_setup();
    measure(id, batch_size, wheeldb_batches, move |batch| {
        wheeldb_rocks_append_batch(batch, &mut db);
    });

    let (mut db, id) = no_wheeldb_rocks_setup();
    measure(id, batch_size, no_wheeldb_batches, move |batch| {
        no_wheeldb_rocks_append_batch(batch, &mut db)
    });

    Ok(())
}


fn no_wheeldb_rocks_setup() -> (wheeldb_rocks::no_wheel_db::NoWheelDB, &'static str) {
    let db = wheeldb_rocks::no_wheel_db::NoWheelDB::open_default("/tmp/no_wheeldb_rocks");
    (db, "NoWheelDB")
}

fn measure(
    id: &str,
    batch_size: usize,
    batches: Vec<Vec<RideData>>,
    mut f: impl FnMut(Vec<RideData>) -> (),
) {
    let total_batches = batches.len();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let full = Instant::now();
    for batch in batches {
        let now = Instant::now();
        f(batch);
        hist.record(now.elapsed().as_micros() as u64).unwrap();
    }
    let runtime = full.elapsed();

    println!(
        "{} ingestion ran at {} ops/s (took {:.2}s)",
        id,
        (batch_size * total_batches) as f64 / runtime.as_secs_f64(),
        runtime.as_secs_f64(),
    );
    println!(
        "batch latencies:\t\t\t\t\t\tmin: {: >4}us\tp50: {: >4}us\tp99: {: \
         >4}us\tp99.9: {: >4}us\tp99.99: {: >4}us\tp99.999: {: >4}us\t max: {: >4}us",
        Duration::from_micros(hist.min()).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.5)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.99)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.999)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.9999)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.99999)).as_micros(),
        Duration::from_micros(hist.max()).as_micros(),
    );
    println!("mean latency {}", hist.mean());
}
