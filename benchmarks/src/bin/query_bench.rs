use std::{time::Instant, borrow::Cow};

use benchmarks::*;
use clap::Parser;
use duckdb::Result;
use hdrhistogram::Histogram;
use hierarchical_aggregation_wheel::time;
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1000000)]
    keys: usize,
    #[clap(short, long, value_parser, default_value_t = 100)]
    num_batches: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000)]
    batch_size: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000_0)]
    events_per_min: usize,
    #[clap(short, long, value_parser, default_value_t = 10000)]
    queries: usize,
    #[clap(short, long, action)]
    disk: bool,
    #[clap(arg_enum, value_parser, default_value_t = DistributionMode::Zipf)]
    mode: DistributionMode,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let total_queries = args.queries;
    let events_per_min = args.events_per_min;

    println!("Running with {:#?}", args);

    let (watermark, batches) = DataGenerator::generate_query_data(events_per_min);
    let duckdb_batches = batches.clone();

    let point_queries_low_interval = QueryGenerator::generate_low_interval_point_queries(total_queries);
    let point_queries_high_interval = QueryGenerator::generate_high_interval_point_queries(total_queries);
    let olap_queries_low_interval = QueryGenerator::generate_low_interval_olap(total_queries);
    let olap_queries_high_interval = QueryGenerator::generate_high_interval_olap(total_queries);

    let wheeldb_point_queries_low_interval = point_queries_low_interval.clone();
    let wheeldb_point_queries_high_interval = point_queries_high_interval.clone();
    let wheeldb_olap_queries_low_interval = olap_queries_low_interval.clone();
    let wheeldb_olap_queries_high_interval= olap_queries_high_interval.clone();

    let total_entries = batches.len() * events_per_min;
    println!("Running with total entries {}", total_entries);

    // Prepare DuckDB
    let (mut duckdb, id) = duckdb_setup(args.disk);
    for batch in duckdb_batches {
        duckdb_append_batch(batch, &mut duckdb).unwrap();
    }
    println!(
        "Finished preparing {}",
        id,
    );

    // Prepare WheelDB
    let (mut wheeldb, id) = wheeldb_rocks_setup();
    for batch in batches {
        wheeldb_rocks_append_batch(batch, &mut wheeldb);
        wheeldb.advance_to(wheeldb.watermark() + 60000u64);
    }
    wheeldb.flush();

    println!(
        "Finished preparing {}",
        id,
    );

    dbg!(watermark);

    //duckdb_run("DuckDB OLAP Low Intervals", watermark, &duckdb, olap_queries_low_interval);
    //duckdb_run("DuckDB OLAP High Intervals", watermark, &duckdb, olap_queries_high_interval);
    duckdb_run("DuckDB Point Queries Low Intervals", watermark, &duckdb, point_queries_low_interval);
    duckdb_run("DuckDB Point Queries High Intervals", watermark, &duckdb, point_queries_high_interval);

    //wheeldb_run("WheelDB OLAP Low Intervals", watermark, &wheeldb, wheeldb_olap_queries_low_interval);
    //wheeldb_run("WheelDB OLAP High Intervals", watermark, &wheeldb, wheeldb_olap_queries_high_interval);
    wheeldb_run("WheelDB Point Queries Low Intervals", watermark, &wheeldb, wheeldb_point_queries_low_interval);
    wheeldb_run("WheelDB Point Queries High Intervals", watermark, &wheeldb, wheeldb_point_queries_high_interval);

    Ok(())
}
fn wheeldb_run(id: &str, _watermark: u64, db: &wheeldb_rocks::WheelDB, queries: Vec<Query>) {
    let total_queries = queries.len();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let full = Instant::now();
    for query in queries {
        let now = Instant::now();
        let wheel_opt = match query.query_type {
            QueryType::Keyed(pu_location_id) => db.get(pu_location_id.to_le_bytes()).map(|w| Cow::Owned(w)),
            QueryType::All => Some(Cow::Borrowed(db.get_star_wheel())),
        };
        /* 
        let wheel_opt = match query.query_type {
            QueryType::Keyed(pu_location_id) => db.get(pu_location_id.to_le_bytes()),
            QueryType::All => db.get_star_wheel_from_disk(),
        };
        */
        if let Some(wheel) = wheel_opt {
            match query.interval {
                QueryInterval::Seconds(secs) => {
                    let result = wheel.interval(time::Duration::seconds(secs as i64));
                    drop(result);
                }
                QueryInterval::Minutes(mins) => {
                    let result = wheel.interval(time::Duration::minutes(mins as i64));
                    drop(result);
                }
                QueryInterval::Hours(hours) => {
                    let result = wheel.interval(time::Duration::hours(hours as i64));
                    drop(result);
                }
                QueryInterval::Days(days) => {
                    let result = wheel.interval(time::Duration::days(days as i64));
                    drop(result);
                }
                QueryInterval::Landmark => {
                    let result = wheel.range(..);
                    drop(result);
                }
            }
        }
        hist.record(now.elapsed().as_micros() as u64).unwrap();
    }
    let runtime = full.elapsed();
    println!(
        "{} ran with {} queries/s (took {:.2}s)",
        id,
        total_queries as f64 / runtime.as_secs_f64(),
        runtime.as_secs_f64(),
    );

    print_hist(id, &hist);
}

fn duckdb_run(id: &str, watermark: u64, db: &duckdb::Connection, queries: Vec<Query>) {
    let total_queries = queries.len();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let base_str = "SELECT AVG(fare_amount), SUM(fare_amount), MIN(fare_amount), MAX(fare_amount), COUNT(fare_amount) FROM rides";
    let sql_queries: Vec<String> = queries
        .iter()
        .map(|query| {

            // Generate Key clause. If no key then leave as empty ""
            let key_clause = match query.query_type {
                QueryType::Keyed(pu_location_id) => {
                    if let QueryInterval::Landmark = query.interval {
                        format!("WHERE pu_location_id={}", pu_location_id)
                    } else {
                        format!("WHERE pu_location_id={} AND", pu_location_id)
                    }
                }
                QueryType::All => {
                    if let QueryInterval::Landmark = query.interval {
                        "".to_string()
                    } else {
                        "WHERE".to_string()
                    }
                }
            };
            let interval = match query.interval {
                QueryInterval::Seconds(secs) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::seconds(secs.into()).whole_milliseconds() as u64,
                    );
                    format!("do_time >= {} AND do_time < {}", start_ts, watermark)
                }
                QueryInterval::Minutes(mins) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::minutes(mins.into()).whole_milliseconds() as u64,
                    );
                    format!("do_time >= {} AND do_time < {}", start_ts, watermark)
                }
                QueryInterval::Hours(hours) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::hours(hours.into()).whole_milliseconds() as u64,
                    );
                    format!("do_time >= {} AND do_time < {}", start_ts, watermark)
                }
                QueryInterval::Days(days) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::days(days.into()).whole_milliseconds() as u64,
                    );
                    format!("do_time >= {} AND do_time < {}", start_ts, watermark)
                }
                QueryInterval::Landmark => "".to_string(),
            };
            // Generate SQL str to be executed
            let full_query = format!("{} {} {}", base_str, key_clause, interval);
            //println!("QUERY {}", full_query);
            full_query
        })
        .collect();

    let full = Instant::now();
    for sql_query in sql_queries {
        let now = Instant::now();
        duckdb_query(&sql_query, &db).unwrap();
        hist.record(now.elapsed().as_micros() as u64).unwrap();
    }
    let runtime = full.elapsed();

    println!(
        "{} ran with {} queries/s (took {:.2}s)",
        id,
        total_queries as f64 / runtime.as_secs_f64(),
        runtime.as_secs_f64(),
    );
    print_hist(id, &hist);
}

fn print_hist(id: &str, hist: &Histogram<u64>) {
    println!(
        "{} latencies:\t\t\t\t\t\tmin: {: >4}us\tp50: {: >4}us\tp99: {: \
         >4}us\tp99.9: {: >4}us\tp99.99: {: >4}us\tp99.999: {: >4}us\t max: {: >4}us \t count: {}",
        id,
        Duration::from_micros(hist.min()).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.5)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.99)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.999)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.9999)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.99999)).as_micros(),
        Duration::from_micros(hist.max()).as_micros(),
        hist.len(),
    );
}
