use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Bencher, BenchmarkId, Criterion,
    Throughput,
};
use hierarchical_aggregation_wheel::aggregator::{Aggregator, AllAggregator, U32SumAggregator};
use hierarchical_aggregation_wheel::*;

const NUM_ELEMENTS: usize = 100000;

pub fn insert_benchmark(c: &mut Criterion) {
    /* 
    {
        let mut group = c.benchmark_group("wheel-latency");
        group.bench_function("insert-no-overflow", insert_no_overflow_latency);
        group.bench_function("insert-no-wheel", insert_no_wheel_latency);
        group.bench_function("insert-all-overflow", insert_all_overflow_latency);
        group.bench_function("insert-mixed", insert_mixed_latency);
    }
    */

    let mut group = c.benchmark_group("wheel-throughput");
    group.throughput(Throughput::Elements(NUM_ELEMENTS as u64));
    //group.bench_function("insert-no-overflow", insert_no_overflow_bench);
    //group.bench_function("insert-no-wheel", insert_no_wheel);
    //group.bench_function("insert-mixed", insert_mixed_bench);

    for out_of_order in [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("insert_out_of_order_{}", out_of_order)),
            out_of_order,
            |b, &out_of_order| {
                insert_out_of_order(out_of_order as f32, b);
            },
        );
    }

    /* 
    for counter in [1, 5000, 10000, 50000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("advance_with_time_{}", counter)),
            counter,
            |b, &counter| {
                insert_with_advance_time(counter as usize, b);
            },
        );
    }
    */
    group.finish();
}

fn insert_no_wheel_latency(bencher: &mut Bencher) {
    let aggregator = AllAggregator;
    let mut current = aggregator.lift(1.0);
    bencher.iter(|| {
        let entry = aggregator.lift(1.0);
        current = aggregator.combine(entry, current);
        current
    });
}

fn insert_no_wheel(bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let aggregator = U32SumAggregator;
            aggregator
        },
        |aggregator| {
            let mut current = aggregator.lift(0u32);
            for _ in 0..NUM_ELEMENTS {
                let entry = aggregator.lift(1u32);
                current = aggregator.combine(entry, current);
            }
            aggregator
        },
        BatchSize::PerIteration,
    );
}

fn insert_no_overflow_latency(bencher: &mut Bencher) {
    let time = 1000;
    let mut wheel = Wheel::<AllAggregator>::new(time);
    bencher.iter(|| {
        wheel.insert(Entry::new(1.0, time)).unwrap();
    });
}

fn insert_no_overflow_bench(bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let time = 0;
            let wheel = Wheel::<AllAggregator>::new(time);
            wheel
        },
        |mut wheel| {
            let entry = Entry::new(1.0, 0);
            for _ in 0..NUM_ELEMENTS {
                wheel.insert(entry).unwrap();
            }
            wheel
        },
        BatchSize::PerIteration,
    );
}

use rand::prelude::*;

fn generate_out_of_order_timestamps(size: usize, percent: f32) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    let mut timestamps: Vec<u64> = (0..=60000).collect();
    let num_swaps = (timestamps.len() as f32 * percent / 100.0).round() as usize;

    for _ in 0..num_swaps {
        let i = rng.gen_range(0..timestamps.len());
        let j = rng.gen_range(0..timestamps.len());
        timestamps.swap(i, j);
    }

    timestamps.truncate(size);
    timestamps.shuffle(&mut rng);

    timestamps
}
fn generate_out_of_order_timestamps_v2(size: usize, percent: f32) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    let timestamps_per_second = 1000;
    let num_seconds = 60;
    let timestamps: Vec<u64> = (0..=num_seconds)
        .flat_map(|second| {
            let start_timestamp = second * 1000;
            let end_timestamp = start_timestamp + 999;
            (start_timestamp..=end_timestamp)
                .into_iter()
                .cycle()
                .take(timestamps_per_second)
        })
        .collect();

    let num_swaps = (timestamps.len() as f32 * percent / 100.0).round() as usize;

    let mut shuffled_timestamps = timestamps.clone();
    shuffled_timestamps.shuffle(&mut rng);

        for i in 0..num_swaps {
        let j = (i + 1..timestamps.len())
            .filter(|&x| shuffled_timestamps[x] > shuffled_timestamps[i])
            .max_by_key(|&x| shuffled_timestamps[x])
            .unwrap_or(i);
        shuffled_timestamps.swap(i, j);
    }

    shuffled_timestamps.truncate(size);
    shuffled_timestamps
}

fn insert_out_of_order(percentage: f32, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let time = 0;
            let wheel = Wheel::<AllAggregator>::new(time);
            let timestamps = generate_out_of_order_timestamps_v2(NUM_ELEMENTS, percentage);
            (wheel, timestamps)
        },
        |(mut wheel, timestamps)| {
            let mut advance_counter = 0;
            let mut time = 0;
            for timestamp in timestamps {
                wheel.insert(Entry::new(1.0, timestamp)).unwrap();

            }
            wheel
        },
        BatchSize::PerIteration,
    );

}
fn insert_with_advance_time(limit: usize, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let time = 0;
            let wheel = Wheel::<AllAggregator>::new(time);
            wheel
        },
        |mut wheel| {
            let mut advance_counter = 0;
            let mut time = 0;
            for _ in 0..NUM_ELEMENTS {
                wheel.insert(Entry::new(1.0, time)).unwrap();
                advance_counter += 1;
                if advance_counter == limit {
                    time += 1000;
                    wheel.advance_to(time);
                    advance_counter = 0;
                }
            }
            wheel
        },
        BatchSize::PerIteration,
    );
}

fn insert_all_overflow_latency(bencher: &mut Bencher) {
    let time = 0;
    let mut wheel = Wheel::<AllAggregator>::new(time);
    wheel.advance_to(3000);
    bencher.iter(|| {
        let overflow_time = fastrand::u64(160000..u64::MAX);
        black_box(assert!(wheel
            .insert(Entry::new(1.0, overflow_time))
            .unwrap_err()
            .is_overflow()));
    });
}

fn insert_mixed_latency(bencher: &mut Bencher) {
    let mut wheel = Wheel::<AllAggregator>::new(0);
    wheel.advance_to(3000);
    bencher.iter(|| {
        let time = fastrand::u64(3000..20000);
        wheel.insert(Entry::new(1.0, time)).unwrap();
    });
}

fn insert_mixed_bench(bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let mut wheel = Wheel::<AllAggregator>::new(0);
            wheel.advance_to(3000);
            wheel
        },
        |mut wheel| {
            for _ in 0..NUM_ELEMENTS {
                let time = fastrand::u64(3000..20000);
                wheel.insert(Entry::new(1.0, time)).unwrap();
            }
            wheel
        },
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, insert_benchmark);
criterion_main!(benches);
