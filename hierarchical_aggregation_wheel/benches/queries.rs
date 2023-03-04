use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use hierarchical_aggregation_wheel::aggregator::{AllAggregator};
use hierarchical_aggregation_wheel::time::Duration;
use hierarchical_aggregation_wheel::*;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheel-queries");
    group.bench_function("random-interval", random_interval_bench);
    group.bench_function("random-seconds-interval", random_seconds_interval_bench);
    group.bench_function("random-minutes-interval", random_minutes_interval_bench);
    group.bench_function("random-hours-interval", random_hour_interval_bench);
    group.bench_function("random-days-interval", random_days_interval_bench);

    group.bench_function("landmark-window", landmark_window_bench);

    group.bench_function("combine-lower-time-random", combine_and_lower_time_random);
    group.bench_function(
        "combine-lower-time-worst-possible",
        combine_and_lower_worst_time_possible,
    );

    group.finish();
}
fn random_interval() -> Duration {
    let pick = fastrand::u8(0..4);
    if pick == 0u8 {
        random_seconds()
    } else if pick == 1u8 {
        random_minute()
    } else if pick == 2u8 {
        random_hours()
    } else {
        random_days()
    }
}
fn random_hours() -> Duration {
    Duration::hours(fastrand::u64(1..24) as i64)
}
fn random_minute() -> Duration {
    Duration::minutes(fastrand::u64(1..59) as i64)
}
fn random_days() -> Duration {
    Duration::days(fastrand::u64(1..7) as i64)
}
fn random_seconds() -> Duration {
    Duration::seconds(fastrand::u64(1..59) as i64)
}

fn large_wheel() -> (Wheel<AllAggregator>, AllAggregator) {
    let aggregator = AllAggregator;
    let mut time = 0;
    let mut wheel = Wheel::new(time);
    let ticks = 604800; // 7-days as seconds
    for _ in 0..ticks {
        wheel.advance_to(time);
        wheel.insert(Entry::new(1.0, time)).unwrap();
        time += 1000;
    }
    (wheel, aggregator)
}

fn random_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.interval(random_interval()).is_some());
    });
}
fn landmark_window_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.range(..).is_some());
    });
}

fn random_seconds_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.interval(random_seconds()).is_some());
    });
}
fn random_minutes_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.interval(random_minute()).is_some());
    });
}

fn random_hour_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.interval(random_hours()).is_some());
    });
}
fn random_days_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.interval(random_days()).is_some());
    });
}

fn combine_and_lower_time_random(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        let pick = fastrand::u8(0..4);
        if pick == 0u8 {
            let seconds = fastrand::u8(1..60);
            let minutes = fastrand::u8(1..60);
            let hour = fastrand::u8(1..24);
            let day = fastrand::u8(1..8);
            assert!(wheel
                .combine_and_lower_time(Some(seconds), Some(minutes), Some(hour), Some(day))
                .is_some());
        } else if pick == 1u8 {
            let seconds = fastrand::u8(1..60);
            let minutes = fastrand::u8(1..60);
            let hour = fastrand::u8(1..24);
            assert!(wheel
                .combine_and_lower_time(Some(seconds), Some(minutes), Some(hour), None)
                .is_some());
        } else if pick == 2u8 {
            let minutes = fastrand::u8(1..60);
            let hour = fastrand::u8(1..24);
            assert!(wheel
                .combine_and_lower_time(None, Some(minutes), Some(hour), None)
                .is_some());
        } else {
            let hour = fastrand::u8(1..24);
            assert!(wheel
                .combine_and_lower_time(None, None, Some(hour), None)
                .is_some());
        }
    });
}

fn combine_and_lower_worst_time_possible(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        let seconds = 59u8;
        let minutes = 59u8;
        let hour = 23u8;
        let day = 07u8;
        assert!(wheel
            .combine_and_lower_time(Some(seconds), Some(minutes), Some(hour), Some(day))
            .is_some());
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
