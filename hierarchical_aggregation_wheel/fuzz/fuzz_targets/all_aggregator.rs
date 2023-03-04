#![no_main]

use libfuzzer_sys::fuzz_target;
use hierarchical_aggregation_wheel::*;
use arbitrary::Arbitrary;
use hierarchical_aggregation_wheel::aggregator::AllAggregator;
use hierarchical_aggregation_wheel::HierarchicalAggregationWheel;

#[derive(Debug, Arbitrary)]
enum Op {
    Insert(f64, u64),
    Advance(u64),
    Range(u64, u64),
}

fuzz_target!(|ops: Vec<Op>| {
    let time = 0u64;
    let mut wheel: HierarchicalAggregationWheel<AllAggregator> = HierarchicalAggregationWheel::new(time);
    let aggregator = AllAggregator::default();

    for op in ops {
        match op {
            Op::Insert(data, timestamp) => {
                let _ = wheel.insert(WheelEntry::new(data, timestamp));
            }
            Op::Advance(watermark) => {
                wheel.advance_to(watermark);
            }
            Op::Range(start, end) => {
                //let _ = wheel.range(start..end);
            }
        }
    }
});
