/// An All Aggregator enabling the following functions (MAX, MIN, SUM, COUNT, AVG).
pub mod all;
/// Incremental Sum aggregation
pub mod sum;
/// Temporal Top-K aggregator on top of another aggregator
///
/// The implementation assumes fixed-sized keys
pub mod top_k;

pub use all::{AggState, AllAggregator};
pub use sum::*;
pub use top_k::{map::TopKMap, TopKAggregator};
