pub mod impls;

pub use impls::all::{AggState, AllAggregator};
pub use impls::sum::*;
pub use impls::top_k::*;

#[cfg(feature = "rkyv")]
use rkyv::Archive;

use std::fmt::Debug;

#[cfg(not(feature = "rkyv"))]
pub trait PartialAggregateBounds: Default + Debug + Clone + Copy + Send {}

#[cfg(feature = "rkyv")]
pub trait PartialAggregateBounds: Archive + Default + Debug + Clone + Copy + Send {}

macro_rules! impl_agg_bounds {
    ($type:ty) => {
        impl PartialAggregateBounds for $type {}
    };
}

impl_agg_bounds!(u16);
impl_agg_bounds!(u32);
impl_agg_bounds!(u64);
impl_agg_bounds!(u128);

impl_agg_bounds!(i8);
impl_agg_bounds!(i16);
impl_agg_bounds!(i32);
impl_agg_bounds!(i64);
impl_agg_bounds!(i128);

impl_agg_bounds!(f32);
impl_agg_bounds!(f64);

/// Aggregation interface that library users must implement to use Hierarchical Aggregation Wheels
pub trait Aggregator: Default + Debug + 'static {
    /// Input type that can be `lifted` into a partial aggregate
    type Input: Debug + Copy + Send;
    /// Final Aggregate type
    type Aggregate: Send;
    /// Partial Aggregate type
    type PartialAggregate: PartialAggregateBounds;

    /// Convert the input entry to a partial aggregate
    fn lift(&self, input: Self::Input) -> Self::PartialAggregate;
    /// Combine two partial aggregates and produce new output
    fn combine(
        &self,
        a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate;
    /// Convert a partial aggregate to a final result
    fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate;
}
