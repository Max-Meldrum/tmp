#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

use std::{cmp::Ordering, fmt::Debug};

use crate::{Aggregator, PartialAggregateBounds};

#[repr(C)]
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
pub struct AggState {
    min: f64,
    max: f64,
    count: u64,
    sum: f64,
}
impl AggState {
    #[inline]
    pub fn new(value: f64) -> Self {
        Self {
            min: value,
            max: value,
            count: 1,
            sum: value,
        }
    }
    #[inline]
    pub fn merge(&mut self, other: Self) {
        self.min = f64::min(self.min, other.min);
        self.max = f64::max(self.max, other.max);
        self.count += other.count;
        self.sum += other.sum;
    }
    pub fn min_value(&self) -> f64 {
        self.min
    }
    pub fn max_value(&self) -> f64 {
        self.max
    }
    pub fn sum(&self) -> f64 {
        self.sum
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn avg(&self) -> f64 {
        self.sum / self.count as f64
    }
}

impl PartialAggregateBounds for AggState {}
impl Ord for AggState {
    fn cmp(&self, other: &Self) -> Ordering {
        f64::total_cmp(&self.sum, &other.sum)
    }
}

impl PartialOrd for AggState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for AggState {}
impl PartialEq for AggState {
    fn eq(&self, other: &Self) -> bool {
        self.sum == other.sum
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AllAggregator;

impl Aggregator for AllAggregator {
    type Input = f64;
    type Aggregate = AggState;
    type PartialAggregate = AggState;

    #[inline]
    fn lift(&self, input: Self::Input) -> Self::PartialAggregate {
        AggState::new(input)
    }
    #[inline]
    fn combine(
        &self,
        mut a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate {
        a.merge(b);
        a
    }
    #[inline]
    fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }
}

#[cfg(test)]
mod tests {
    use crate::Wheel;

    use super::*;

    #[test]
    fn all_test() {
        let aggregator = AllAggregator::default();
        let mut time = 0u64;
        let mut wheel = Wheel::new(time);

        for _ in 0..wheel.remaining_ticks() {
            wheel.advance_to(time);
            let entry = crate::Entry::new(1.0, time);
            wheel.insert(entry).unwrap();
            time += 1000; // increase by 1 second
        }

        let all = wheel.minutes_wheel().lower(1, &aggregator).unwrap();
        assert_eq!(all.sum(), 60.0);
        assert_eq!(all.count(), 60);
        assert_eq!(all.max_value(), 1.0);
        assert_eq!(all.min_value(), 1.0);
        assert_eq!(all.avg(), 1.0);
    }
}
