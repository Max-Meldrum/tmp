use crate::Aggregator;
use std::fmt::Debug;

mod entry;
pub mod map;
mod state;

pub use map::TopKMap;
pub use state::TopKState;

#[derive(Default, Debug, Clone, Copy)]
pub struct TopKAggregator<const K: usize, const KEY_BYTES: usize>;

impl<const K: usize, const KEY_BYTES: usize> Aggregator for TopKAggregator<K, KEY_BYTES> {
    type Input = TopKState<K, KEY_BYTES>;
    type Aggregate = TopKState<K, KEY_BYTES>;
    type PartialAggregate = TopKState<K, KEY_BYTES>;

    #[inline]
    fn lift(&self, input: Self::Input) -> Self::PartialAggregate {
        input
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
    use crate::aggregator::impls::TopKAggregator;
    use crate::aggregator::AggState;
    use crate::Wheel;

    use super::map::TopKMap;
    use super::state::TopKState;

    #[test]
    fn top_k_test() {
        let top_k_aggregator: TopKAggregator<10, 4> = TopKAggregator::default();
        let mut time = 0u64;
        // top_k wheel
        let mut wheel = Wheel::new(time);

        let mut top_k_map: TopKMap<4> = TopKMap::with_capacity(1024);

        // insert some keys
        top_k_map.insert(1u32.to_le_bytes(), AggState::new(10.0));
        top_k_map.insert(2u32.to_le_bytes(), AggState::new(50.0));
        top_k_map.insert(3u32.to_le_bytes(), AggState::new(30.0));

        // verify Top 10
        let state: TopKState<10, 4> = top_k_map.to_state();
        let arr = state.iter();
        assert_eq!(arr[0].unwrap().data.sum(), 10.0);
        assert_eq!(arr[1].unwrap().data.sum(), 30.0);
        assert_eq!(arr[2].unwrap().data.sum(), 50.0);
        assert!(arr[3].is_none());
        assert!(arr[4].is_none());
        assert!(arr[5].is_none());
        assert!(arr[6].is_none());
        assert!(arr[7].is_none());
        assert!(arr[8].is_none());
        assert!(arr[9].is_none());

        let entry = crate::Entry::new(state, time);
        wheel.insert(entry).unwrap();

        // insert same keys with different values
        top_k_map.insert(1u32.to_le_bytes(), AggState::new(100.0));
        top_k_map.insert(2u32.to_le_bytes(), AggState::new(10.0));
        top_k_map.insert(3u32.to_le_bytes(), AggState::new(20.0));

        // insert some new keys
        top_k_map.insert(4u32.to_le_bytes(), AggState::new(15.0));
        top_k_map.insert(5u32.to_le_bytes(), AggState::new(1.0));
        top_k_map.insert(6u32.to_le_bytes(), AggState::new(5000.0));

        // insert 2nd batch to wheel
        let state: TopKState<10, 4> = top_k_map.to_state();
        let entry = crate::Entry::new(state, time);
        wheel.insert(entry).unwrap();

        // advance wheel
        time += 1000; // increase by 1 second
        wheel.advance_to(time);

        // combined TopKState
        let agg = wheel.seconds_wheel().lower(1, &top_k_aggregator).unwrap();
        let arr = agg.iter();
        assert_eq!(arr[0].unwrap().data.sum(), 1.0);
        assert_eq!(arr[1].unwrap().data.sum(), 15.0);
        assert_eq!(arr[2].unwrap().data.sum(), 50.0);
        assert_eq!(arr[3].unwrap().data.sum(), 60.0);
        assert_eq!(arr[4].unwrap().data.sum(), 110.0);
        assert_eq!(arr[5].unwrap().data.sum(), 5000.0);
        assert!(arr[6].is_none());
        assert!(arr[7].is_none());
        assert!(arr[8].is_none());
        assert!(arr[9].is_none());
    }
}
