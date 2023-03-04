use hierarchical_aggregation_wheel::aggregator::AggState;
use hierarchical_aggregation_wheel::Entry;
use std::path::PathBuf;

use rocksdb::{MergeOperands, Options, DB};

pub static mut MERGE_LATENCY: Option<hdrhistogram::Histogram<u64>> = None;

fn partial_merge(
    _key: &[u8],
    _existing_val: Option<&[u8]>,
    _operands: &MergeOperands,
) -> Option<Vec<u8>> {
    None
}

// merge function for AggState (AVG, MAX, MIN, MAX, COUNT) without any time dimension
fn merge(_key: &[u8], existing_val: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let now = minstant::Instant::now();
    let mut agg_state = if let Some(val) = existing_val {
        let state: AggState = unsafe { rkyv::from_bytes_unchecked(val).unwrap() };
        state
    } else {
        AggState::default()
    };

    for op in operands {
        let state: AggState = unsafe { rkyv::from_bytes_unchecked(op).unwrap() };
        agg_state.merge(state);
    }
    let bytes = rkyv::to_bytes::<AggState, 64>(&agg_state).unwrap();
    unsafe {
        if let Some(ref mut hist) = MERGE_LATENCY {
            hist.record(now.elapsed().as_micros() as u64).unwrap();
        }
    }
    Some(bytes.to_vec()) 
}

pub struct NoWheelDB {
    db: DB,
    watermark: u64,
}

impl NoWheelDB {
    pub fn open_default(path: impl Into<PathBuf>) -> Self {
        unsafe {
            MERGE_LATENCY = Some(hdrhistogram::Histogram::<u64>::new(4).unwrap());
        };
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator("wheel operator", merge, partial_merge);
        let db = DB::open(&opts, path.into()).unwrap();
        Self {
            db,
            watermark: 1000,
        }
    }
    pub fn flush(&self) {
        self.db.flush().unwrap();
    }
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<AggState> {
        self.db.get(key.as_ref()).unwrap().map(|bytes| {
            let state: AggState = unsafe { rkyv::from_bytes_unchecked(&bytes).unwrap() };
            state
        })
    }
    pub fn materialize_merge(&self) {
        let start = None as Option<&[u8]>;
        let end = None as Option<&[u8]>;
        self.db.compact_range(start, end);
    }
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) {
        if watermark > self.watermark {
            self.watermark = watermark;
        }
    }
    #[inline]
    pub fn append(&mut self, key: impl AsRef<[u8]>, entry: Entry<f64>) {
        let agg_state = AggState::new(entry.data);
        let bytes = rkyv::to_bytes::<AggState, 64>(&agg_state).unwrap();
        self.db.merge(key.as_ref(), &bytes).unwrap();
    }
}
