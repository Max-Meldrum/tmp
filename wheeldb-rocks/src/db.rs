use ahash::AHashMap;
use hierarchical_aggregation_wheel::Wheel;
use hierarchical_aggregation_wheel::{aggregator::AllAggregator, Entry};
use minstant::Instant;
use std::collections::HashSet;
use std::path::PathBuf;

pub const DEFAULT_DIMENSION_NAME: &str = "default";
pub const STAR_WHEEL_ID: &str = "*Wheel";

use rocksdb::{MergeOperands, Options, DB};

use std::sync::atomic::{AtomicU64, Ordering};

static WATERMARK: AtomicU64 = AtomicU64::new(1000);
pub static mut MERGE_LATENCY: Option<hdrhistogram::Histogram<u64>> = None;

fn partial_wheel_merge(
    _key: &[u8],
    _existing_val: Option<&[u8]>,
    _operands: &MergeOperands,
) -> Option<Vec<u8>> {
    None
}

// merge function for pre-aggregating (AVG, MAX, MIN, MAX, COUNT) data into a Wheel
fn full_wheel_merge(
    _key: &[u8],
    existing_wheel: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let now = Instant::now();
    let watermark = WATERMARK.load(Ordering::Relaxed);

    let mut wheel: Wheel<AllAggregator> = if let Some(bytes) = existing_wheel {
        let decompressed = lz4_flex::decompress_size_prepended(bytes).unwrap();
        Wheel::from_bytes(&decompressed)
    } else {
        Wheel::new(1000)
    };

    // TODO: would be great to access all entries directly (but MergeOperands interface is iter only)
    for op in operands {
        let entry: Entry<f64> = unsafe { rkyv::from_bytes_unchecked(op).unwrap() };
        if let Err(_err) = wheel.insert(entry) {
            //eprintln!("{}", err);
        }
    }

    // advance wheel if watermark exceeds wheel's watermark
    if watermark > wheel.watermark() {
        //println!("Advancing WM to {}", watermark);
        wheel.advance_to(watermark);
    }

    let bytes = lz4_flex::compress_prepend_size(&wheel.as_bytes());
    //println!("Wheel merge elapsed {:?}", now.elapsed());
    unsafe {
        if let Some(ref mut hist) = MERGE_LATENCY {
            hist.record(now.elapsed().as_micros() as u64).unwrap();
        }
    }

    Some(bytes)
}

use std::sync::Arc;
pub struct Dimension {
    id: String,
    inner: Arc<DB>,
    //star_wheel: Wheel<AllAggregator>,
}
impl Dimension {
    pub fn new(id: String, core: Arc<DB>) -> Self {
        Self { id, inner: core }
    }
    pub fn merge(&self, key: impl AsRef<[u8]>, entry: Entry<f64>) {
        let cf = self.inner.cf_handle(&self.id).unwrap();
        let bytes = rkyv::to_bytes::<Entry<f64>, 64>(&entry).unwrap();
        self.inner.merge_cf(cf, key.as_ref(), bytes).unwrap();
    }
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<Wheel<AllAggregator>> {
        let cf = self.inner.cf_handle(&self.id).unwrap();
        match self.inner.get_cf(cf, key.as_ref()).unwrap() {
            Some(bytes) => {
                let decompressed = lz4_flex::decompress_size_prepended(&bytes).unwrap();
                let wheel = Wheel::<AllAggregator>::from_bytes(&decompressed);
                /*
                if self.watermark > wheel.watermark() {
                    wheel.advance_to(self.watermark);
                }
                */
                Some(wheel)
            }
            None => None,
        }
    }
}

pub struct WheelDB {
    db: Arc<DB>,
    star_wheel: Wheel<AllAggregator>,
    dimensions: AHashMap<String, Dimension>,
    watermark: u64,
}

impl WheelDB {
    pub fn open_default(path: impl Into<PathBuf>) -> Self {
        unsafe {
            MERGE_LATENCY = Some(hdrhistogram::Histogram::<u64>::new(4).unwrap());
        };
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator("wheel operator", full_wheel_merge, partial_wheel_merge);
        let db = Arc::new(DB::open(&opts, path.into()).unwrap());
        Self {
            db,
            star_wheel: Wheel::new(1000),
            dimensions: Default::default(),
            watermark: 1000,
        }
    }
    pub fn open_default_with_dimensions(path: impl Into<PathBuf>, dimensions: &[&str]) -> Self {
        unsafe {
            MERGE_LATENCY = Some(hdrhistogram::Histogram::<u64>::new(4).unwrap());
        };
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator("wheel operator", full_wheel_merge, partial_wheel_merge);
        let path = path.into();

        let _column_families: HashSet<String> = match DB::list_cf(&opts, &path) {
            Ok(cfs) => cfs.into_iter().filter(|n| n != "default").collect(),
            // TODO: possibly platform-dependant error message check
            Err(e) if e.to_string().contains("No such file or directory") => HashSet::new(),
            _ => panic!("hej"),
            //Err(e) => return Err(e.into()),
        };

        /*
        let cfds = if !column_families.is_empty() {
            column_families
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect()
        } else {
            vec![ColumnFamilyDescriptor::new("default", Options::default())]
        };
        */

        let mut db = DB::open_cf_descriptors(&opts, path, vec![]).unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator("wheel operator", full_wheel_merge, partial_wheel_merge);
        for dim in dimensions {
            db.create_cf(*dim, &opts).unwrap();
        }
        let db = Arc::new(db);
        let mut dim_map = AHashMap::default();
        for dim in dimensions {
            dim_map.insert(dim.to_string(), Dimension::new(dim.to_string(), db.clone()));
        }
        Self {
            db,
            star_wheel: Wheel::new(1000),
            dimensions: dim_map,
            watermark: 1000,
        }
    }
    pub fn dimension(&self, dim: impl AsRef<str>) -> &Dimension {
        self.dimensions.get(dim.as_ref()).unwrap()
    }

    pub fn flush(&self) {
        // persist star wheel first
        let compressed = lz4_flex::compress_prepend_size(&self.star_wheel.as_bytes());
        self.db.put(STAR_WHEEL_ID, &compressed).unwrap();

        self.db.flush().unwrap();
    }
    pub fn snapshot(&self) {
        self.db.snapshot();
    }
    pub fn merge_hist(&self) -> Option<&hdrhistogram::Histogram<u64>> {
        unsafe { MERGE_LATENCY.as_ref() }
    }
    // force materialization of rocksdb's merge operator
    pub fn materialize_merge(&self) {
        let start = None as Option<&[u8]>;
        let end = None as Option<&[u8]>;
        self.db.compact_range(start, end);
    }
    pub fn watermark(&self) -> u64 {
        WATERMARK.load(Ordering::Relaxed)
    }
    pub fn get_star_wheel(&self) -> &Wheel<AllAggregator> {
        &self.star_wheel
    }
    pub fn get_star_wheel_from_disk(&self) -> Option<Wheel<AllAggregator>> {
        self.get(STAR_WHEEL_ID)
    }
    pub fn get_star_wheel_mut(&mut self) -> &mut Wheel<AllAggregator> {
        &mut self.star_wheel
    }
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<Wheel<AllAggregator>> {
        match self.db.get(key.as_ref()).unwrap() {
            Some(bytes) => {
                let decompressed = lz4_flex::decompress_size_prepended(&bytes).unwrap();
                let mut wheel = Wheel::<AllAggregator>::from_bytes(&decompressed);
                if self.watermark > wheel.watermark() {
                    wheel.advance_to(self.watermark);
                }
                Some(wheel)
            }
            None => None,
        }
    }
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) {
        if watermark > self.watermark {
            self.watermark = watermark;
            WATERMARK.swap(watermark, Ordering::Relaxed);
            self.star_wheel.advance_to(watermark);
        }
    }
    #[inline]
    pub fn merge(&mut self, key: impl AsRef<[u8]>, entry: Entry<f64>) {
        if let Err(_err) = self.star_wheel.insert(entry) {
            // log
        }
        let bytes = rkyv::to_bytes::<Entry<f64>, 64>(&entry).unwrap();
        self.db.merge(key.as_ref(), &bytes).unwrap();
        //self.db.merge(STAR_WHEEL_ID, &bytes).unwrap();
    }

    #[inline]
    pub fn merge_with_dim<K>(&mut self, dim: impl AsRef<str>, key: K, entry: Entry<f64>)
    where
        K: AsRef<[u8]>,
    {
        if let Err(_err) = self.star_wheel.insert(entry) {
            // log
        }
        let bytes = rkyv::to_bytes::<Entry<f64>, 64>(&entry).unwrap();
        let cf = self.db.cf_handle(dim.as_ref()).unwrap();
        self.db.merge_cf(cf, key.as_ref(), &bytes).unwrap();
    }
}
