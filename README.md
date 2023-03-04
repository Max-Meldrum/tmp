# WheelDB

The benchmarks in this repo have been executed in a Linux environment. However, it should be possibly to also execute under MACOSX.

## Install Rust

1. Go to https://rustup.rs/.
2. Follow the installation instructions for your operating system.
3. Verify the installation with rustc --version and cargo --version.

## RocksDB dependecies

You need to ensure that your system has the correct deps in order to compile RocksDB

```bash
apt-get install g++ make libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
```


## Running Benchmarks

The benchmarks for **Streaming Ingestion** and **Analytical Queries** may be found in the benchmarks directory.

To run the benchmarks, see the scripts within the directory.
To run all types of benchmarks, run the following:

```bash
./query_bench.sh
./ingestion_bench.sh
./pre_materialization_bench.sh
```

For micro-benchmarks of the HAW data structure, please enter the **hierarchical_aggregation_wheel** directory and execute the following.

```bash
cargo bench
```

*  Benchmarks (Streaming ingestion + Analytical)
*  hierarchical aggregation wheel (HAW)
    * + HAW micro-benchmarks
*  wheeldb-rocks (WheelDB + RocksDB impl)

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.