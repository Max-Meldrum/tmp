# WheelDB

The benchmarks in this repo have been executed in a Linux environment, more specfically debian-based linux.

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

To run the benchmarks, see the scripts within the directory. Note that it may take a while depending on your configured hardware.
If a run is killed, then you may not have enough DRAM (see paper on hardware requirements).

To run all types of benchmarks, run the following:

```bash
./ingestion_bench.sh
./pre_materialize_bench.sh
./query_bench.sh
```

For micro-benchmarks of the HAW data structure, please enter the **hierarchical_aggregation_wheel** directory and execute the following.

```bash
cargo bench
```

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.