#!/bin/bash

batch_sizes=(100 1000 10000)

for batch_size in "${batch_sizes[@]}"
do
  echo "Running query bench with events per min $batch_size"
  #RUSTFLAGS="-A warnings" cargo run --release --bin ingestion_bench -- --disk --batch-size $batch_size
  cargo run --release --bin query_bench -- --disk --events-per-min $batch_size
  rm duckdb_ingestion.db
  rm -r /tmp/wheeldb_rocks
done
