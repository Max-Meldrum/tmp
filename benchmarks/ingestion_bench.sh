#!/bin/bash

batch_sizes=(10 100 1000 10000)

for batch_size in "${batch_sizes[@]}"
do
  echo "Running ingestion bench with batch size $batch_size"
  #RUSTFLAGS="-A warnings" cargo run --release --bin ingestion_bench -- --disk --batch-size $batch_size
  cargo run --release --bin ingestion_bench -- --disk --num-batches $batch_size
  rm duckdb_ingestion.db
  rm -r /tmp/wheeldb_rocks
  rm -r /tmp/no_wheeldb_rocks
done
