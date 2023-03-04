#!/bin/bash

batch_sizes=(10000 50000 100000)

for batch_size in "${batch_sizes[@]}"
do
  echo "Running pre materialize bench with batch size $batch_size"
  #RUSTFLAGS="-A warnings" cargo run --release --bin ingestion_bench -- --disk --batch-size $batch_size
  cargo run --release --bin pre_materialize_bench -- --disk --batch-size $batch_size
  rm -r /tmp/wheeldb_rocks
  rm -r /tmp/no_wheeldb_rocks
done
