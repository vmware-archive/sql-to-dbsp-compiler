#!/bin/bash

cd ../../database-stream-processor
FILE=../sql-to-dbsp-compiler/SQL-compiler/x.json
cargo run -p dataflow-jit --bin dataflow-jit --features binary -- $FILE
#cargo run -p dataflow-jit --features binary -- --print-schema $FILE
