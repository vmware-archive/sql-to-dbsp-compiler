#!/bin/bash

FILE=$1
cd ../../database-stream-processor
FILEPATH=../sql-to-dbsp-compiler/SQL-compiler/${FILE}
cargo run -p dataflow-jit --bin dataflow-jit --features binary -- ${FILEPATH}
#cargo run -p dataflow-jit --features binary -- --print-schema ${FILEPATH}
