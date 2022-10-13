//! Readers that can read OrdZSet data from various sources.

#![allow(unused_imports)]
#![allow(non_snake_case)]
#![allow(dead_code)]

use std::{
    fs::File,
    io::BufReader,
    path::Path,
};
use size_of::*;
use dbsp::{
    zset,
    trace::Batch,
    algebra::{ZRingValue,ZSet,HasOne},
    OrdZSet,
    DBWeight,
    DBData,
};
use serde::{
    Serialize,
    Deserialize,
};
use csv::{
    Reader,
    ReaderBuilder,
};
use sqlvalue::{
    SqlRow,
    SqlValue,
    ToSqlRow,
};
use std::fmt::{
    Formatter,
    Debug,
    Result as FmtResult,
};

fn csv_source<T, Weight>(source_file_path: &str) -> OrdZSet<T, Weight>
where
    T: DBData + for<'de> serde::Deserialize<'de>,
    Weight: DBWeight + HasOne,
{
    let path = Path::new(source_file_path);
    let file = BufReader::new(File::open(&path).unwrap_or_else(|error| {
        panic!(
            "failed to open file '{}': {}",
            source_file_path,
            error,
        )
    }));

    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .from_reader(file);
    let vec = csv_reader.deserialize()
        .map(|x| (x.unwrap(), Weight::one()))
        .collect();
    OrdZSet::<T, Weight>::from_keys((), vec)
}

#[cfg(test)]
use tuple::declare_tuples;

#[cfg(test)]
declare_tuples! {
    Tuple2<T0, T1>,
}

#[test]
fn csv_test() {
    let src = csv_source::<Tuple2<String, u32>, isize>("src/test.csv");
    assert_eq!(zset!(
        Tuple2::new(String::from("Mihai"),0) => 1,
        Tuple2::new(String::from("Leonid"),1) => 1,
        Tuple2::new(String::from("Chase"),2) => 1,
        Tuple2::new(String::from("Gerd"),3) => 1
    ), src);
}
