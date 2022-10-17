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

pub fn read_csv<T, Weight>(source_file_path: &str) -> OrdZSet<T, Weight>
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
    Tuple3<T0, T1, T2>,
}

#[test]
fn csv_test() {
    let src = read_csv::<Tuple3<bool, Option<String>, Option<u32>>, isize>("src/test.csv");
    assert_eq!(zset!(
        Tuple3::new(true, Some(String::from("Mihai")),Some(0)) => 1,
        Tuple3::new(false, Some(String::from("Leonid")),Some(1)) => 1,
        Tuple3::new(true, Some(String::from("Chase")),Some(2)) => 1,
        Tuple3::new(false, Some(String::from("Gerd")),Some(3)) => 1,
        Tuple3::new(true, None, None) => 1,
        Tuple3::new(false, Some(String::from("Nina")),None) => 1,
        Tuple3::new(true, None, Some(6)) => 1,
    ), src);
}
