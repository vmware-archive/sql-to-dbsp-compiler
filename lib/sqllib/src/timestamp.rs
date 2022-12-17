//! Support for SQL Timestamp data type.
//! Similar to a unix timestamp: a positive time interval between Jan 1 1970 and the current time.
//! The supported range is limited (e.g., up to 2038 in MySQL).
//! We use milliseconds to represent the interval.

use std::ops::Add;
use size_of::SizeOf;
use chrono::*;
use serde::*;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf, Serialize, Deserialize)]
pub struct Timestamp {
    // since unix epoch
    milliseconds: i64,
}

impl Timestamp {
    pub const fn new(milliseconds: i64) -> Self {
        Self { milliseconds: milliseconds }
    }

    pub fn milliseconds(&self) -> i64 {
        self.milliseconds
    }

    pub fn toDateTime(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.milliseconds / 1000, (self.milliseconds % 1000) as u32).single().unwrap()
    }
}

impl<T> From<T> for Timestamp
where i64: From<T>
{
    fn from(value: T) -> Self {
        Self { milliseconds: i64::from(value) }
    }
}

impl Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, value: i64) -> Self {
        Self { milliseconds: self.milliseconds + value }
    }
}

////////////////////////////

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf, Serialize, Deserialize)]
pub struct Date {
    // since unix epoch
    days: i32,
}

impl Date {
    pub const fn new(days: i32) -> Self {
        Self { days: days }
    }

    pub fn days(&self) -> i32 {
        self.days
    }
}

impl<T> From<T> for Date
where
    i32: From<T>
{
    fn from(value: T) -> Self {
        Self { days: i32::from(value) }
    }
}

pub fn extract_ISODOW(t: Timestamp) -> i64 {
    let date = t.toDateTime();
    let weekday = date.weekday();
    let day = weekday.number_from_sunday();
    (if day == 0 {7} else {day}) as i64
}

pub fn extract_EPOCH(t: Timestamp) -> i64 {
    t.milliseconds() / 1000
}
