//! Support for SQL Timestamp data type.
//! Similar to a unix timestamp: a positive time interval between Jan 1 1970 and the current time.
//! The supported range is limited (e.g., up to 2038 in MySQL).
//! We use milliseconds to represent the interval.

use std::ops::Add;
use size_of::SizeOf;
use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Utc};
use serde::{de::Error as _, ser::Error as _, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf)]
pub struct Timestamp {
    // since unix epoch
    milliseconds: i64,
}

/// Serialize timestamp into the `YYYY-MM-DD HH:MM:SS.fff` format, where
/// `fff` is the fractional part of a second.
impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let datetime =
            NaiveDateTime::from_timestamp_millis(self.milliseconds).ok_or_else(|| {
                S::Error::custom(format!(
                    "timestamp value '{}' out of range",
                    self.milliseconds
                ))
            })?;

        serializer.serialize_str(&datetime.format("%F %T%.f").to_string())
    }
}

/// Deserialize timestamp from the `YYYY-MM-DD HH:MM:SS.fff` format.
///
/// # Caveats
///
/// * The format does not include a time zone, because the `TIMESTAMP` type is supposed to be
///   timezone-agnostic.
/// * Different databases may use different default export formats for timestamps.  Moreover,
///   I suspect that binary serialization formats represent timestamps as numbers (which is how
///   they are stored inside the DB), so in the future we may need a smarter implementation that
///   supports all these variants.
impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let timestamp_str: &'de str = Deserialize::deserialize(deserializer)?;

        let timestamp = NaiveDateTime::parse_from_str(&timestamp_str, "%F %T%.f").map_err(|e| {
            D::Error::custom(format!("invalid timestamp string '{timestamp_str}': {e}"))
        })?;

        Ok(Self::new(timestamp.timestamp_millis()))
    }
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf)]
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

/// Serialize date into the `YYYY-MM-DD` format
impl Serialize for Date {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = (self.days as i64) * 86400 * 1000;
        let datetime =
            NaiveDateTime::from_timestamp_millis(millis).ok_or_else(|| {
                S::Error::custom(format!(
                    "date value '{}' out of range",
                    self.days
                ))
            })?;

        serializer.serialize_str(&datetime.format("%F").to_string())
    }
}

/// Deserialize date from the `YYYY-MM-DD` format.
impl<'de> Deserialize<'de> for Date {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: &'de str = Deserialize::deserialize(deserializer)?;
        let timestamp = NaiveDateTime::parse_from_str(&str, "%Y-%m-%d").map_err(|e| {
            D::Error::custom(format!("invalid date string '{str}': {e}"))
        })?;
        Ok(Self::new((timestamp.timestamp() / 86400) as i32))
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
