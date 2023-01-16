#![allow(non_snake_case)]

pub mod casts;
pub mod geopoint;
pub mod interval;
pub mod timestamp;

use std::ops::Add;
use chrono::{Utc, TimeZone, Datelike};
use dbsp::algebra::{F32, F64, ZRingValue, Semigroup, SemigroupValue};
use geopoint::GeoPoint;
use rust_decimal::prelude::*;
use crate::interval::{
    ShortInterval,
    LongInterval,
};
use crate::timestamp::{
    Timestamp,
    Date,
};
use std::marker::PhantomData;

#[derive(Clone)]
pub struct DefaultOptSemigroup<T>(PhantomData<T>);

impl<T> Semigroup<Option<T>> for DefaultOptSemigroup<T>
where
    T: SemigroupValue,
{
    fn combine(left: &Option<T>, right: &Option<T>) -> Option<T> {
        match (left, right) {
            (None, _) => None,
            (_, None) => None,
            (Some(x), Some(y)) => Some(x.add_by_ref(y)),
        }
    }
}

#[derive(Clone)]
pub struct PairSemigroup<T, R, TS, RS>(PhantomData<(T, R, TS, RS)>);

impl<T, R, TS, RS> Semigroup<(T, R)> for PairSemigroup<T, R, TS, RS>
where
    TS: Semigroup<T>,
    RS: Semigroup<R>,
{
    fn combine(left: &(T, R), right: &(T, R)) -> (T, R) {
        (
            TS::combine(&left.0, &right.0),
            RS::combine(&left.1, &right.1),
        )
    }
}

#[inline(always)]
pub fn or_b_b(left: bool, right: bool) -> bool
{
    left || right
}

#[inline(always)]
pub fn or_bN_b(left: Option<bool>, right: bool) -> Option<bool>
{
    match (left, right) {
        (Some(l), r) => Some(l || r),
        (_, true) => Some(true),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn or_b_bN(left: bool, right: Option<bool>) -> Option<bool>
{
    match (left, right) {
        (l, Some(r)) => Some(l || r),
        (true, _) => Some(true),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn or_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l || r),
        (Some(true), _) => Some(true),
        (_, Some(true)) => Some(true),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn and_b_b(left: bool, right: bool) -> bool
{
    left && right
}

#[inline(always)]
pub fn and_bN_b(left: Option<bool>, right: bool) -> Option<bool>
{
    match (left, right) {
        (Some(l), r) => Some(l && r),
        (_, false) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn and_b_bN(left: bool, right: Option<bool>) -> Option<bool>
{
    match (left, right) {
        (l, Some(r)) => Some(l && r),
        (false, _) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn and_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l && r),
        (Some(false), _) => Some(false),
        (_, Some(false)) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn is_null<T>(value: Option<T>) -> bool
{
    match value {
        Some(_) => false,
        _       => true,
    }
}

#[inline(always)]
pub fn indicator<T>(value: Option<T>) -> i64
{
    match value {
        None => 0,
        Some(_) => 1,
    }
}

pub fn agg_max_N_N<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Ord + Copy
{
    match (left, right) {
        (None, _) => right,
        (_, None) => left,
        (Some(x), Some(y)) => Some(x.max(y))
    }
}

pub fn agg_min_N_N<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Ord + Copy
{
    match (left, right) {
        (None, _) => right,
        (_, None) => left,
        (Some(x), Some(y)) => Some(x.min(y))
    }
}

pub fn agg_max_N_<T>(left: Option<T>, right: T) -> Option<T>
where
    T: Ord + Copy
{
    match (left, right) {
        (None, _) => Some(right),
        (Some(x), y) => Some(x.max(y))
    }
}

pub fn agg_min_N_<T>(left: Option<T>, right: T) -> Option<T>
where
    T: Ord + Copy
{
    match (left, right) {
        (None, _) => Some(right),
        (Some(x), y) => Some(x.min(y))
    }
}

pub fn agg_max__<T>(left: T, right: T) -> T
where
    T: Ord + Copy
{
    left.max(right)
}

pub fn agg_min__<T>(left: T, right: T) -> T
where
    T: Ord + Copy
{
    left.min(right)
}

pub fn agg_plus_N_N<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Add<T, Output = T> + Copy
{
    match (left, right) {
        (None, _) => right,
        (_, None) => left,
        (Some(x), Some(y)) => Some(x + y)
    }
}

pub fn agg_plus_N_<T>(left: Option<T>, right: T) -> Option<T>
where
    T: Add<T, Output = T> + Copy
{
    match (left, right) {
        (None, _) => Some(right),
        (Some(x), y) => Some(x + y),
    }
}

pub fn agg_plus__N<T>(left: T, right: Option<T>) -> Option<T>
where
    T: Add<T, Output = T> + Copy
{
    match (left, right) {
        (_, None) => Some(left),
        (x, Some(y)) => Some(x + y),
    }
}

pub fn agg_plus__<T>(left: T, right: T) -> T
where
    T: Add<T, Output = T> + Copy
{
    left + right
}

#[inline(always)]
pub fn div_i16_i16(left: i16, right: i16) -> Option<i16>
{
    match right {
        0 => None,
        _ => Some(left / right),
    }
}

#[inline(always)]
pub fn div_i32_i32(left: i32, right: i32) -> Option<i32>
{
    match right {
        0 => None,
        _ => Some(left / right),
    }
}

#[inline(always)]
pub fn div_i64_i64(left: i64, right: i64) -> Option<i64>
{
    match right {
        0 => None,
        _ => Some(left / right),
    }
}

#[inline(always)]
pub fn div_i16N_i16(left: Option<i16>, right: i16) -> Option<i16>
{
    match (left, right, ) {
        (_, 0) => None,
        (Some(l), r, ) => Some(l / r),
        (_, _, ) => None::<i16>,
    }
}

#[inline(always)]
pub fn div_i32N_i32(left: Option<i32>, right: i32) -> Option<i32>
{
    match (left, right, ) {
        (_, 0) => None,
        (Some(l), r, ) => Some(l / r),
        (_, _, ) => None::<i32>,
    }
}

#[inline(always)]
pub fn div_i64N_i64(left: Option<i64>, right: i64) -> Option<i64>
{
    match (left, right, ) {
        (_, 0) => None,
        (Some(l), r, ) => Some(l / r),
        (_, _, ) => None::<i64>,
    }
}

#[inline(always)]
pub fn div_i16_i16N(left: i16, right: Option<i16>) -> Option<i16>
{
    match (left, right) {
        (_, Some(0)) => None,
        (l, Some(r)) => Some(l / r),
        (_, _) => None::<i16>,
    }
}

#[inline(always)]
pub fn div_i32_i32N(left: i32, right: Option<i32>) -> Option<i32>
{
    match (left, right) {
        (_, Some(0)) => None,
        (l, Some(r)) => Some(l / r),
        (_, _) => None::<i32>,
    }
}

#[inline(always)]
pub fn div_i64_i64N(left: i64, right: Option<i64>) -> Option<i64>
{
    match (left, right) {
        (_, Some(0)) => None,
        (l, Some(r)) => Some(l / r),
        (_, _) => None::<i64>,
    }
}

#[inline(always)]
pub fn div_i16N_i16N(left: Option<i16>, right: Option<i16>) -> Option<i16>
{
    match (left, right) {
        (_, Some(0)) => None,
        (Some(l), Some(r)) => Some(l / r),
        (_, _) => None::<i16>,
    }
}

#[inline(always)]
pub fn div_i32N_i32N(left: Option<i32>, right: Option<i32>) -> Option<i32>
{
    match (left, right) {
        (_, Some(0)) => None,
        (Some(l), Some(r)) => Some(l / r),
        (_, _) => None::<i32>,
    }
}

#[inline(always)]
pub fn div_i64N_i64N(left: Option<i64>, right: Option<i64>) -> Option<i64>
{
    match (left, right) {
        (_, Some(0)) => None,
        (Some(l), Some(r)) => Some(l / r),
        (_, _) => None::<i64>,
    }
}

#[inline(always)]
pub fn div_f_f(left: F32, right: F32) -> Option<F32>
{
    Some(F32::new(left.into_inner() / right.into_inner()))
}

#[inline(always)]
pub fn div_f_fN(left: F32, right: Option<F32>) -> Option<F32>
{
    match right {
        None => None,
        Some(right) => Some(F32::new(left.into_inner() / right.into_inner())),
    }
}

#[inline(always)]
pub fn div_fN_f(left: Option<F32>, right: F32) -> Option<F32>
{
    match left {
        None => None,
        Some(left) => Some(F32::new(left.into_inner() / right.into_inner())),
    }
}

#[inline(always)]
pub fn div_fN_fN(left: Option<F32>, right: Option<F32>) -> Option<F32>
{
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(left), Some(right)) => Some(F32::new(left.into_inner() / right.into_inner())),
    }
}

#[inline(always)]
pub fn div_d_d(left: F64, right: F64) -> Option<F64>
{
    Some(F64::new(left.into_inner() / right.into_inner()))
}

#[inline(always)]
pub fn div_d_dN(left: F64, right: Option<F64>) -> Option<F64>
{
    match right {
        None => None,
        Some(right) => Some(F64::new(left.into_inner() / right.into_inner())),
    }
}

#[inline(always)]
pub fn div_dN_d(left: Option<F64>, right: F64) -> Option<F64>
{
    match left {
        None => None,
        Some(left) => Some(F64::new(left.into_inner() / right.into_inner())),
    }
}

#[inline(always)]
pub fn div_dN_dN(left: Option<F64>, right: Option<F64>) -> Option<F64>
{
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(left), Some(right)) => Some(F64::new(left.into_inner() / right.into_inner())),
    }
}

#[inline(always)]
pub fn abs_i16_(left: i16) -> i16
{
    left.abs()
}

#[inline(always)]
pub fn abs_i32_(left: i32) -> i32
{
    left.abs()
}

#[inline(always)]
pub fn abs_i64_(left: i64) -> i64
{
    left.abs()
}

#[inline(always)]
pub fn abs_i16N_(left: Option<i16>) -> Option<i16>
{
    match left {
        Some(l) => Some(l.abs()),
        _ => None::<i16>,
    }
}

#[inline(always)]
pub fn abs_i32N_(left: Option<i32>) -> Option<i32>
{
    match left {
        Some(l) => Some(l.abs()),
        _ => None::<i32>,
    }
}

#[inline(always)]
pub fn abs_i64N_(left: Option<i64>) -> Option<i64>
{
    match left {
        Some(l) => Some(l.abs()),
        _ => None::<i64>,
    }
}

#[inline(always)]
pub fn abs_f_(left: F32) -> F32
{
    left.abs()
}

#[inline(always)]
pub fn abs_fN_(left: Option<F32>) -> Option<F32>
{
    match left {
        Some(left) => Some(left.abs()),
        None => None,
    }
}

#[inline(always)]
pub fn abs_d(left: F64) -> F64
{
    left.abs()
}

#[inline(always)]
pub fn abs_dN(left: Option<F64>) -> Option<F64>
{
    match left {
        Some(left) => Some(left.abs()),
        None => None,
    }
}

#[inline(always)]
pub fn is_true_b_(left: bool) -> bool
{
    left
}

#[inline(always)]
pub fn is_true_bN_(left: Option<bool>) -> bool
{
    match left {
        Some(true) => true,
        _ => false,
    }
}

#[inline(always)]
pub fn is_false_b_(left: bool) -> bool
{
    !left
}

#[inline(always)]
pub fn is_false_bN_(left: Option<bool>) -> bool
{
    match left {
        Some(false) => true,
        _ => false,
    }
}

#[inline(always)]
pub fn is_not_true_b_(left: bool) -> bool
{
    !left
}

#[inline(always)]
pub fn is_not_true_bN_(left: Option<bool>) -> bool
{
    match left {
        Some(true) => false,
        Some(false) => true,
        _ => true,
    }
}

#[inline(always)]
pub fn is_not_false_b_(left: bool) -> bool
{
    left
}

#[inline(always)]
pub fn is_not_false_bN_(left: Option<bool>) -> bool
{
    match left {
        Some(true) => true,
        Some(false) => false,
        _ => true,
    }
}

#[inline(always)]
pub fn is_distinct__<T>(left: T, right: T) -> bool
    where T: Eq
{
    left != right
}

#[inline(always)]
pub fn is_distinct_N_N<T>(left: Option<T>, right: Option<T>) -> bool
    where T: Eq
{
    match (left, right) {
        (Some(a), Some(b)) => a != b,
        (None, None) => false,
        _ => true,
    }
}

#[inline(always)]
pub fn is_distinct__N<T>(left: T, right: Option<T>) -> bool
    where T: Eq
{
    match right {
        Some(b) => left != b,
        None => true,
    }
}

#[inline(always)]
pub fn is_distinct_N_<T>(left: Option<T>, right: T) -> bool
    where T: Eq
{
    match left {
        Some(a) => a != right,
        None => true,
    }
}


pub fn weighted_push<T, W>(vec: &mut Vec<T>, value: &T, weight: W)
where
    W: ZRingValue,
    T: Clone,
{
    let mut w = weight;
    let negone = W::one().neg();
    while w != W::zero() {
        vec.push(value.clone());
        w = w.add_by_ref(&negone);
    }
}

pub fn st_distance__(left: GeoPoint, right: GeoPoint) -> F64
{
    left.distance(&right)
}

pub fn st_distance_N_(left: Option<GeoPoint>, right: GeoPoint) -> Option<F64>
{
    match left {
        None => None,
        Some(x) => Some(st_distance__(x, right)),
    }
}

pub fn st_distance__N(left: GeoPoint, right: Option<GeoPoint>) -> Option<F64>
{
    match right {
        None => None,
        Some(x) => Some(st_distance__(left, x)),
    }
}

pub fn st_distance_N_N(left: Option<GeoPoint>, right: Option<GeoPoint>) -> Option<F64>
{
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(x), Some(y)) => Some(st_distance__(x, y)),
    }
}

pub fn times_ShortInterval_i64(left: ShortInterval, right: i64) -> ShortInterval {
    left * right
}

pub fn times_ShortIntervalN_i64(left: Option<ShortInterval>, right: i64) -> Option<ShortInterval> {
    match left {
        None => None,
        Some(x) => Some(x * right),
    }
}

pub fn times_ShortInterval_i64N(left: ShortInterval, right: Option<i64>) -> Option<ShortInterval> {
    match right {
        None => None,
        Some(x) => Some(left * x),
    }
}

pub fn times_ShortIntervalN_i64N(left: Option<ShortInterval>, right: Option<i64>) -> Option<ShortInterval> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(x), Some(y)) => Some(x * y),
    }
}

pub fn plus_Timestamp_ShortInterval(left: Timestamp, right: ShortInterval) -> Timestamp {
    Timestamp::from(left.add(right.milliseconds()))
}

pub fn minus_Timestamp_Timestamp_ShortInterval(left: Timestamp, right: Timestamp) -> ShortInterval {
    ShortInterval::from(left.milliseconds() - right.milliseconds())
}

pub fn minus_Timestamp_Timestamp_LongInterval(left: Timestamp, right: Timestamp) -> LongInterval {
    let ldate = left.toDateTime();
    let rdate = right.toDateTime();
    let ly = ldate.year();
    let lm = ldate.month() as i32;
    let ld = ldate.day() as i32;
    let lt = ldate.time();

    let ry = rdate.year();
    let mut rm = rdate.month() as i32;
    let rd = rdate.day() as i32;
    let rt = rdate.time();
    if (ld < rd) || ((ld == rd) && lt < rt) {
        // the full month is not yet elapsed
        rm = rm + 1;
    }
    LongInterval::from((ly - ry) * 12 + lm - rm)
}

pub fn minus_date_date_LongInterval(left: Date, right: Date) -> LongInterval {
    let ld = Utc.timestamp_opt(left.days() as i64 * 86400, 0).single().unwrap();
    let rd = Utc.timestamp_opt(right.days() as i64 * 86400, 0).single().unwrap();
    let ly = ld.year();
    let lm = ld.month() as i32;
    let ry = rd.year();
    let rm = rd.month() as i32;
    LongInterval::from((ly - ry) * 12 + lm - rm)
}

pub fn minus_dateN_date_LongInterval(left: Option<Date>, right: Date) -> Option<LongInterval> {
    match left {
        None => None,
        Some(x) => Some(LongInterval::new(x.days() - right.days())),
    }
}

pub fn minus_date_dateN_LongInterval(left: Date, right: Option<Date>) -> Option<LongInterval> {
    match right {
        None => None,
        Some(x) => Some(LongInterval::new(left.days() - x.days())),
    }
}

pub fn minus_dateN_dateN_LongInterval(left: Option<Date>, right: Option<Date>) -> Option<LongInterval> {
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(x), Some(y)) => Some(LongInterval::new(x.days() - y.days())),
    }
}

/***** decimals ******/

#[inline(always)]
pub fn times_decimal_decimal(left: Decimal, right: Decimal) -> Decimal
{
    left * right
}

#[inline(always)]
pub fn times_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal>
{
    match left {
        Some(l) => Some(l * right),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn times_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal>
{
    match right {
        Some(r) => Some(left * r),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn times_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l * r),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn plus_decimal_decimal(left: Decimal, right: Decimal) -> Decimal
{
    left + right
}

#[inline(always)]
pub fn plus_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal>
{
    match left {
        Some(l) => Some(l + right),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn plus_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal>
{
    match right {
        Some(r) => Some(left + r),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn plus_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l + r),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn minus_decimal_decimal(left: Decimal, right: Decimal) -> Decimal
{
    left - right
}

#[inline(always)]
pub fn minus_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal>
{
    match left {
        Some(l) => Some(l - right),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn minus_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal>
{
    match right {
        Some(r) => Some(left - r),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn minus_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l - r),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn mod_decimal_decimal(left: Decimal, right: Decimal) -> Decimal
{
    left % right
}

#[inline(always)]
pub fn mod_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<Decimal>
{
    match left {
        Some(l) => Some(l % right),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn mod_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<Decimal>
{
    match right {
        Some(r) => Some(left % r),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn mod_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l % r),
        _ => None::<Decimal>,
    }
}

#[inline(always)]
pub fn lt_decimal_decimal(left: Decimal, right: Decimal) -> bool
{
    left < right
}

#[inline(always)]
pub fn lt_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<bool>
{
    match left {
        Some(l) => Some(l < right),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn lt_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<bool>
{
    match right {
        Some(r) => Some(left < r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn lt_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l < r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn eq_decimal_decimal(left: Decimal, right: Decimal) -> bool
{
    left == right
}

#[inline(always)]
pub fn eq_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<bool>
{
    match left {
        Some(l) => Some(l == right),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn eq_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<bool>
{
    match right {
        Some(r) => Some(left == r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn eq_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l == r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn gt_decimal_decimal(left: Decimal, right: Decimal) -> bool
{
    left > right
}

#[inline(always)]
pub fn gt_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<bool>
{
    match left {
        Some(l) => Some(l > right),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn gt_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<bool>
{
    match right {
        Some(r) => Some(left > r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn gt_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l > r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn gte_decimal_decimal(left: Decimal, right: Decimal) -> bool
{
    left >= right
}

#[inline(always)]
pub fn gte_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<bool>
{
    match left {
        Some(l) => Some(l >= right),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn gte_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<bool>
{
    match right {
        Some(r) => Some(left >= r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn gte_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l >= r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn neq_decimal_decimal(left: Decimal, right: Decimal) -> bool
{
    left != right
}

#[inline(always)]
pub fn neq_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<bool>
{
    match left {
        Some(l) => Some(l != right),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn neq_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<bool>
{
    match right {
        Some(r) => Some(left != r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn neq_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l != r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn lte_decimal_decimal(left: Decimal, right: Decimal) -> bool
{
    left <= right
}

#[inline(always)]
pub fn lte_decimalN_decimal(left: Option<Decimal>, right: Decimal) -> Option<bool>
{
    match left {
        Some(l) => Some(l <= right),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn lte_decimal_decimalN(left: Decimal, right: Option<Decimal>) -> Option<bool>
{
    match right {
        Some(r) => Some(left <= r),
        _ => None::<bool>,
    }
}

#[inline(always)]
pub fn lte_decimalN_decimalN(left: Option<Decimal>, right: Option<Decimal>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l <= r),
        _ => None::<bool>,
    }
}
