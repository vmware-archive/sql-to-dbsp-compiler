#![allow(non_snake_case)]

use std::ops::Add;
use dbsp::algebra::{F32, F64, ZRingValue};

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
        (Some(x), y) => Some(x + y)
    }
}

pub fn agg_plus__N<T>(left: T, right: Option<T>) -> Option<T>
where
    T: Add<T, Output = T> + Copy
{
    match (left, right) {
        (_, None) => Some(left),
        (x, Some(y)) => Some(x + y)
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

pub fn st_distance__(left: (F64, F64), right: (F64, F64)) -> F64
{
    let d1 = left.0 - right.0;
    let d2 = left.1 - right.1;
    F64::new((d1 * d1 + d2 * d2).into_inner().sqrt())
}

pub fn st_distance_N_(left: Option<(F64, F64)>, right: (F64, F64)) -> Option<F64>
{
    match left {
        None => None,
        Some(x) => Some(st_distance__(x, right)),
    }
}

pub fn st_distance__N(left: (F64, F64), right: Option<(F64, F64)>) -> Option<F64>
{
    match right {
        None => None,
        Some(x) => Some(st_distance__(left, x)),
    }
}

pub fn st_distance_N_N(left: Option<(F64, F64)>, right: Option<(F64, F64)>) -> Option<F64>
{
    match (left, right) {
        (None, _) => None,
        (_, None) => None,
        (Some(x), Some(y)) => Some(st_distance__(x, y)),
    }
}
