#![allow(non_snake_case)]

use std::ops::Add;

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
