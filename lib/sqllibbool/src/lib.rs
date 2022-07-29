#![allow(non_snake_case)]

pub fn or_b_b(left: bool, right: bool) -> bool
{
    left || right
}

pub fn or_bN_b(left: Option<bool>, right: bool) -> Option<bool>
{
    match (left, right) {
        (Some(l), r) => Some(l || r),
        (_, true) => Some(true),
        (_, _) => None::<bool>,
    }
}

pub fn or_b_bN(left: bool, right: Option<bool>) -> Option<bool>
{
    match (left, right) {
        (l, Some(r)) => Some(l || r),
        (true, _) => Some(true),
        (_, _) => None::<bool>,
    }
}

pub fn or_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l || r),
        (Some(true), _) => Some(true),
        (_, Some(true)) => Some(true),
        (_, _) => None::<bool>,
    }
}

pub fn and_b_b(left: bool, right: bool) -> bool
{
    left && right
}

pub fn and_bN_b(left: Option<bool>, right: bool) -> Option<bool>
{
    match (left, right) {
        (Some(l), r) => Some(l && r),
        (_, false) => Some(false),
        (_, _) => None::<bool>,
    }
}

pub fn and_b_bN(left: bool, right: Option<bool>) -> Option<bool>
{
    match (left, right) {
        (l, Some(r)) => Some(l && r),
        (false, _) => Some(false),
        (_, _) => None::<bool>,
    }
}

pub fn and_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool>
{
    match (left, right) {
        (Some(l), Some(r)) => Some(l && r),
        (Some(false), _) => Some(false),
        (_, Some(false)) => Some(false),
        (_, _) => None::<bool>,
    }
}
