// I cannot use the standard geopoint object because it doesn't implement Ord

use dbsp::algebra::F64;
use size_of::*;
use ::serde::{Deserialize,Serialize};
use geo::EuclideanDistance;
use geo::Point;

#[derive(Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, SizeOf, Serialize, Deserialize, Debug)]
pub struct GeoPoint(F64, F64);

impl GeoPoint {
    pub fn new<T, S>(left: T, right: S) -> Self
    where
        F64: From<T>,
        F64: From<S>,
    {
        Self(F64::from(left), F64::from(right))
    }

    pub fn to_point(self: &Self) -> Point
    {
        Point::new(self.0.into_inner(), self.1.into_inner())
    }

    pub fn distance(self: &Self, other: &GeoPoint) -> F64
    {
        let left = self.to_point();
        let right = other.to_point();
        F64::from(left.euclidean_distance(&right))
    }
}
