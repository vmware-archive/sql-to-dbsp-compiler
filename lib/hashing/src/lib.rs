use dbsp::{
    trace::{
        ord::OrdZSet,
        BatchReader,
        cursor::Cursor,
    },
    algebra::ZRingValue,
};
use core::{
    cmp::Ordering,
    fmt::Debug,
};
use sqlvalue::*;
use md5;

#[derive(Eq, PartialEq)]
pub enum SortOrder {
    None,
    Row,
    Value,
}

fn compare<T>(left: &Vec<T>, right: &Vec<T>) -> Ordering
where
    T: Ord
{
    let llen = left.len();
    let rlen = right.len();
    let min;
    if llen < rlen {
        min = llen;
    } else {
        min = rlen;
    }
    for i in 0..min {
        let cmp = left[i].cmp(&right[i]);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    return llen.cmp(&rlen)
}

/// This function mimics the md5 checksum computation from SqlLogicTest
pub fn hash<K, W>(set: &OrdZSet<K, W>, format: String, order: SortOrder) -> String
where
    K: Ord + Clone + Debug + 'static + ToSqlRow,
    W: ZRingValue,
{
    let mut vec = Vec::<Vec::<String>>::new();
    let mut cursor = set.cursor();
    while cursor.key_valid() {
        let mut w = cursor.weight();
        if !w.ge0() {
            panic!("Negative weight in output set!");
        }
        while !w.le0() {
            let row_vec = cursor.key().to_row().to_slt_strings(&format);
            if order == SortOrder::Row {
                vec.push(row_vec);
            } else if order == SortOrder::Value {
                for r in row_vec {
                    vec.push(vec!(r))
                }
            } else {
                panic!("Didn't expect sort order 'None'");
            }
            w = w.add(W::neg(W::one()));
        }
        cursor.step_key();
    }
    vec.sort_by(&compare);
    for row in &vec {
        for elem in row {
            print!("{},", elem);
        }
        println!();
    }

    let mut builder = String::default();
    for row in vec {
        for col in row {
            builder = builder + &col + "\n"
        }
    }
    let digest = md5::compute(builder);
    return format!("{:x}", digest)
}
