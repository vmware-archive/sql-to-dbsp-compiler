// SqlValue is a dynamically-typed object that holds a value that at
// runtime can appear in a SQL program.  These values are not used
// during computations, but they are used to display values.  The
// Tuple* types are used for computations, and they are converted
// to SqlRow objects when they need to be displayed.

use ordered_float::OrderedFloat;

pub enum SqlValue {
    Int(i32),
    Str(String),
    Flt(f32),
    Dbl(f64),
    Bool(bool),

    OptInt(Option<i32>),
    OptStr(Option<String>),
    OptFlt(Option<f32>),
    OptDbl(Option<f64>),
    OptBool(Option<bool>),
}

impl From<i32> for SqlValue {
    fn from(value: i32) -> Self {
        SqlValue::Int(value)
    }
}

impl From<f32> for SqlValue {
    fn from(value: f32) -> Self {
        SqlValue::Flt(value)
    }
}

impl From<f64> for SqlValue {
    fn from(value: f64) -> Self {
        SqlValue::Dbl(value)
    }
}

impl From<OrderedFloat<f32>> for SqlValue {
    fn from(value: OrderedFloat<f32>) -> Self {
        SqlValue::Flt(value.into())
    }
}

impl From<OrderedFloat<f64>> for SqlValue {
    fn from(value: OrderedFloat<f64>) -> Self {
        SqlValue::Dbl(value.into())
    }
}

impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        SqlValue::Str(value)
    }
}

impl From<Option<i32>> for SqlValue {
    fn from(value: Option<i32>) -> Self {
        SqlValue::OptInt(value)
    }
}

impl From<Option<f32>> for SqlValue {
    fn from(value: Option<f32>) -> Self {
        SqlValue::OptFlt(value)
    }
}

impl From<Option<f64>> for SqlValue {
    fn from(value: Option<f64>) -> Self {
        SqlValue::OptDbl(value)
    }
}

impl From<Option<OrderedFloat<f32>>> for SqlValue {
    fn from(value: Option<OrderedFloat<f32>>) -> Self {
        match value {
            None => SqlValue::OptFlt(None),
            Some(OrderedFloat(x)) => SqlValue::OptFlt(Some(x)),
        }
    }
}

impl From<Option<OrderedFloat<f64>>> for SqlValue {
    fn from(value: Option<OrderedFloat<f64>>) -> Self {
        match value {
            None => SqlValue::OptDbl(None),
            Some(OrderedFloat(x)) => SqlValue::OptDbl(Some(x)),
        }
    }
}

impl From<Option<String>> for SqlValue {
    fn from(value: Option<String>) -> Self {
        SqlValue::OptStr(value)
    }
}

pub struct SqlRow {
    values: Vec<SqlValue>,
}

pub trait ToSqlRow {
    fn to_row(&self) -> SqlRow;
}

impl SqlRow {
    pub fn new() -> Self {
        SqlRow { values: Vec::default() }
    }

    // Output the SqlRow value in the format expected by the tests
    // in SqlLogicTest.
    // 'format' is a string with characters I, R, or T, standing
    // respectively for integers, real, or text.
    pub fn to_slt_strings(self, format: &String) -> Vec<String> {
        if self.values.len() != format.len() {
            panic!("Mismatched format {} vs len {}", format.len(), self.values.len())
        }
        let mut result = Vec::<String>::new();
        for elem in self.values.iter().zip(format.chars()) {
            result.push(elem.0.format_slt(&elem.1));
        }
        result
    }
}

impl SqlRow {
    pub fn push(self: &mut Self, value: SqlValue) {
        self.values.push(value)
    }
}

impl Default for SqlRow {
    fn default() -> Self {
        SqlRow { values: Vec::default() }
    }
}

pub trait SqlLogicTestFormat {
    fn format_slt(&self, arg: &char) -> String;
}


fn print_string(s: &String) -> String {
    if s == "" {
        return String::from("(empty)")
    }
    let mut result = String::new();
    for mut c in s.chars() {
        if c < ' ' || c > '~' {
            c = '@';
        }
        result.push(c);
    }
    result
}

// Format a SqlValue according to SqlLogicTest rules
// the arg is one character of the form I - i32, R - f32, or T - String.
impl SqlLogicTestFormat for SqlValue {
    fn format_slt(self: &Self, arg: &char) -> String {
        match (self, arg) {
            (SqlValue::Int(x), _) => format!("{}", x),
            (SqlValue::OptInt(None), _) => String::from("NULL"),
            (SqlValue::OptInt(Some(x)), _) => format!("{}", x),

            (SqlValue::OptFlt(None), _) => String::from("NULL"),
            (SqlValue::Flt(x), 'I') => format!("{}", *x as i32),
            (SqlValue::OptFlt(Some(x)), 'I') => format!("{}", *x as i32),
            (SqlValue::Flt(x), _) => format!("{:.3}", x),
            (SqlValue::OptFlt(Some(x)), _) => format!("{:.3}", x),

            (SqlValue::OptDbl(None), _) => String::from("NULL"),
            (SqlValue::Dbl(x), 'I') => format!("{}", *x as i32),
            (SqlValue::OptDbl(Some(x)), 'I') => format!("{}", *x as i32),
            (SqlValue::Dbl(x), _) => format!("{:.3}", x),
            (SqlValue::OptDbl(Some(x)), _) => format!("{:.3}", x),

            (SqlValue::Str(x), 'T') => print_string(x),
            (SqlValue::OptStr(None), 'T') => String::from("NULL"),
            (SqlValue::OptStr(Some(x)), 'T') => print_string(x),

            (SqlValue::OptBool(None), _) => String::from("NULL"),
            (SqlValue::Bool(b), _) => format!("{}", b),
            (SqlValue::OptBool(Some(b)), _) => format!("{}", b),
            _ => panic!("Unexpected combination"),
        }
    }
}