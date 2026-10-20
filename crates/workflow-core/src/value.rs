use std::collections::BTreeMap;

/// Minimal JSON-like value. Avoids serde_json so the crate builds offline.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
}

impl Default for Value {
    fn default() -> Self {
        Value::Object(BTreeMap::new())
    }
}

impl Value {
    pub fn object() -> Self {
        Value::Object(BTreeMap::new())
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Object(m) => m.get(key),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Object(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Number(n) if n.fract() == 0.0 => Some(*n as i64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn insert(&mut self, k: impl Into<String>, v: Value) {
        if let Value::Object(m) = self {
            m.insert(k.into(), v);
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Number(v as f64)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Number(v as f64)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Number(v)
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

pub fn merge(dst: &mut Value, src: &Value) {
    match (dst, src) {
        (Value::Object(d), Value::Object(s)) => {
            for (k, v) in s {
                d.insert(k.clone(), v.clone());
            }
        }
        (dst, src) if !src.is_null() => *dst = src.clone(),
        _ => {}
    }
}

pub fn obj<const N: usize>(pairs: [(&str, Value); N]) -> Value {
    let mut m = BTreeMap::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v);
    }
    Value::Object(m)
}

impl From<Option<String>> for Value {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(s) => Value::String(s),
            None => Value::Null,
        }
    }
}

impl From<&String> for Value {
    fn from(v: &String) -> Self {
        Value::String(v.clone())
    }
}

impl From<&Option<String>> for Value {
    fn from(v: &Option<String>) -> Self {
        Value::from(v.clone())
    }
}

#[macro_export]
macro_rules! json {
    ({}) => { $crate::value::Value::object() };
    ({ $($key:literal : $val:expr),+ $(,)? }) => {
        $crate::value::obj([ $(($key, $crate::value::Value::from(($val).clone()))),+ ])
    };
}

impl From<Option<&str>> for Value {
    fn from(v: Option<&str>) -> Self {
        match v {
            Some(s) => Value::String(s.to_string()),
            None => Value::Null,
        }
    }
}
impl From<Vec<String>> for Value {
    fn from(v: Vec<String>) -> Self {
        Value::Array(v.into_iter().map(Value::String).collect())
    }
}
impl From<usize> for Value {
    fn from(v: usize) -> Self {
        Value::Number(v as f64)
    }
}
