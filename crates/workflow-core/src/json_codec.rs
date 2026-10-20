//! Minimal JSON codec for `Value` and process graphs.
//! Used to read/write Neon `jsonb` columns without a serde dependency.

use std::collections::BTreeMap;

use crate::types::*;
use crate::value::Value;

pub fn stringify(v: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, v);
    out
}

fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => {
            if n.fract() == 0.0 && n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                out.push_str(&format!("{}", *n as i64));
            } else {
                out.push_str(&format!("{n}"));
            }
        }
        Value::String(s) => write_str(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (k, val)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_str(out, k);
                out.push(':');
                write_value(out, val);
            }
            out.push('}');
        }
    }
}

fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // Any other control character is illegal raw in JSON (Postgres' jsonb refuses it).
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

pub fn parse(s: &str) -> Result<Value, String> {
    let mut p = Parser {
        b: s.as_bytes(),
        i: 0,
    };
    p.skip_ws();
    let v = p.value()?;
    p.skip_ws();
    if p.i != p.b.len() {
        return Err("trailing json".into());
    }
    Ok(v)
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }
    fn bump(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.i += 1;
        Some(c)
    }
    fn value(&mut self) -> Result<Value, String> {
        self.skip_ws();
        match self.peek() {
            Some(b'n') => self.ident(b"null", Value::Null),
            Some(b't') => self.ident(b"true", Value::Bool(true)),
            Some(b'f') => self.ident(b"false", Value::Bool(false)),
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b'[') => self.array(),
            Some(b'{') => self.object(),
            Some(b'-') | Some(b'0'..=b'9') => self.number(),
            other => Err(format!("unexpected {:?}", other)),
        }
    }
    fn ident(&mut self, lit: &[u8], v: Value) -> Result<Value, String> {
        if self.b[self.i..].starts_with(lit) {
            self.i += lit.len();
            Ok(v)
        } else {
            Err("bad ident".into())
        }
    }
    fn string(&mut self) -> Result<String, String> {
        if self.bump() != Some(b'"') {
            return Err("string".into());
        }
        // BYTES, decoded once at the end. This pushed each byte as a `char` (`c as char`), which reads a multi-byte
        // UTF-8 character as that many Latin-1 characters: every "—" came back as "â" plus two invisible ones, so
        // every non-ASCII value the engine read back from storage (payloads, variables, event data — names with
        // accents among them) was corrupted.
        let mut out: Vec<u8> = Vec::new();
        loop {
            match self.bump() {
                None => return Err("unterminated string".into()),
                Some(b'"') => {
                    return String::from_utf8(out)
                        .map_err(|_| "string is not valid UTF-8".to_string())
                }
                Some(b'\\') => match self.bump() {
                    Some(b'"') => out.push(b'"'),
                    Some(b'\\') => out.push(b'\\'),
                    Some(b'/') => out.push(b'/'),
                    Some(b'b') => out.push(0x08),
                    Some(b'f') => out.push(0x0c),
                    Some(b'n') => out.push(b'\n'),
                    Some(b'r') => out.push(b'\r'),
                    Some(b't') => out.push(b'\t'),
                    Some(b'u') => {
                        let first = self.hex4()?;
                        // A code point above U+FFFF arrives as a surrogate PAIR of \u escapes.
                        let ch = if (0xD800..0xDC00).contains(&first)
                            && self.b[self.i..].starts_with(b"\\u")
                        {
                            self.i += 2;
                            let low = self.hex4()?;
                            if (0xDC00..0xE000).contains(&low) {
                                char::from_u32(0x10000 + ((first - 0xD800) << 10) + (low - 0xDC00))
                            } else {
                                None
                            }
                        } else {
                            char::from_u32(first)
                        };
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(
                            ch.unwrap_or('\u{FFFD}').encode_utf8(&mut buf).as_bytes(),
                        );
                    }
                    Some(other) => {
                        out.push(b'\\');
                        out.push(other);
                    }
                    None => return Err("unterminated string".into()),
                },
                Some(c) => out.push(c),
            }
        }
    }
    fn hex4(&mut self) -> Result<u32, String> {
        let digits = self.b.get(self.i..self.i + 4).ok_or("uhex")?;
        let hex = std::str::from_utf8(digits).map_err(|_| "uhex")?;
        let cp = u32::from_str_radix(hex, 16).map_err(|_| "uhex")?;
        self.i += 4;
        Ok(cp)
    }
    fn number(&mut self) -> Result<Value, String> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.i += 1;
        }
        if self.peek() == Some(b'.') {
            self.i += 1;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.i += 1;
            }
        }
        let s = std::str::from_utf8(&self.b[start..self.i]).unwrap();
        s.parse::<f64>()
            .map(Value::Number)
            .map_err(|_| "number".into())
    }
    fn array(&mut self) -> Result<Value, String> {
        self.bump();
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.bump();
            return Ok(Value::Array(items));
        }
        loop {
            items.push(self.value()?);
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b']') => return Ok(Value::Array(items)),
                _ => return Err("array".into()),
            }
        }
    }
    fn object(&mut self) -> Result<Value, String> {
        self.bump();
        let mut map = BTreeMap::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(Value::Object(map));
        }
        loop {
            self.skip_ws();
            let k = self.string()?;
            self.skip_ws();
            if self.bump() != Some(b':') {
                return Err("colon".into());
            }
            let v = self.value()?;
            map.insert(k, v);
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b'}') => return Ok(Value::Object(map)),
                _ => return Err("object".into()),
            }
        }
    }
}

pub fn graph_to_json(graph: &ProcessGraph) -> String {
    stringify(&graph_to_value(graph))
}

pub fn graph_from_json(s: &str) -> Result<ProcessGraph, String> {
    graph_from_value(&parse(s)?)
}

fn graph_to_value(graph: &ProcessGraph) -> Value {
    let mut nodes = BTreeMap::new();
    for (k, n) in &graph.nodes {
        nodes.insert(k.clone(), node_to_value(n));
    }
    let mut m = BTreeMap::new();
    m.insert("nodes".into(), Value::Object(nodes));
    m.insert(
        "startNodeId".into(),
        Value::from(graph.start_node_id.as_str()),
    );
    if let Some(order) = &graph.display_order {
        m.insert(
            "displayOrder".into(),
            Value::Array(order.iter().map(|s| Value::from(s.as_str())).collect()),
        );
    }
    Value::Object(m)
}

fn node_to_value(n: &NodeDefinition) -> Value {
    let mut m = BTreeMap::new();
    m.insert("id".into(), Value::from(n.id.as_str()));
    m.insert("type".into(), Value::from(n.node_type.as_str()));
    if let Some(v) = &n.name {
        m.insert("name".into(), Value::from(v.as_str()));
    }
    if let Some(v) = &n.description {
        m.insert("description".into(), Value::from(v.as_str()));
    }
    if let Some(ts) = &n.transitions {
        m.insert(
            "transitions".into(),
            Value::Array(
                ts.iter()
                    .map(|t| {
                        let mut tm = BTreeMap::new();
                        tm.insert("name".into(), Value::from(t.name.as_str()));
                        tm.insert("to".into(), Value::from(t.to.as_str()));
                        if let Some(c) = &t.condition {
                            tm.insert("condition".into(), Value::from(c.as_str()));
                        }
                        if let Some(r) = t.required {
                            tm.insert("required".into(), Value::Bool(r));
                        }
                        Value::Object(tm)
                    })
                    .collect(),
            ),
        );
    }
    if let Some(v) = &n.form_key {
        m.insert("formKey".into(), Value::from(v.as_str()));
    }
    if let Some(v) = &n.candidate_groups {
        m.insert(
            "candidateGroups".into(),
            Value::Array(v.iter().map(|s| Value::from(s.as_str())).collect()),
        );
    }
    if let Some(v) = n.priority {
        m.insert("priority".into(), Value::from(v));
    }
    if let Some(ds) = &n.decisions {
        m.insert(
            "decisions".into(),
            Value::Array(
                ds.iter()
                    .map(|d| {
                        let mut dm = BTreeMap::new();
                        dm.insert("condition".into(), Value::from(d.condition.as_str()));
                        dm.insert("transition".into(), Value::from(d.transition.as_str()));
                        Value::Object(dm)
                    })
                    .collect(),
            ),
        );
    }
    if let Some(v) = &n.command_type {
        m.insert("commandType".into(), Value::from(v.as_str()));
    }
    if let Some(v) = &n.transition {
        m.insert("transition".into(), Value::from(v.as_str()));
    }
    if let Some(v) = n.refresh_facts {
        m.insert("refreshFacts".into(), Value::Bool(v));
    }
    if let Some(v) = &n.count_variable {
        m.insert("countVariable".into(), Value::from(v.as_str()));
    }
    if let Some(v) = &n.plan_variable {
        m.insert("planVariable".into(), Value::from(v.as_str()));
    }
    if let Some(v) = &n.branch_node {
        m.insert("branchNode".into(), Value::from(v.as_str()));
    }
    if let Some(v) = &n.join {
        m.insert("join".into(), Value::from(v.as_str()));
    }
    if let Some(v) = n.minimum {
        m.insert("minimum".into(), Value::from(v));
    }
    if let Some(v) = n.maximum {
        m.insert("maximum".into(), Value::from(v));
    }
    if let Some(o) = n.outcome {
        m.insert(
            "outcome".into(),
            Value::from(match o {
                ProcessOutcome::Completed => "completed",
                ProcessOutcome::Cancelled => "cancelled",
                ProcessOutcome::Failed => "failed",
                ProcessOutcome::Conflict => "conflict",
            }),
        );
    }
    if let Some(timer) = &n.timer {
        let mut tm = BTreeMap::new();
        if let Some(v) = &timer.due_at {
            tm.insert("dueAt".into(), Value::from(v.as_str()));
        }
        if let Some(v) = &timer.due_at_variable {
            tm.insert("dueAtVariable".into(), Value::from(v.as_str()));
        }
        if let Some(v) = &timer.transition {
            tm.insert("transition".into(), Value::from(v.as_str()));
        }
        m.insert("timer".into(), Value::Object(tm));
    }
    if let Some(v) = &n.input_mappings {
        m.insert("inputMappings".into(), v.clone());
    }
    Value::Object(m)
}

fn graph_from_value(v: &Value) -> Result<ProcessGraph, String> {
    let obj = v.as_object().ok_or("graph object")?;
    let start = obj
        .get("startNodeId")
        .and_then(|x| x.as_str())
        .ok_or("startNodeId")?
        .to_string();
    let mut nodes = BTreeMap::new();
    if let Some(Value::Object(nm)) = obj.get("nodes") {
        for (k, nv) in nm {
            nodes.insert(k.clone(), node_from_value(nv)?);
        }
    }
    let display_order = obj.get("displayOrder").and_then(|x| x.as_array()).map(|a| {
        a.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    });
    Ok(ProcessGraph {
        nodes,
        start_node_id: start,
        display_order,
    })
}

fn node_from_value(v: &Value) -> Result<NodeDefinition, String> {
    let o = v.as_object().ok_or("node")?;
    let strf = |k: &str| o.get(k).and_then(|x| x.as_str()).map(|s| s.to_string());
    let mut node = NodeDefinition {
        id: strf("id").unwrap_or_default(),
        node_type: strf("type").unwrap_or_default(),
        name: strf("name"),
        description: strf("description"),
        form_key: strf("formKey"),
        command_type: strf("commandType"),
        transition: strf("transition"),
        count_variable: strf("countVariable"),
        plan_variable: strf("planVariable"),
        branch_node: strf("branchNode"),
        join: strf("join"),
        priority: o.get("priority").and_then(|x| x.as_i64()).map(|n| n as i32),
        refresh_facts: o.get("refreshFacts").and_then(|x| match x {
            Value::Bool(b) => Some(*b),
            _ => None,
        }),
        minimum: o.get("minimum").and_then(|x| x.as_i64()).map(|n| n as i32),
        maximum: o.get("maximum").and_then(|x| x.as_i64()).map(|n| n as i32),
        input_mappings: o.get("inputMappings").cloned(),
        ..Default::default()
    };
    if let Some(Value::Array(arr)) = o.get("candidateGroups") {
        node.candidate_groups = Some(
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect(),
        );
    }
    if let Some(Value::Array(arr)) = o.get("transitions") {
        node.transitions = Some(
            arr.iter()
                .filter_map(|t| {
                    let to = t.as_object()?;
                    Some(TransitionDefinition {
                        name: to.get("name")?.as_str()?.to_string(),
                        to: to.get("to")?.as_str()?.to_string(),
                        condition: to
                            .get("condition")
                            .and_then(|c| c.as_str())
                            .map(|s| s.to_string()),
                        required: to.get("required").and_then(|c| match c {
                            Value::Bool(b) => Some(*b),
                            _ => None,
                        }),
                    })
                })
                .collect(),
        );
    }
    if let Some(Value::Array(arr)) = o.get("decisions") {
        node.decisions = Some(
            arr.iter()
                .filter_map(|d| {
                    let o = d.as_object()?;
                    Some(DecisionArm {
                        condition: o.get("condition")?.as_str()?.to_string(),
                        transition: o.get("transition")?.as_str()?.to_string(),
                    })
                })
                .collect(),
        );
    }
    if let Some(s) = strf("outcome") {
        node.outcome = match s.as_str() {
            "completed" => Some(ProcessOutcome::Completed),
            "cancelled" => Some(ProcessOutcome::Cancelled),
            "failed" => Some(ProcessOutcome::Failed),
            "conflict" => Some(ProcessOutcome::Conflict),
            _ => None,
        };
    }
    if let Some(Value::Object(tm)) = o.get("timer") {
        node.timer = Some(TimerSpec {
            due_at: tm
                .get("dueAt")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string()),
            due_at_variable: tm
                .get("dueAtVariable")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string()),
            transition: tm
                .get("transition")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string()),
        });
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn roundtrip_object() {
        let v = crate::value::obj([("approved", Value::Bool(true)), ("n", Value::from(3))]);
        let s = stringify(&v);
        assert_eq!(parse(&s).unwrap(), v);
    }

    #[test]
    fn graph_json_uses_ts_field_names() {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "start".into(),
            NodeDefinition {
                id: "start".into(),
                node_type: "start".into(),
                transitions: Some(vec![TransitionDefinition {
                    name: "go".into(),
                    to: "end".into(),
                    condition: None,
                    required: Some(true),
                }]),
                ..Default::default()
            },
        );
        let g = ProcessGraph {
            nodes,
            start_node_id: "start".into(),
            display_order: Some(vec!["start".into()]),
        };
        let raw = graph_to_json(&g);
        assert!(raw.contains("\"startNodeId\""));
        assert!(raw.contains("\"displayOrder\""));
        let back = graph_from_json(&raw).unwrap();
        assert_eq!(back.start_node_id, "start");
        assert_eq!(back.nodes["start"].node_type, "start");
        assert_eq!(
            back.nodes["start"].transitions.as_ref().unwrap()[0].to,
            "end"
        );
    }

    /// The corruption that motivated the byte-wise decoder: a multi-byte character read back as Latin-1 mojibake.
    #[test]
    fn non_ascii_text_survives_a_round_trip() {
        for text in [
            "Lead — Execution Plan",
            "José Peña",
            "Zürich ñandú",
            "emoji 😀 done",
            "日本語",
        ] {
            let json = stringify(&Value::from(text));
            assert_eq!(parse(&json).expect("parses"), Value::from(text), "{text}");
        }
    }

    #[test]
    fn escapes_surrogate_pairs_and_control_characters_decode_and_encode() {
        assert_eq!(parse(r#""café""#).unwrap(), Value::from("café"));
        assert_eq!(parse(r#""😀""#).unwrap(), Value::from("😀"));
        assert_eq!(
            parse(r#""a\/b\b\f""#).unwrap(),
            Value::from("a/b\u{8}\u{c}")
        );
        let with_control = Value::from("bell\u{7}end");
        let json = stringify(&with_control);
        assert!(
            !json.contains('\u{7}'),
            "a raw control character is not valid JSON: {json:?}"
        );
        assert_eq!(parse(&json).unwrap(), with_control);
    }

    #[test]
    fn a_truncated_unicode_escape_is_an_error_not_a_panic() {
        assert!(parse(r#""\u12""#).is_err());
        assert!(parse(r#""\u"#).is_err());
    }
}
