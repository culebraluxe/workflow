//! Bounded XML 1.0 → ProcessGraph (host-neutral).
//!
//! The XML file is the definition; this module only parses it. Host-specific attributes (for
//! example Forge's `service=`) are read by the host from [`parse_element`]'s raw tree; the generic
//! node model intentionally ignores them.

use std::collections::BTreeMap;

use crate::{
    DecisionArm, DefinitionStatus, NodeDefinition, ProcessDefinition, ProcessGraph, ProcessOutcome,
    TransitionDefinition,
};

#[derive(Debug)]
pub struct XmlError(pub String);

impl std::fmt::Display for XmlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct XmlElement {
    pub name: String,
    pub attrs: BTreeMap<String, String>,
    pub children: Vec<XmlNode>,
}

#[derive(Debug, Clone)]
pub enum XmlNode {
    Element(XmlElement),
    Text,
}

struct Cur<'a> {
    s: &'a str,
    i: usize,
}

impl<'a> Cur<'a> {
    fn eof(&self) -> bool {
        self.i >= self.s.len()
    }
    fn peek(&self) -> Option<char> {
        self.s[self.i..].chars().next()
    }
    fn starts(&self, p: &str) -> bool {
        self.s[self.i..].starts_with(p)
    }
    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.i += c.len_utf8();
        Some(c)
    }
    fn skip(&mut self, n: usize) {
        self.i += n;
    }
    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_whitespace()) {
            self.bump();
        }
    }
    fn fail(&self, m: &str) -> XmlError {
        XmlError(format!("{m} at {}", self.i))
    }
}

pub fn parse_element(src: &str) -> Result<XmlElement, XmlError> {
    let mut c = Cur { s: src, i: 0 };
    skip_misc(&mut c)?;
    let root = parse_elem(&mut c)?;
    skip_misc(&mut c)?;
    if !c.eof() {
        return Err(c.fail("unexpected content after root"));
    }
    Ok(root)
}

fn skip_misc(c: &mut Cur<'_>) -> Result<(), XmlError> {
    loop {
        c.skip_ws();
        if c.starts("<?") {
            let rest = &c.s[c.i..];
            let Some(end) = rest.find("?>") else {
                return Err(c.fail("unterminated declaration"));
            };
            c.i += end + 2;
            continue;
        }
        if c.starts("<!--") {
            let rest = &c.s[c.i..];
            let Some(end) = rest.find("-->") else {
                return Err(c.fail("unterminated comment"));
            };
            c.i += end + 3;
            continue;
        }
        return Ok(());
    }
}

fn is_name_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}
fn is_name_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-')
}

fn parse_name(c: &mut Cur<'_>) -> Result<String, XmlError> {
    let Some(ch) = c.peek() else {
        return Err(c.fail("expected name"));
    };
    if !is_name_start(ch) {
        return Err(c.fail("expected name"));
    }
    let mut n = String::new();
    n.push(c.bump().unwrap());
    while matches!(c.peek(), Some(ch) if is_name_char(ch)) {
        n.push(c.bump().unwrap());
    }
    Ok(n)
}

fn parse_elem(c: &mut Cur<'_>) -> Result<XmlElement, XmlError> {
    if c.peek() != Some('<') {
        return Err(c.fail("expected element"));
    }
    c.bump();
    let name = parse_name(c)?;
    let mut attrs = BTreeMap::new();
    loop {
        c.skip_ws();
        match c.peek() {
            Some('/') => {
                c.bump();
                if c.peek() != Some('>') {
                    return Err(c.fail("malformed self-close"));
                }
                c.bump();
                return Ok(XmlElement {
                    name,
                    attrs,
                    children: vec![],
                });
            }
            Some('>') => {
                c.bump();
                break;
            }
            None => return Err(c.fail("unterminated element")),
            _ => {
                let an = parse_name(c)?;
                c.skip_ws();
                if c.peek() != Some('=') {
                    return Err(c.fail("expected ="));
                }
                c.bump();
                c.skip_ws();
                let q = c.bump().ok_or_else(|| c.fail("expected quote"))?;
                if q != '"' && q != '\'' {
                    return Err(c.fail("attribute must be quoted"));
                }
                let mut val = String::new();
                loop {
                    match c.bump() {
                        None => return Err(c.fail("unterminated attr")),
                        Some(ch) if ch == q => break,
                        Some('&') => val.push_str(&parse_ent(c)?),
                        Some(ch) => val.push(ch),
                    }
                }
                attrs.insert(an, val);
            }
        }
    }
    let mut children = Vec::new();
    loop {
        if c.eof() {
            return Err(XmlError(format!("missing </{name}>")));
        }
        if c.starts("</") {
            c.skip(2);
            let close = parse_name(c)?;
            c.skip_ws();
            if c.peek() != Some('>') {
                return Err(c.fail("malformed close"));
            }
            c.bump();
            if close != name {
                return Err(XmlError(format!("expected </{name}>, found </{close}>")));
            }
            return Ok(XmlElement {
                name,
                attrs,
                children,
            });
        }
        if c.starts("<!--") {
            skip_misc(c)?;
            continue;
        }
        if c.peek() == Some('<') {
            children.push(XmlNode::Element(parse_elem(c)?));
            continue;
        }
        let mut text = String::new();
        while !c.eof() && c.peek() != Some('<') {
            if c.peek() == Some('&') {
                c.bump();
                text.push_str(&parse_ent(c)?);
            } else {
                text.push(c.bump().unwrap());
            }
        }
        if !text.trim().is_empty() {
            children.push(XmlNode::Text);
        }
    }
}

fn parse_ent(c: &mut Cur<'_>) -> Result<String, XmlError> {
    let rest = &c.s[c.i..];
    let Some(end) = rest.find(';') else {
        return Err(c.fail("unterminated entity"));
    };
    let body = &rest[..end];
    c.i += end + 1;
    Ok(match body {
        "amp" => "&".into(),
        "lt" => "<".into(),
        "gt" => ">".into(),
        "quot" => "\"".into(),
        "apos" => "'".into(),
        _ => return Err(XmlError(format!("unknown entity &{body};"))),
    })
}

fn req(el: &XmlElement, k: &str) -> Result<String, XmlError> {
    el.attrs
        .get(k)
        .filter(|s| !s.is_empty())
        .cloned()
        .ok_or_else(|| XmlError(format!("missing {k} on <{}>", el.name)))
}

fn collect_transitions(el: &XmlElement) -> Result<Option<Vec<TransitionDefinition>>, XmlError> {
    let mut out = Vec::new();
    for ch in &el.children {
        let XmlNode::Element(e) = ch else { continue };
        if e.name != "transition" {
            continue;
        }
        out.push(TransitionDefinition {
            name: req(e, "name")?,
            to: req(e, "to")?,
            condition: e.attrs.get("condition").cloned(),
            required: e.attrs.get("required").map(|s| s == "true"),
        });
    }
    Ok(if out.is_empty() { None } else { Some(out) })
}

fn parse_node(el: &XmlElement) -> Result<NodeDefinition, XmlError> {
    let id = req(el, "id")?;
    let label = el.attrs.get("label").cloned().unwrap_or_else(|| id.clone());
    let description = el.attrs.get("description").cloned();
    let responsibility = el.attrs.get("responsibility").cloned();
    let transitions = collect_transitions(el)?;
    let mut node = NodeDefinition {
        id: id.clone(),
        name: Some(label),
        description: description.clone(),
        responsibility: responsibility.clone(),
        transitions,
        ..Default::default()
    };
    match el.name.as_str() {
        "start-state" => node.node_type = "start".into(),
        "state" => node.node_type = "state".into(),
        "end-state" => {
            node.node_type = "end".into();
            node.outcome = Some(match el.attrs.get("outcome").map(|s| s.as_str()) {
                Some("cancelled") => ProcessOutcome::Cancelled,
                Some("failed") => ProcessOutcome::Failed,
                Some("conflict") => ProcessOutcome::Conflict,
                _ => ProcessOutcome::Completed,
            });
        }
        "task-node" => {
            node.node_type = "task".into();
            if let Some(r) = &responsibility {
                node.candidate_groups = Some(vec![r.clone()]);
            }
            node.form_key = el.attrs.get("form-key").cloned();
            if let Some(p) = el.attrs.get("priority") {
                node.priority = p.parse().ok();
            }
        }
        "command-node" => {
            node.node_type = "command".into();
            node.command_type = Some(req(el, "command-type")?);
            node.transition = el.attrs.get("transition").cloned();
        }
        "decision" => {
            node.node_type = "decision".into();
            node.refresh_facts = el.attrs.get("refresh-facts").map(|s| s == "true");
            let mut decisions = Vec::new();
            for ch in &el.children {
                let XmlNode::Element(e) = ch else { continue };
                if e.name != "on" {
                    continue;
                }
                decisions.push(DecisionArm {
                    condition: req(e, "condition")?,
                    transition: req(e, "transition")?,
                });
            }
            node.decisions = Some(decisions);
        }
        "fork" => node.node_type = "fork".into(),
        "join" => node.node_type = "join".into(),
        "timer" => {
            node.node_type = "timer".into();
            node.timer = Some(crate::TimerSpec {
                due_at: el.attrs.get("due-at").cloned(),
                due_at_variable: el.attrs.get("due-at-variable").cloned(),
                transition: el.attrs.get("on-fire").cloned(),
            });
        }
        "dynamic-fork" => {
            node.node_type = "dynamic-fork".into();
            node.count_variable = Some(req(el, "count-variable")?);
            node.branch_command_type = Some(req(el, "branch-command-type")?);
            node.join = Some(req(el, "join")?);
            node.branch_node = el.attrs.get("branch-node").cloned();
            node.plan_variable = el.attrs.get("plan-variable").cloned();
            node.minimum = el.attrs.get("minimum").and_then(|s| s.parse().ok());
            node.maximum = el.attrs.get("maximum").and_then(|s| s.parse().ok());
        }
        other => return Err(XmlError(format!("unsupported <{other}>"))),
    }
    Ok(node)
}

#[derive(Debug, Clone)]
pub struct ParsedDefinition {
    pub key: String,
    pub version: i32,
    pub name: String,
    pub description: Option<String>,
    pub graph: ProcessGraph,
}

pub fn parse_process_definition_xml(source: &str) -> Result<ParsedDefinition, XmlError> {
    let root = parse_element(source)?;
    if root.name != "process-definition" {
        return Err(XmlError(format!("root is <{}>", root.name)));
    }
    let key = req(&root, "key")?;
    let version: i32 = req(&root, "version")?
        .parse()
        .map_err(|_| XmlError("bad version".into()))?;
    let name = req(&root, "name")?;
    let description = root.attrs.get("description").cloned();
    let mut nodes = BTreeMap::new();
    let mut display_order = Vec::new();
    let mut start_id = None;
    for ch in &root.children {
        let XmlNode::Element(el) = ch else {
            continue;
        };
        if el.name == "display-order" {
            for n in &el.children {
                let XmlNode::Element(r) = n else { continue };
                if r.name == "node" {
                    display_order.push(req(r, "ref")?);
                }
            }
            continue;
        }
        let node = parse_node(el)?;
        if node.node_type == "start" {
            start_id = Some(node.id.clone());
        }
        if nodes.contains_key(&node.id) {
            return Err(XmlError(format!("duplicate node {}", node.id)));
        }
        nodes.insert(node.id.clone(), node);
    }
    let start_node_id = start_id.ok_or_else(|| XmlError("missing start-state".into()))?;
    for n in nodes.values() {
        for t in n.transitions.as_deref().unwrap_or(&[]) {
            if !nodes.contains_key(&t.to) {
                return Err(XmlError(format!(
                    "transition '{}' on '{}' targets missing '{}'",
                    t.name, n.id, t.to
                )));
            }
        }
    }
    Ok(ParsedDefinition {
        key,
        version,
        name,
        description,
        graph: ProcessGraph {
            nodes,
            start_node_id,
            display_order: if display_order.is_empty() {
                None
            } else {
                Some(display_order)
            },
        },
    })
}

/// Wrap a parsed definition in the engine's [`ProcessDefinition`] identity.
pub fn definition_from_xml(source: &str) -> Result<ProcessDefinition, XmlError> {
    let p = parse_process_definition_xml(source)?;
    Ok(ProcessDefinition {
        id: format!("{}-v{}", p.key, p.version),
        tenant_id: None,
        key: p.key,
        version: p.version,
        name: p.name,
        description: p.description,
        definition: p.graph,
        status: DefinitionStatus::Active,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minimal_definition_parses_without_host_attributes() {
        let xml = r#"<process-definition key="smoke" version="1" name="Smoke">
            <start-state id="start"><transition name="go" to="end"/></start-state>
            <end-state id="end"/>
        </process-definition>"#;
        let parsed = parse_process_definition_xml(xml).expect("minimal definition parses");
        assert_eq!(parsed.key, "smoke");
        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.graph.start_node_id, "start");
        let def = definition_from_xml(xml).expect("definition wraps");
        assert_eq!(def.key, "smoke");
        assert_eq!(def.id, "smoke-v1");
    }

    #[test]
    fn the_raw_tree_exposes_attributes_for_host_helpers() {
        let xml = r#"<process-definition key="k" version="1" name="n"><task-node id="t" service="forge.smith"/></process-definition>"#;
        let root = parse_element(xml).expect("raw tree");
        let XmlNode::Element(task) = &root.children[0] else {
            panic!("expected element child");
        };
        assert_eq!(
            task.attrs.get("service").map(String::as_str),
            Some("forge.smith")
        );
    }
}
