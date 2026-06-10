//! Exclusive XML Canonicalization (xml-exc-c14n) for SAML XML-DSig.
//!
//! This is a focused, pure-Rust implementation of
//! <https://www.w3.org/TR/xml-exc-c14n/> sufficient to canonicalize the
//! `<ds:SignedInfo>` octets and the signed `<saml:Assertion>` element before
//! digesting/verifying an XML-DSig signature. It exists so SAML assertion
//! validation does NOT rely on the verbatim element bytes "as received" (which
//! is bypassable under XML transforms) — instead we canonicalize per the spec
//! and digest/verify the canonical form, exactly as a conforming IdP signs it.
//!
//! Scope / supported subset (matches what SAML POST assertions actually use):
//!   * Exclusive canonicalization (`xml-exc-c14n#`) with an optional
//!     `InclusiveNamespaces` PrefixList.
//!   * Elements, attributes, text, CDATA (folded to text), and whitespace
//!     between elements. Comments are ALWAYS omitted (the `#WithComments`
//!     variant is not used by SAML signatures and a comment-injection must not
//!     change the digest).
//!   * Namespace axis per exclusive c14n §2: a prefix's declaration is emitted
//!     on an element iff that prefix is *visibly utilised* by the element (its
//!     own name or one of its attributes), or it appears in the PrefixList, AND
//!     the same (prefix,uri) was not already emitted by an output ancestor.
//!   * Attribute ordering per c14n §2.2: namespace declarations first (sorted by
//!     local name, with the default `xmlns` before prefixed `xmlns:*`), then
//!     ordinary attributes sorted by (namespace-URI, local-name).
//!
//! NOT implemented (not needed for SAML signature verification, and their
//! presence would be rejected upstream): DTDs/entity refs beyond the 5 builtins,
//! processing instructions inside the signed element, and the `#WithComments`
//! mode. Inputs that use them simply canonicalize without them; the digest then
//! won't match and verification fails closed — never the reverse.

use std::collections::BTreeMap;

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};

/// A namespace declaration: prefix (empty string == default namespace) → URI.
type NsMap = BTreeMap<String, String>;

/// Canonicalize the first element with the given local name (e.g. `SignedInfo`
/// or `Assertion`) found in `xml`, using exclusive C14N. `inclusive_prefixes`
/// is the `InclusiveNamespaces/@PrefixList` (whitespace-separated) if present.
///
/// The element is canonicalized *in the namespace context of the full document*
/// (i.e. namespaces declared on ancestors are inherited and rendered on the
/// apex element when visibly utilised), which is what XML-DSig requires when the
/// signed element is a fragment of a larger response.
pub fn canonicalize_element(
    xml: &str,
    local: &str,
    inclusive_prefixes: &[String],
) -> Option<Vec<u8>> {
    canonicalize(xml, &Target::Local(local), inclusive_prefixes, false)
}

/// Canonicalize the element whose `ID`/`Id`/`xml:id` attribute equals `id`,
/// using exclusive C14N, with the **enveloped-signature transform** applied:
/// any descendant `<ds:Signature>` element is removed before canonicalization
/// (XML-DSig's standard transform — the signature cannot cover itself). Used to
/// canonicalize the element named by an XML-DSig `<ds:Reference URI="#id">` so
/// its digest can be compared to `<ds:DigestValue>`.
pub fn canonicalize_element_by_id(
    xml: &str,
    id: &str,
    inclusive_prefixes: &[String],
) -> Option<Vec<u8>> {
    canonicalize(xml, &Target::Id(id), inclusive_prefixes, true)
}

/// What element [`canonicalize`] should select as the canonicalization apex.
enum Target<'a> {
    /// First element with this local name.
    Local(&'a str),
    /// Element whose ID attribute equals this value.
    Id(&'a str),
}

impl Target<'_> {
    fn matches(&self, e: &BytesStart<'_>) -> bool {
        match self {
            Target::Local(local) => local_name(e.name().as_ref()) == **local,
            Target::Id(id) => element_id(e).as_deref() == Some(*id),
        }
    }
}

/// Extract an element's signing ID from `ID` / `Id` / `xml:id` (local-name match).
fn element_id(e: &BytesStart<'_>) -> Option<String> {
    for attr in e.attributes().flatten() {
        let key = attr.key.as_ref();
        let lname = local_name(key);
        if lname == "ID" || lname == "Id" || lname == "id" {
            return Some(unescape_bytes(&attr.value));
        }
    }
    None
}

fn canonicalize(
    xml: &str,
    target: &Target<'_>,
    inclusive_prefixes: &[String],
    omit_enveloped_signature: bool,
) -> Option<Vec<u8>> {
    let mut reader = Reader::from_str(xml);
    // Do NOT trim: c14n preserves whitespace inside element content.
    reader.trim_text(false);
    reader.expand_empty_elements(false);
    reader.check_end_names(false);

    // Namespace context stack as we descend the *whole* document, so that when
    // we reach the target element we know every in-scope (prefix→uri) binding
    // inherited from its ancestors.
    let mut ctx_stack: Vec<NsMap> = vec![BTreeMap::new()];

    let mut out: Vec<u8> = Vec::new();
    // Once we enter the target subtree this holds the depth at which we entered.
    let mut in_target_depth: Option<usize> = None;
    // What each *output* ancestor already emitted (exclusive c14n diff base).
    let mut rendered_stack: Vec<NsMap> = Vec::new();
    // Original qnames of currently-open OUTPUT elements, so we can emit the
    // matching end tag (c14n always uses explicit start+end tags).
    let mut open_qnames: Vec<Vec<u8>> = Vec::new();
    let mut depth = 0usize;
    // Enveloped-signature transform: when set, we are inside a descendant
    // `<ds:Signature>` subtree that must be excluded from the canonical output
    // (its content is suppressed, but namespace-context tracking continues).
    let mut skip_depth: Option<usize> = None;

    loop {
        // Output is emitted only when inside the target AND not inside a skipped
        // (enveloped-signature) subtree.
        let emit = in_target_depth.is_some() && skip_depth.is_none();
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let frame = push_ns_frame(&mut ctx_stack, &e);

                let entering_target = in_target_depth.is_none() && target.matches(&e);
                if entering_target {
                    in_target_depth = Some(depth);
                    rendered_stack.clear();
                    open_qnames.clear();
                }

                // Begin skipping an enveloped <ds:Signature> child of the target.
                if omit_enveloped_signature
                    && in_target_depth.is_some()
                    && !entering_target
                    && skip_depth.is_none()
                    && local_name(e.name().as_ref()) == "Signature"
                {
                    skip_depth = Some(depth);
                }

                // Emit when inside the target (now possibly just-entered) and not
                // inside a skipped enveloped-signature subtree.
                if in_target_depth.is_some() && skip_depth.is_none() {
                    let prev_rendered = rendered_stack.last().cloned().unwrap_or_default();
                    let emitted =
                        write_start_tag(&mut out, &e, &frame, &prev_rendered, inclusive_prefixes);
                    rendered_stack.push(emitted);
                    open_qnames.push(e.name().as_ref().to_vec());
                }
                depth += 1;
            }
            Ok(Event::End(_)) => {
                depth = depth.saturating_sub(1);
                ctx_stack.pop();
                // Leaving the skipped <ds:Signature> subtree?
                if let Some(sd) = skip_depth {
                    if depth == sd {
                        skip_depth = None;
                    }
                    // While still inside the skip subtree, emit nothing.
                    if skip_depth.is_some() {
                        continue;
                    }
                }
                if emit {
                    if let Some(td) = in_target_depth {
                        if depth >= td {
                            rendered_stack.pop();
                            if let Some(qname) = open_qnames.pop() {
                                out.extend_from_slice(b"</");
                                out.extend_from_slice(&qname);
                                out.push(b'>');
                            }
                        }
                    }
                }
                if let Some(td) = in_target_depth {
                    if depth == td {
                        // Left the target element entirely.
                        break;
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                // Expand empty element into explicit start+end per c14n.
                let frame = push_ns_frame(&mut ctx_stack, &e);
                ctx_stack.pop(); // empty element opens and closes immediately

                let entering_target = in_target_depth.is_none() && target.matches(&e);
                // A self-closing enveloped <ds:Signature/> contributes nothing.
                let is_skipped_sig = omit_enveloped_signature
                    && in_target_depth.is_some()
                    && !entering_target
                    && local_name(e.name().as_ref()) == "Signature";
                if (emit || entering_target) && !is_skipped_sig {
                    let prev_rendered = if entering_target {
                        BTreeMap::new()
                    } else {
                        rendered_stack.last().cloned().unwrap_or_default()
                    };
                    write_start_tag(&mut out, &e, &frame, &prev_rendered, inclusive_prefixes);
                    out.extend_from_slice(b"</");
                    out.extend_from_slice(e.name().as_ref());
                    out.push(b'>');
                    if entering_target {
                        // A self-closing apex target: done.
                        in_target_depth = Some(depth);
                        break;
                    }
                }
            }
            Ok(Event::Text(t)) => {
                if emit {
                    let raw = t.into_inner();
                    let s = unescape_bytes(&raw);
                    out.extend_from_slice(canon_text(&s).as_bytes());
                }
            }
            Ok(Event::CData(t)) => {
                if emit {
                    let raw = t.into_inner();
                    let s = String::from_utf8_lossy(&raw);
                    out.extend_from_slice(canon_text(&s).as_bytes());
                }
            }
            // Comments are dropped (no #WithComments). PIs/Decl dropped too.
            Ok(_) => {}
            Err(_) => return None,
        }
    }

    if in_target_depth.is_some() {
        Some(out)
    } else {
        None
    }
}

/// Push a new in-scope namespace frame onto `ctx_stack` for start tag `e`,
/// applying its xmlns declarations (and `xmlns=""` undeclaration). Returns the
/// new frame (full inherited + local bindings).
fn push_ns_frame(ctx_stack: &mut Vec<NsMap>, e: &BytesStart<'_>) -> NsMap {
    let mut frame = ctx_stack.last().cloned().unwrap_or_default();
    for (p, u) in collect_ns_decls(e) {
        if u.is_empty() && p.is_empty() {
            frame.remove("");
        } else {
            frame.insert(p, u);
        }
    }
    ctx_stack.push(frame.clone());
    frame
}

/// Collect xmlns / xmlns:prefix declarations on a start tag as (prefix, uri).
/// The default namespace uses prefix "".
fn collect_ns_decls(e: &BytesStart<'_>) -> Vec<(String, String)> {
    let mut v = Vec::new();
    for attr in e.attributes().flatten() {
        let key = attr.key.as_ref();
        if key == b"xmlns" {
            let uri = unescape_bytes(&attr.value);
            v.push((String::new(), uri));
        } else if let Some(prefix) = key.strip_prefix(b"xmlns:") {
            let p = String::from_utf8_lossy(prefix).to_string();
            let uri = unescape_bytes(&attr.value);
            v.push((p, uri));
        }
    }
    v
}

/// Write a canonical start tag and return the namespace map (prefix→uri) this
/// element actually *emitted* (for descendants to diff against). `in_scope` is
/// the full inherited namespace context; `ancestor_rendered` is what output
/// ancestors already emitted.
fn write_start_tag(
    out: &mut Vec<u8>,
    e: &BytesStart<'_>,
    in_scope: &NsMap,
    ancestor_rendered: &NsMap,
    inclusive_prefixes: &[String],
) -> NsMap {
    let qname = e.name();
    let qname = qname.as_ref();
    out.push(b'<');
    out.extend_from_slice(qname);

    // ── Determine visibly-utilised prefixes ──────────────────────────────────
    // The element's own prefix.
    let mut utilised: BTreeMap<String, ()> = BTreeMap::new();
    utilised.insert(prefix_of(qname), ());
    // Prefixes of non-namespace attributes.
    for attr in e.attributes().flatten() {
        let key = attr.key.as_ref();
        if key == b"xmlns" || key.starts_with(b"xmlns:") {
            continue;
        }
        let p = prefix_of(key);
        // An unprefixed attribute is NOT in any namespace and does not utilise
        // the default namespace; only prefixed attributes utilise a prefix.
        if !p.is_empty() {
            utilised.insert(p, ());
        }
    }
    // PrefixList forces these prefixes to be treated as utilised.
    for p in inclusive_prefixes {
        let key = if p == "#default" {
            String::new()
        } else {
            p.clone()
        };
        utilised.insert(key, ());
    }

    // ── Emit namespace declarations (sorted; default first) ──────────────────
    // Build the set we will output: for each utilised prefix that is in scope,
    // emit it unless an output ancestor already emitted the same (prefix,uri).
    let mut this_rendered = ancestor_rendered.clone();
    let mut ns_to_emit: Vec<(String, String)> = Vec::new();
    for (prefix, _) in &utilised {
        if let Some(uri) = in_scope.get(prefix) {
            let already = ancestor_rendered
                .get(prefix)
                .map(|u| u == uri)
                .unwrap_or(false);
            if !already && !(prefix.is_empty() && uri.is_empty()) {
                ns_to_emit.push((prefix.clone(), uri.clone()));
                this_rendered.insert(prefix.clone(), uri.clone());
            }
        } else if prefix.is_empty() {
            // Default namespace utilised but not in scope → it's the null
            // namespace. If an ancestor rendered a non-empty default, we must
            // undeclare it with xmlns="".
            if let Some(prev) = ancestor_rendered.get("") {
                if !prev.is_empty() {
                    ns_to_emit.push((String::new(), String::new()));
                    this_rendered.insert(String::new(), String::new());
                }
            }
        }
    }
    // Sort: default namespace (empty prefix) first, then by prefix name.
    ns_to_emit.sort_by(|a, b| match (a.0.is_empty(), b.0.is_empty()) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.0.cmp(&b.0),
    });
    for (prefix, uri) in &ns_to_emit {
        if prefix.is_empty() {
            out.extend_from_slice(b" xmlns=\"");
        } else {
            out.extend_from_slice(b" xmlns:");
            out.extend_from_slice(prefix.as_bytes());
            out.extend_from_slice(b"=\"");
        }
        out.extend_from_slice(canon_attr_value(uri).as_bytes());
        out.push(b'"');
    }

    // ── Emit ordinary attributes, sorted by (namespace-uri, local-name) ──────
    let mut attrs: Vec<(String, String, Vec<u8>, Vec<u8>)> = Vec::new();
    for attr in e.attributes().flatten() {
        let key = attr.key.as_ref().to_vec();
        if key == b"xmlns" || key.starts_with(b"xmlns:") {
            continue;
        }
        let prefix = prefix_of(&key);
        let ns_uri = if prefix.is_empty() {
            // Unprefixed attributes have no namespace (NOT the default ns).
            String::new()
        } else {
            in_scope.get(&prefix).cloned().unwrap_or_default()
        };
        let local = local_name(&key);
        attrs.push((ns_uri, local, key, attr.value.to_vec()));
    }
    attrs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    for (_, _, key, value) in &attrs {
        out.push(b' ');
        out.extend_from_slice(key);
        out.extend_from_slice(b"=\"");
        let v = unescape_bytes(value);
        out.extend_from_slice(canon_attr_value(&v).as_bytes());
        out.push(b'"');
    }

    out.push(b'>');
    this_rendered
}

/// Local name of a qname (`ds:Foo` → `Foo`).
fn local_name(qname: &[u8]) -> String {
    let s = String::from_utf8_lossy(qname);
    match s.rsplit_once(':') {
        Some((_, local)) => local.to_string(),
        None => s.to_string(),
    }
}

/// Prefix of a qname (`ds:Foo` → `ds`, `Foo` → ``).
fn prefix_of(qname: &[u8]) -> String {
    let s = String::from_utf8_lossy(qname);
    match s.split_once(':') {
        Some((p, _)) => p.to_string(),
        None => String::new(),
    }
}

/// Unescape the 5 XML builtins (and numeric refs) in a raw byte slice to a
/// String. Inputs here are already UTF-8.
fn unescape_bytes(raw: &[u8]) -> String {
    let s = String::from_utf8_lossy(raw);
    unescape_str(&s)
}

fn unescape_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'&' {
            if let Some(semi) = s[i..].find(';') {
                let ent = &s[i + 1..i + semi];
                let replaced = match ent {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    _ if ent.starts_with("#x") || ent.starts_with("#X") => {
                        u32::from_str_radix(&ent[2..], 16)
                            .ok()
                            .and_then(char::from_u32)
                    }
                    _ if ent.starts_with('#') => {
                        ent[1..].parse::<u32>().ok().and_then(char::from_u32)
                    }
                    _ => None,
                };
                if let Some(c) = replaced {
                    out.push(c);
                    i += semi + 1;
                    continue;
                }
            }
        }
        // Push one UTF-8 char.
        let ch_len = utf8_len(bytes[i]);
        let end = (i + ch_len).min(bytes.len());
        out.push_str(&s[i..end]);
        i = end;
    }
    out
}

fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else if b >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

/// Canonicalize text content per c14n §2.3: `&` → `&amp;`, `<` → `&lt;`,
/// `>` → `&gt;`, and CR (`\r`, 0x0D) → `&#xD;`. (Line-ending normalization to
/// LF is done by the XML parser before this point.)
fn canon_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' => out.push_str("&#xD;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c14n(xml: &str, local: &str) -> String {
        let bytes = canonicalize_element(xml, local, &[]).expect("canonicalize");
        String::from_utf8(bytes).expect("utf8")
    }

    #[test]
    fn attributes_are_sorted_and_ns_emitted_at_apex() {
        // Namespaces declared on an ancestor must surface on the canonical apex
        // when visibly utilised; attributes sort by (ns-uri, local).
        let xml = r#"<root xmlns:a="urn:a"><a:el z="2" a="1" a:b="x">text</a:el></root>"#;
        let out = c14n(xml, "el");
        assert_eq!(
            out,
            r#"<a:el xmlns:a="urn:a" a="1" z="2" a:b="x">text</a:el>"#
        );
    }

    #[test]
    fn unused_namespace_is_dropped_exclusive() {
        // `unused` prefix is declared on the ancestor but NOT visibly utilised by
        // the canonicalized element → exclusive c14n drops it.
        let xml = r#"<root xmlns:unused="urn:x" xmlns:a="urn:a"><a:el>v</a:el></root>"#;
        let out = c14n(xml, "el");
        assert_eq!(out, r#"<a:el xmlns:a="urn:a">v</a:el>"#);
    }

    #[test]
    fn empty_element_is_expanded() {
        let xml = r#"<root><inner/></root>"#;
        assert_eq!(c14n(xml, "inner"), "<inner></inner>");
    }

    #[test]
    fn comments_are_omitted() {
        let with = r#"<root><x>a<!-- c -->b</x></root>"#;
        let without = r#"<root><x>ab</x></root>"#;
        assert_eq!(c14n(with, "x"), c14n(without, "x"));
        assert_eq!(c14n(with, "x"), "<x>ab</x>");
    }

    #[test]
    fn text_and_attr_special_chars_are_escaped() {
        let xml = r#"<root><x t="a&lt;b&#9;c">1&amp;2&lt;3</x></root>"#;
        let out = c14n(xml, "x");
        // c14n §2.3: `<` is escaped to `&lt;` inside attribute values (not just
        // in text), alongside `&`→`&amp;`, `"`→`&quot;`, TAB→`&#x9;`.
        assert_eq!(out, r#"<x t="a&lt;b&#x9;c">1&amp;2&lt;3</x>"#);
    }

    #[test]
    fn inclusive_prefix_list_is_honored() {
        // `xs` is not utilised by the element but the PrefixList forces it.
        let xml = r#"<root xmlns:xs="urn:xs" xmlns:a="urn:a"><a:el>v</a:el></root>"#;
        let bytes = canonicalize_element(xml, "el", &["xs".to_string()]).expect("canonicalize");
        let out = String::from_utf8(bytes).unwrap();
        assert_eq!(out, r#"<a:el xmlns:a="urn:a" xmlns:xs="urn:xs">v</a:el>"#);
    }

    #[test]
    fn transform_changes_canonical_form() {
        // Two documents whose canonical forms differ → their digests must differ
        // (this is the property that closes the SignedInfo-verbatim bypass).
        let a = c14n(r#"<root><x>hello</x></root>"#, "x");
        let b = c14n(r#"<root><x>hell0</x></root>"#, "x");
        assert_ne!(a, b);
    }

    #[test]
    fn whitespace_inside_element_is_preserved() {
        let xml = "<root><x>  a\n  b  </x></root>";
        assert_eq!(c14n(xml, "x"), "<x>  a\n  b  </x>");
    }
}

/// Canonicalize an attribute value per c14n §2.3: `&` → `&amp;`, `<` → `&lt;`,
/// `"` → `&quot;`, TAB → `&#x9;`, LF → `&#xA;`, CR → `&#xD;`.
fn canon_attr_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '"' => out.push_str("&quot;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xA;"),
            '\r' => out.push_str("&#xD;"),
            _ => out.push(c),
        }
    }
    out
}
