//! Shared code-aware Rust source scanning for repository governance tests.
//!
//! This Module owns trivia/literal skipping and structural Module discovery.
//! It does not parse Rust semantics or replace compiler/provenance tests.
#![allow(dead_code)] // The two integration-test consumers use disjoint scanner slices.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalCfgTestModule {
    pub(crate) name: String,
    pub(crate) explicit_paths: Vec<String>,
    pub(crate) module_context: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalRustModule {
    pub(crate) name: String,
    pub(crate) module_context: Vec<String>,
    pub(crate) is_private: bool,
    pub(crate) is_unconditional: bool,
    pub(crate) uses_default_path: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RustTestFunction {
    pub(crate) name: String,
    pub(crate) module_context: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RustPathAttribute {
    Literal(String),
    Unsupported,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LintSuppressionCounts {
    pub(crate) too_many_arguments: usize,
    pub(crate) broad: usize,
}

fn quoted_literal_end(bytes: &[u8], start: usize, quote: u8) -> Option<usize> {
    let mut cursor = start.checked_add(1)?;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' => cursor = cursor.checked_add(2)?,
            byte if byte == quote => return Some(cursor + 1),
            b'\n' if quote == b'\'' => return None,
            _ => cursor += 1,
        }
    }
    None
}

fn raw_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let raw_marker = if bytes.get(start) == Some(&b'r') {
        start
    } else if (bytes.get(start) == Some(&b'b') || bytes.get(start) == Some(&b'c'))
        && bytes.get(start + 1) == Some(&b'r')
    {
        start + 1
    } else {
        return None;
    };
    let mut quote = raw_marker + 1;
    while bytes.get(quote) == Some(&b'#') {
        quote += 1;
    }
    if bytes.get(quote) != Some(&b'"') {
        return None;
    }
    let hashes = quote - raw_marker - 1;
    let mut cursor = quote + 1;
    while cursor < bytes.len() {
        if bytes[cursor] == b'"'
            && bytes
                .get(cursor + 1..cursor + 1 + hashes)
                .is_some_and(|suffix| suffix.iter().all(|byte| *byte == b'#'))
        {
            return Some(cursor + 1 + hashes);
        }
        cursor += 1;
    }
    None
}

fn block_comment_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 1usize;
    let mut cursor = start.checked_add(2)?;
    while cursor + 1 < bytes.len() {
        match (bytes[cursor], bytes[cursor + 1]) {
            (b'/', b'*') => {
                depth = depth.checked_add(1)?;
                cursor += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                cursor += 2;
                if depth == 0 {
                    return Some(cursor);
                }
            }
            _ => cursor += 1,
        }
    }
    None
}

fn rust_non_code_end(source: &str, cursor: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    if bytes.get(cursor..cursor + 2) == Some(b"//") {
        return Some(
            source[cursor..]
                .find('\n')
                .map_or(bytes.len(), |newline| cursor + newline + 1),
        );
    }
    if bytes.get(cursor..cursor + 2) == Some(b"/*") {
        return Some(block_comment_end(bytes, cursor).unwrap_or(bytes.len()));
    }
    if let Some(end) = raw_string_end(bytes, cursor) {
        return Some(end);
    }
    match bytes.get(cursor) {
        Some(b'"') => Some(quoted_literal_end(bytes, cursor, b'"').unwrap_or(bytes.len())),
        Some(b'\'') => quoted_literal_end(bytes, cursor, b'\''),
        _ => None,
    }
}

fn rust_attribute_starts(source: &str) -> Vec<usize> {
    let bytes = source.as_bytes();
    let mut starts = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if let Some(end) = rust_non_code_end(source, cursor) {
            cursor = end;
        } else {
            if bytes.get(cursor..cursor + 2) == Some(b"#[") {
                starts.push(cursor);
            }
            cursor += 1;
        }
    }
    starts
}

fn attribute_end(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let opening_bracket = if bytes.get(start..start + 2) == Some(b"#[") {
        start + 1
    } else if bytes.get(start..start + 3) == Some(b"#![") {
        start + 2
    } else {
        return None;
    };
    let mut depth = 1usize;
    let mut cursor = opening_bracket + 1;
    while cursor < bytes.len() {
        if let Some(end) = rust_non_code_end(source, cursor) {
            cursor = end;
            continue;
        }
        match bytes[cursor] {
            b'[' => depth = depth.checked_add(1)?,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(cursor + 1);
                }
            }
            _ => {}
        }
        cursor += 1;
    }
    None
}

fn matching_parenthesis(expression: &str, open: usize) -> Option<usize> {
    let bytes = expression.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 1usize;
    let mut cursor = open + 1;
    while cursor < bytes.len() {
        if let Some(end) = rust_non_code_end(expression, cursor) {
            cursor = end;
            continue;
        }
        match bytes[cursor] {
            b'(' => depth = depth.checked_add(1)?,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(cursor);
                }
            }
            _ => {}
        }
        cursor += 1;
    }
    None
}

fn cfg_expression_has_positive_test(expression: &str, negated: bool) -> bool {
    let bytes = expression.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if let Some(end) = rust_non_code_end(expression, cursor) {
            cursor = end;
            continue;
        }
        if !(bytes[cursor] == b'_' || bytes[cursor].is_ascii_alphabetic()) {
            cursor += 1;
            continue;
        }

        let identifier_start = cursor;
        cursor += 1;
        while cursor < bytes.len()
            && (bytes[cursor] == b'_' || bytes[cursor].is_ascii_alphanumeric())
        {
            cursor += 1;
        }
        let identifier = &expression[identifier_start..cursor];
        cursor = skip_rust_trivia(expression, cursor);
        if bytes.get(cursor) == Some(&b'(') {
            let Some(close) = matching_parenthesis(expression, cursor) else {
                return false;
            };
            if cfg_expression_has_positive_test(
                &expression[cursor + 1..close],
                negated ^ (identifier == "not"),
            ) {
                return true;
            }
            cursor = close + 1;
        } else if identifier == "test" && !negated {
            return true;
        }
    }
    false
}

fn cfg_attribute_has_positive_test(source: &str, start: usize, end: usize) -> bool {
    let body_start = if source.as_bytes().get(start..start + 3) == Some(b"#![") {
        start + 3
    } else {
        start + 2
    };
    let attribute = source[body_start..end - 1].trim();
    let mut cursor = skip_rust_trivia(attribute, 0);
    let Some((name, next)) = identifier_at(attribute, cursor) else {
        return false;
    };
    if name != "cfg" {
        return false;
    }
    cursor = skip_rust_trivia(attribute, next);
    if attribute.as_bytes().get(cursor) != Some(&b'(') {
        return false;
    }
    let Some(close) = matching_parenthesis(attribute, cursor) else {
        return false;
    };
    skip_rust_trivia(attribute, close + 1) == attribute.len()
        && cfg_expression_has_positive_test(&attribute[cursor + 1..close], false)
}

fn attribute_controls_cfg(source: &str, start: usize, end: usize) -> bool {
    let body_start = if source.as_bytes().get(start..start + 3) == Some(b"#![") {
        start + 3
    } else {
        start + 2
    };
    let attribute = &source[body_start..end - 1];
    identifier_at(attribute, skip_rust_trivia(attribute, 0))
        .is_some_and(|(name, _)| name == "cfg" || name == "cfg_attr")
}

fn skip_rust_trivia(source: &str, mut cursor: usize) -> usize {
    let bytes = source.as_bytes();
    loop {
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        if bytes.get(cursor..cursor + 2) == Some(b"//") {
            cursor = source[cursor..]
                .find('\n')
                .map_or(bytes.len(), |newline| cursor + newline + 1);
        } else if bytes.get(cursor..cursor + 2) == Some(b"/*") {
            cursor = block_comment_end(bytes, cursor).unwrap_or(bytes.len());
        } else {
            return cursor;
        }
    }
}

fn identifier_at(source: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = source.as_bytes();
    let mut cursor = start;
    if bytes.get(cursor..cursor + 2) == Some(b"r#") {
        cursor += 2;
    }
    let identifier_start = cursor;
    if !bytes
        .get(cursor)
        .is_some_and(|byte| *byte == b'_' || byte.is_ascii_alphabetic())
    {
        return None;
    }
    cursor += 1;
    while bytes
        .get(cursor)
        .is_some_and(|byte| *byte == b'_' || byte.is_ascii_alphanumeric())
    {
        cursor += 1;
    }
    Some((&source[identifier_start..cursor], cursor))
}

fn path_attributes_in_attribute(source: &str, start: usize, end: usize) -> Vec<RustPathAttribute> {
    let attribute = &source[start + 2..end - 1];
    let bytes = attribute.as_bytes();
    let mut attributes = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if let Some(non_code_end) = rust_non_code_end(attribute, cursor) {
            cursor = non_code_end;
            continue;
        }
        let Some((name, next)) = identifier_at(attribute, cursor) else {
            cursor += 1;
            continue;
        };
        cursor = next;
        if name != "path" {
            continue;
        }
        let equals = skip_rust_trivia(attribute, cursor);
        if bytes.get(equals) != Some(&b'=') {
            attributes.push(RustPathAttribute::Unsupported);
            continue;
        }
        let literal_start = skip_rust_trivia(attribute, equals + 1);
        if bytes.get(literal_start) != Some(&b'"') {
            attributes.push(RustPathAttribute::Unsupported);
            continue;
        }
        let Some(literal_end) = quoted_literal_end(bytes, literal_start, b'"') else {
            attributes.push(RustPathAttribute::Unsupported);
            continue;
        };
        let value = &attribute[literal_start + 1..literal_end - 1];
        if value.contains('\\') {
            attributes.push(RustPathAttribute::Unsupported);
        } else {
            attributes.push(RustPathAttribute::Literal(value.to_owned()));
        }
        cursor = literal_end;
    }
    attributes
}

pub(crate) fn rust_path_attributes(source: &str) -> Vec<RustPathAttribute> {
    rust_attribute_starts(source)
        .into_iter()
        .flat_map(|start| {
            attribute_end(source, start)
                .map(|end| path_attributes_in_attribute(source, start, end))
                .unwrap_or_default()
        })
        .collect()
}

fn matching_brace(source: &str, open: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    if bytes.get(open) != Some(&b'{') {
        return None;
    }
    let mut depth = 1usize;
    let mut cursor = open + 1;
    while cursor < bytes.len() {
        if let Some(end) = rust_non_code_end(source, cursor) {
            cursor = end;
            continue;
        }
        match bytes[cursor] {
            b'{' => depth = depth.checked_add(1)?,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(cursor);
                }
            }
            _ => {}
        }
        cursor += 1;
    }
    None
}

struct ParsedModule {
    declaration_start: usize,
    name: String,
    positive_test: bool,
    explicit_paths: Vec<String>,
    is_private: bool,
    is_unconditional: bool,
    uses_default_path: bool,
    body: Option<(usize, usize)>,
    end: usize,
}

fn item_after_visibility(source: &str, start: usize) -> Option<(bool, &str, usize)> {
    if source.as_bytes().get(start..start + 2) == Some(b"r#") {
        return None;
    }
    let (first, mut cursor) = identifier_at(source, start)?;
    if first != "pub" {
        return Some((true, first, cursor));
    }
    cursor = skip_rust_trivia(source, cursor);
    if source.as_bytes().get(cursor) == Some(&b'(') {
        cursor += matching_parenthesis(&source[cursor..], 0)? + 1;
    }
    cursor = skip_rust_trivia(source, cursor);
    if source.as_bytes().get(cursor..cursor + 2) == Some(b"r#") {
        return None;
    }
    let (item, next) = identifier_at(source, cursor)?;
    Some((false, item, next))
}

fn module_at(source: &str, start: usize) -> Option<ParsedModule> {
    let mut cursor = start;
    let mut positive_test = false;
    let mut explicit_paths = Vec::new();
    let mut is_unconditional = true;
    let mut uses_default_path = true;
    while source.as_bytes().get(cursor..cursor + 2) == Some(b"#[") {
        let end = attribute_end(source, cursor)?;
        positive_test |= cfg_attribute_has_positive_test(source, cursor, end);
        is_unconditional &= !attribute_controls_cfg(source, cursor, end);
        let path_attributes = path_attributes_in_attribute(source, cursor, end);
        uses_default_path &= path_attributes.is_empty();
        explicit_paths.extend(path_attributes.into_iter().filter_map(
            |attribute| match attribute {
                RustPathAttribute::Literal(path) => Some(path),
                RustPathAttribute::Unsupported => None,
            },
        ));
        cursor = skip_rust_trivia(source, end);
    }

    let declaration_start = cursor;
    let (is_private, item, next) = item_after_visibility(source, cursor)?;
    if item != "mod" {
        return None;
    }
    cursor = next;

    cursor = skip_rust_trivia(source, cursor);
    let (name, next) = identifier_at(source, cursor)?;
    cursor = skip_rust_trivia(source, next);
    explicit_paths.sort();
    explicit_paths.dedup();
    let (body, end) = match source.as_bytes().get(cursor) {
        Some(b';') => (None, cursor + 1),
        Some(b'{') => {
            let close = matching_brace(source, cursor)?;
            (Some((cursor + 1, close)), close + 1)
        }
        _ => return None,
    };
    Some(ParsedModule {
        declaration_start,
        name: name.to_owned(),
        positive_test,
        explicit_paths,
        is_private,
        is_unconditional,
        uses_default_path,
        body,
        end,
    })
}

fn scope_inner_conditions(source: &str, start: usize, end: usize) -> (bool, bool) {
    let mut cursor = skip_rust_trivia(source, start);
    let mut positive_test = false;
    let mut unconditional = true;
    while cursor < end && source.as_bytes().get(cursor..cursor + 3) == Some(b"#![") {
        let Some(attribute_end) = attribute_end(source, cursor) else {
            return (positive_test, false);
        };
        positive_test |= cfg_attribute_has_positive_test(source, cursor, attribute_end);
        unconditional &= !attribute_controls_cfg(source, cursor, attribute_end);
        cursor = skip_rust_trivia(source, attribute_end);
    }
    (positive_test, unconditional)
}

fn test_function_at(source: &str, start: usize) -> Option<(String, usize)> {
    let mut cursor = start;
    let mut is_test = false;
    while source.as_bytes().get(cursor..cursor + 2) == Some(b"#[") {
        let end = attribute_end(source, cursor)?;
        let attribute = &source[cursor + 2..end - 1];
        is_test |=
            identifier_at(attribute, skip_rust_trivia(attribute, 0)).is_some_and(|(name, next)| {
                name == "test" && skip_rust_trivia(attribute, next) == attribute.len()
            });
        cursor = skip_rust_trivia(source, end);
    }
    let (_, mut item, mut next) = item_after_visibility(source, cursor)?;
    if item == "async" {
        (item, next) = identifier_at(source, skip_rust_trivia(source, next))?;
    }
    if !is_test || item != "fn" {
        return None;
    }
    let (name, end) = identifier_at(source, skip_rust_trivia(source, next))?;
    Some((name.to_owned(), end))
}

#[derive(Default)]
struct ModuleDeclarations {
    cfg_test: Vec<(usize, ExternalCfgTestModule)>,
    external: Vec<(usize, ExternalRustModule)>,
    inline: Vec<(usize, String)>,
    inline_cfg_test: Vec<(usize, String)>,
    test_functions: Vec<(usize, RustTestFunction)>,
}

fn collect_external_modules(
    source: &str,
    start: usize,
    end: usize,
    inherited_test: bool,
    inherited_unconditional: bool,
    module_context: &mut Vec<String>,
    declarations: &mut ModuleDeclarations,
) {
    let bytes = source.as_bytes();
    let (inner_test, inner_unconditional) = scope_inner_conditions(source, start, end);
    let inherited_test = inherited_test || inner_test;
    let inherited_unconditional = inherited_unconditional && inner_unconditional;
    let mut cursor = start;
    while cursor < end {
        if let Some(non_code_end) = rust_non_code_end(source, cursor) {
            cursor = non_code_end;
            continue;
        }
        if let Some(module) = module_at(source, cursor) {
            let effective_test = inherited_test || module.positive_test;
            if let Some((body_start, body_end)) = module.body {
                declarations
                    .inline
                    .push((module.declaration_start, module.name.clone()));
                if module.positive_test {
                    declarations
                        .inline_cfg_test
                        .push((module.declaration_start, module.name.clone()));
                }
                module_context.push(module.name);
                collect_external_modules(
                    source,
                    body_start,
                    body_end,
                    effective_test,
                    inherited_unconditional && module.is_unconditional,
                    module_context,
                    declarations,
                );
                module_context.pop();
            } else {
                if effective_test {
                    declarations.cfg_test.push((
                        module.declaration_start,
                        ExternalCfgTestModule {
                            name: module.name.clone(),
                            explicit_paths: module.explicit_paths,
                            module_context: module_context.clone(),
                        },
                    ));
                }
                declarations.external.push((
                    module.declaration_start,
                    ExternalRustModule {
                        name: module.name,
                        module_context: module_context.clone(),
                        is_private: module.is_private,
                        is_unconditional: inherited_unconditional && module.is_unconditional,
                        uses_default_path: module.uses_default_path,
                    },
                ));
            }
            cursor = module.end;
            continue;
        }
        if let Some((name, next)) = test_function_at(source, cursor) {
            declarations.test_functions.push((
                cursor,
                RustTestFunction {
                    name,
                    module_context: module_context.clone(),
                },
            ));
            cursor = next;
            continue;
        }
        if bytes.get(cursor) == Some(&b'{') {
            cursor = matching_brace(source, cursor).map_or(end, |close| close + 1);
        } else {
            cursor += 1;
        }
    }
}

fn module_declarations(source: &str) -> ModuleDeclarations {
    let mut declarations = ModuleDeclarations::default();
    collect_external_modules(
        source,
        0,
        source.len(),
        false,
        true,
        &mut Vec::new(),
        &mut declarations,
    );
    declarations
}

fn sorted_declarations<T>(mut declarations: Vec<(usize, T)>) -> Vec<T> {
    declarations.sort_by_key(|(start, _)| *start);
    declarations.into_iter().map(|(_, value)| value).collect()
}

pub(crate) fn external_cfg_test_modules(source: &str) -> Vec<ExternalCfgTestModule> {
    sorted_declarations(module_declarations(source).cfg_test)
}

pub(crate) fn external_rust_modules(source: &str) -> Vec<ExternalRustModule> {
    sorted_declarations(module_declarations(source).external)
}

pub(crate) fn rust_test_functions(source: &str) -> Vec<RustTestFunction> {
    sorted_declarations(module_declarations(source).test_functions)
}

pub(crate) fn source_include_macro_count(source: &str) -> usize {
    let bytes = source.as_bytes();
    let mut count = 0usize;
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if let Some(end) = rust_non_code_end(source, cursor) {
            cursor = end;
            continue;
        }
        let Some((identifier, next)) = identifier_at(source, cursor) else {
            cursor += 1;
            continue;
        };
        cursor = next;
        if identifier == "include" {
            let marker = skip_rust_trivia(source, cursor);
            let is_invocation = bytes.get(marker) == Some(&b'!');
            let is_alias =
                identifier_at(source, marker).is_some_and(|(identifier, _)| identifier == "as");
            if is_invocation || is_alias {
                count += 1;
            }
        }
    }
    count
}

fn code_only(source: &str) -> String {
    let mut code = source.as_bytes().to_vec();
    let mut cursor = 0usize;
    while cursor < code.len() {
        if let Some(end) = rust_non_code_end(source, cursor) {
            for byte in &mut code[cursor..end] {
                if *byte != b'\n' {
                    *byte = b' ';
                }
            }
            cursor = end;
        } else {
            cursor += 1;
        }
    }
    String::from_utf8(code).expect("masking Rust non-code bytes must preserve UTF-8")
}

fn identifier_occurrences(source: &str, needle: &str) -> usize {
    let mut count = 0usize;
    let mut cursor = 0usize;
    while cursor < source.len() {
        if let Some((identifier, next)) = identifier_at(source, cursor) {
            count += usize::from(identifier == needle);
            cursor = next;
        } else {
            cursor += 1;
        }
    }
    count
}

fn lint_path_occurrences(source: &str, namespace: &str, lint: &str) -> usize {
    let bytes = source.as_bytes();
    let mut count = 0usize;
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        let Some((identifier, next)) = identifier_at(source, cursor) else {
            cursor += 1;
            continue;
        };
        cursor = next;
        if identifier != namespace {
            continue;
        }
        let first_colon = skip_rust_trivia(source, cursor);
        if bytes.get(first_colon) != Some(&b':') {
            continue;
        }
        let second_colon = skip_rust_trivia(source, first_colon + 1);
        if bytes.get(second_colon) != Some(&b':') {
            continue;
        }
        let lint_start = skip_rust_trivia(source, second_colon + 1);
        let Some((candidate, next)) = identifier_at(source, lint_start) else {
            continue;
        };
        count += usize::from(candidate == lint);
        cursor = next;
    }
    count
}

pub(crate) fn lint_suppression_counts(source: &str) -> LintSuppressionCounts {
    let code = code_only(source);
    let bytes = code.as_bytes();
    let mut counts = LintSuppressionCounts::default();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        let Some((identifier, next)) = identifier_at(&code, cursor) else {
            cursor += 1;
            continue;
        };
        cursor = next;
        if identifier != "allow" && identifier != "expect" {
            continue;
        }
        let open = skip_rust_trivia(&code, cursor);
        if bytes.get(open) != Some(&b'(') {
            continue;
        }
        let Some(close) = matching_parenthesis(&code, open) else {
            continue;
        };
        let body = &code[open + 1..close];
        counts.too_many_arguments += lint_path_occurrences(body, "clippy", "too_many_arguments");
        counts.broad += lint_path_occurrences(body, "clippy", "all");
        counts.broad += identifier_occurrences(body, "warnings");
        cursor = close + 1;
    }
    counts
}

pub(crate) fn inline_cfg_test_module_names(source: &str) -> Vec<String> {
    sorted_declarations(module_declarations(source).inline_cfg_test)
}

pub(crate) fn inline_rust_module_names(source: &str) -> Vec<String> {
    sorted_declarations(module_declarations(source).inline)
}
