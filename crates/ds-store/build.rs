//! Generates the two inputs of the schema gate (`schema`, spec §12.2):
//!
//! - `required_migration.rs`, `schema::REQUIRED_MIGRATION` (plan task
//!   1A.12): the version of the newest migration in
//!   `crates/ds-store/migrations`, the directory `migrate`'s
//!   `sqlx::migrate!()` embeds (plan task 1B.1 moved it here from
//!   `crates/api`). Generated rather than hand-written so adding a migration
//!   needs no second edit, and so it is, by construction, the newest
//!   migration built into the same binary.
//! - `required_privileges.rs` (plan task 1B.2): for every role in the
//!   chart's `db-grants.yaml`, the table privileges that file gives it, so
//!   the gate can check one `has_table_privilege` per required table for
//!   the calling service's role. Baked in at build time so the binary
//!   checks the grants it was built against, as with the migration.
//!
//! The `rerun-if-changed` on the migrations directory also makes cargo
//! rebuild this crate, and so re-embed the migrations, when a migration is
//! added.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::{env, fs};

/// The migrations directory, relative to this crate.
const MIGRATIONS: &str = "migrations";
/// The grants, relative to this crate (the whole repository is in every
/// image's build context).
const GRANTS: &str = "../../charts/distant-signal/files/db-grants.yaml";

/// The classes of table the `read_shared` group may SELECT
/// (`READ_SHARED_CLASSES` in `scripts/gen-db-grants.py`).
const READ_SHARED_CLASSES: &[&str] = &["shared-train", "ingest", "derived", "reference"];

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap_or_default());
    println!("cargo::rerun-if-changed=build.rs");

    let dir = manifest_dir.join(MIGRATIONS);
    // A directory: cargo reruns this when any file in it changes.
    println!("cargo::rerun-if-changed={}", dir.display());
    let version = newest_version(&dir)
        .unwrap_or_else(|err| panic!("reading the migrations in {}: {err}", dir.display()));
    fs::write(out.join("required_migration.rs"), format!("{version}\n"))
        .unwrap_or_else(|err| panic!("writing required_migration.rs: {err}"));

    let grants = manifest_dir.join(GRANTS);
    println!("cargo::rerun-if-changed={}", grants.display());
    let text = fs::read_to_string(&grants)
        .unwrap_or_else(|err| panic!("reading {}: {err}", grants.display()));
    let privileges =
        role_privileges(&text).unwrap_or_else(|err| panic!("parsing {}: {err}", grants.display()));
    fs::write(out.join("required_privileges.rs"), render(&privileges))
        .unwrap_or_else(|err| panic!("writing required_privileges.rs: {err}"));
}

/// The largest version among the migrations sqlx would apply: the leading
/// digits of each `<version>_<description>.sql`, skipping `.down.sql`
/// (reverts, which `migrate!` does not run forwards).
fn newest_version(dir: &Path) -> Result<i64, String> {
    let mut newest = None;
    for entry in fs::read_dir(dir).map_err(|err| err.to_string())? {
        let name = entry.map_err(|err| err.to_string())?.file_name();
        let Some(name) = name.to_str() else { continue };
        let is_sql = Path::new(name).extension().is_some_and(|ext| ext == "sql");
        if !is_sql || name.ends_with(".down.sql") {
            continue;
        }
        let Some((version, _)) = name.split_once('_') else {
            return Err(format!("{name} is not <version>_<description>.sql"));
        };
        let version: i64 = version
            .parse()
            .map_err(|err| format!("{name}: version {version:?}: {err}"))?;
        newest = newest.max(Some(version));
    }
    newest.ok_or_else(|| "no migrations".to_owned())
}

/// One required privilege: (table, privilege, column).
type Privilege = (String, &'static str, Option<String>);

/// Role key -> the privileges db-grants.yaml gives it: its own grants in
/// `tables` and `views`, plus SELECT on every `READ_SHARED_CLASSES` table
/// when it is in the `read_shared` group.
///
/// db-grants.yaml is read by a small parser for the subset of YAML it
/// uses (no YAML crate at build time): top-level sections; under `roles`,
/// block-style entries whose `groups` is a flow list; under `tables` and
/// `views`, one flow mapping per line. Anything else in those sections
/// fails the build with the line, so a new shape is noticed rather than
/// skipped. `scripts/gen-db-grants.py` (with `PyYAML`) stays the full
/// validator.
fn role_privileges(text: &str) -> Result<BTreeMap<String, BTreeSet<Privilege>>, String> {
    let mut roles: BTreeMap<String, BTreeSet<Privilege>> = BTreeMap::new();
    let mut read_shared: BTreeSet<String> = BTreeSet::new();
    let mut section = "";
    let mut role = String::new();
    for (number, raw) in text.lines().enumerate() {
        let line = strip_comment(raw);
        if line.trim().is_empty() {
            continue;
        }
        let at = |err: String| format!("line {}: {err}: {raw}", number + 1);
        let indent = line.len() - line.trim_start().len();
        let line = line.trim();
        if indent == 0 {
            let (key, rest) = line
                .split_once(':')
                .ok_or_else(|| at("expected `key:`".to_owned()))?;
            section = match key {
                "roles" => "roles",
                "tables" | "views" => {
                    // `views: {}`: empty, in flow style.
                    if !matches!(rest.trim(), "" | "{}") {
                        return Err(at("expected an indented block or {}".to_owned()));
                    }
                    if roles.is_empty() {
                        return Err(at("expected `roles` before this section".to_owned()));
                    }
                    if key == "tables" { "tables" } else { "views" }
                }
                _ => "",
            };
            continue;
        }
        match section {
            "roles" if indent == 2 => {
                let key = line
                    .strip_suffix(':')
                    .ok_or_else(|| at("expected `<role>:`".to_owned()))?;
                key.clone_into(&mut role);
                roles.entry(role.clone()).or_default();
            }
            "roles" => {
                if let Some(groups) = line.strip_prefix("groups:") {
                    let Flow::List(groups) = parse_flow(groups).map_err(at)? else {
                        return Err(at("groups: expected a flow list".to_owned()));
                    };
                    if groups
                        .iter()
                        .any(|g| matches!(g, Flow::Scalar(g) if g == "read_shared"))
                    {
                        read_shared.insert(role.clone());
                    }
                }
            }
            "tables" | "views" if indent == 2 => {
                let (table, body) = line
                    .split_once(':')
                    .ok_or_else(|| at("expected `<table>: {...}`".to_owned()))?;
                let class = table_privileges(table.trim(), body, &mut roles).map_err(at)?;
                // The group's SELECT covers tables only, not views.
                if section == "tables" && READ_SHARED_CLASSES.contains(&class.as_str()) {
                    shared_select(&mut roles, table.trim(), &read_shared);
                }
            }
            "tables" | "views" => {
                return Err(at("expected one flow mapping per entry".to_owned()));
            }
            _ => {}
        }
    }
    if roles.is_empty() {
        return Err("no roles".to_owned());
    }
    Ok(roles)
}

/// Adds SELECT on `table` for every role in the `read_shared` group. The
/// `roles` section comes before `tables` in db-grants.yaml, so the group's
/// members are known by then.
fn shared_select(
    roles: &mut BTreeMap<String, BTreeSet<Privilege>>,
    table: &str,
    read_shared: &BTreeSet<String>,
) {
    for role in read_shared {
        roles
            .entry(role.clone())
            .or_default()
            .insert((table.to_owned(), "SELECT", None));
    }
}

/// Adds one table's (or view's) `grants` to `roles`; returns its `class`.
fn table_privileges(
    table: &str,
    body: &str,
    roles: &mut BTreeMap<String, BTreeSet<Privilege>>,
) -> Result<String, String> {
    let Flow::Map(fields) = parse_flow(body)? else {
        return Err("expected a flow mapping".to_owned());
    };
    let class = match fields.iter().find(|(k, _)| k == "class") {
        Some((_, Flow::Scalar(class))) => class.clone(),
        _ => return Err("expected `class: <class>`".to_owned()),
    };
    let grants = match fields.iter().find(|(k, _)| k == "grants") {
        Some((_, Flow::Map(grants))) => grants,
        Some(_) => return Err("grants: expected a mapping".to_owned()),
        None => return Ok(class),
    };
    for (role, spec) in grants {
        // One spec, or a list of them (each with its own columns).
        let specs: Vec<&Flow> = match spec {
            Flow::List(items) => items.iter().collect(),
            other => vec![other],
        };
        for spec in specs {
            let (letters, columns) = grant_spec(role, spec)?;
            add_privileges(roles, role, table, &letters, &columns)?;
        }
    }
    Ok(class)
}

/// One grant spec: `SIUD` letters, or `{privileges: ..., columns: [...]}`.
fn grant_spec(role: &str, spec: &Flow) -> Result<(String, Vec<String>), String> {
    match spec {
        Flow::Scalar(letters) => Ok((letters.clone(), Vec::new())),
        Flow::Map(body) => {
            let letters = match body.iter().find(|(k, _)| k == "privileges") {
                Some((_, Flow::Scalar(letters))) => letters.clone(),
                _ => return Err(format!("{role}: expected `privileges: <letters>`")),
            };
            let columns = match body.iter().find(|(k, _)| k == "columns") {
                Some((_, Flow::List(columns))) => columns
                    .iter()
                    .map(|c| match c {
                        Flow::Scalar(c) => Ok(c.clone()),
                        _ => Err(format!("{role}.columns: expected names")),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                _ => return Err(format!("{role}: expected `columns: [...]`")),
            };
            Ok((letters, columns))
        }
        Flow::List(_) => Err(format!("{role}: expected privileges")),
    }
}

/// Adds `role`'s `letters` on `table` (on each of `columns`, if any).
fn add_privileges(
    roles: &mut BTreeMap<String, BTreeSet<Privilege>>,
    role: &str,
    table: &str,
    letters: &str,
    columns: &[String],
) -> Result<(), String> {
    let entry = roles
        .get_mut(role)
        .ok_or_else(|| format!("unknown role {role}"))?;
    for letter in letters.chars() {
        let privilege = match letter {
            'S' => "SELECT",
            'I' => "INSERT",
            'U' => "UPDATE",
            'D' => "DELETE",
            _ => return Err(format!("{role}: unknown privilege {letter:?}")),
        };
        // DELETE is table-level only; Postgres has no column DELETE.
        if columns.is_empty() || privilege == "DELETE" {
            entry.insert((table.to_owned(), privilege, None));
        } else {
            for column in columns {
                entry.insert((table.to_owned(), privilege, Some(column.clone())));
            }
        }
    }
    Ok(())
}

/// Drops a trailing ` # comment` (the file's scalars never contain `#`).
fn strip_comment(line: &str) -> &str {
    if line.trim_start().starts_with('#') {
        return "";
    }
    line.find(" #").map_or(line, |at| &line[..at])
}

/// A YAML flow value: `{k: v, ...}`, `[a, ...]` or a plain or quoted scalar.
#[derive(Debug)]
enum Flow {
    Scalar(String),
    Map(Vec<(String, Flow)>),
    List(Vec<Flow>),
}

fn parse_flow(text: &str) -> Result<Flow, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut at = 0;
    let value = flow_value(&chars, &mut at)?;
    skip_space(&chars, &mut at);
    if at != chars.len() {
        return Err(format!(
            "unexpected {:?}",
            chars[at..].iter().collect::<String>()
        ));
    }
    Ok(value)
}

fn skip_space(chars: &[char], at: &mut usize) {
    while chars.get(*at).is_some_and(|c| c.is_whitespace()) {
        *at += 1;
    }
}

fn flow_value(chars: &[char], at: &mut usize) -> Result<Flow, String> {
    skip_space(chars, at);
    match chars.get(*at) {
        Some('{') => {
            *at += 1;
            let mut fields = Vec::new();
            loop {
                skip_space(chars, at);
                if chars.get(*at) == Some(&'}') {
                    *at += 1;
                    return Ok(Flow::Map(fields));
                }
                let key = scalar(chars, at, &[':'])?;
                if chars.get(*at) != Some(&':') {
                    return Err(format!("{key}: expected `:`"));
                }
                *at += 1;
                fields.push((key, flow_value(chars, at)?));
                separator(chars, at, '}')?;
            }
        }
        Some('[') => {
            *at += 1;
            let mut items = Vec::new();
            loop {
                skip_space(chars, at);
                if chars.get(*at) == Some(&']') {
                    *at += 1;
                    return Ok(Flow::List(items));
                }
                items.push(flow_value(chars, at)?);
                separator(chars, at, ']')?;
            }
        }
        Some(_) => scalar(chars, at, &[',', '}', ']']).map(Flow::Scalar),
        None => Err("expected a value".to_owned()),
    }
}

/// After an item: `,` (consumed) or the closing bracket (left in place).
fn separator(chars: &[char], at: &mut usize, close: char) -> Result<(), String> {
    skip_space(chars, at);
    match chars.get(*at) {
        Some(',') => {
            *at += 1;
            Ok(())
        }
        Some(c) if *c == close => Ok(()),
        _ => Err(format!("expected `,` or `{close}`")),
    }
}

fn scalar(chars: &[char], at: &mut usize, ends: &[char]) -> Result<String, String> {
    skip_space(chars, at);
    if let Some(quote @ ('"' | '\'')) = chars.get(*at).copied() {
        *at += 1;
        let start = *at;
        while chars.get(*at).is_some_and(|c| *c != quote) {
            *at += 1;
        }
        if *at == chars.len() {
            return Err("unterminated quote".to_owned());
        }
        let value = chars[start..*at].iter().collect();
        *at += 1;
        return Ok(value);
    }
    let start = *at;
    while chars.get(*at).is_some_and(|c| !ends.contains(c)) {
        *at += 1;
    }
    let value: String = chars[start..*at].iter().collect();
    let value = value.trim();
    if value.is_empty() {
        return Err("expected a scalar".to_owned());
    }
    Ok(value.to_owned())
}

/// `&[(role, &[RequiredPrivilege { .. }, ..]), ..]`, sorted, for
/// `include!` in `schema.rs`.
fn render(roles: &BTreeMap<String, BTreeSet<Privilege>>) -> String {
    let mut out = String::from("&[\n");
    for (role, privileges) in roles {
        let _ = writeln!(out, "    ({role:?}, &[");
        for (table, privilege, column) in privileges {
            let _ = writeln!(
                out,
                "        RequiredPrivilege {{ table: {table:?}, privilege: {privilege:?}, column: {column:?} }},"
            );
        }
        out.push_str("    ]),\n");
    }
    out.push_str("]\n");
    out
}
