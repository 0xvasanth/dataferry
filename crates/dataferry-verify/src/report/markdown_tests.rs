//! `report.md` holds what the source and the target held: dead-letter
//! errors and examples, finalize messages, orphan keys, assertion errors.
//! Parsed as Markdown, each value must read back as it was, in its own
//! cell or list item, whatever it contains.

use std::collections::BTreeMap;
use std::time::SystemTime;

use dataferry_registry::Redactor;

use crate::target::DeadLetters;
use crate::verify::{
    AssertionCheck, Counts, DeadLetterCheck, FinalizeCheck, Partitions, RunInfo, Status,
    TableCheck, Verification,
};
use crate::{Checksum, Report};
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

/// Pipes, a backslash, newlines and backticks: each could end a cell, a
/// row or a code span.
const NASTY: &str = "bad | value\nnext line\r\nwith `code` and a \\ and \\| and ``";
/// What a code span shows of it: on one line.
const NASTY_CODE: &str = "bad | value next line with `code` and a \\ and \\| and ``";
/// Each could become HTML, a link, an image, emphasis, strikethrough or an
/// entity, and the backslash at the end could escape what follows.
const MARKUP: &str = "<b>x</b> *em* **st** [a](http://x) ![i][r] &amp; ~~s~~ _u_ __ a_b trail\\";
/// Code that starts and ends with a space and holds a backtick.
const PADDED: &str = " lead `and` trail ";

/// Each could start a heading, a nested list, a block quote, an HTML block
/// or a link reference definition after the list marker.
const PROBLEMS: [&str; 8] = [
    "# not a heading\n- not a list item | nor a cell",
    "1. not an ordered list",
    "2) nor this",
    "+ nor a bullet",
    "> not a quote",
    "[mongo]: http://mongo.example",
    // An HTML block needs no `>` to start.
    "<div class",
    "\n",
];

fn verification() -> Verification {
    let now = SystemTime::now();
    let sum = Checksum { rows: 8, sum: 10 };
    Verification {
        status: Status::Failed,
        run: RunInfo {
            id: "r1".into(),
            config_hash: "abc".into(),
            engine_version: "0.0.1".into(),
            connections: vec![],
        },
        target: "warehouse".into(),
        partitions: Partitions {
            pending: 0,
            leased: 0,
            done: 3,
            failed: 0,
        },
        counts: Counts {
            read: 9,
            dead_lettered: 2,
            written: BTreeMap::new(),
            flows: vec![],
        },
        tables: vec![TableCheck {
            table: "orders".into(),
            flow: "orders".into(),
            written: 8,
            at_write: sum,
            read_back: Some(sum),
            timestamp_digits: BTreeMap::new(),
            error: None,
            passed: true,
        }],
        constraints: vec![],
        assertions: vec![AssertionCheck {
            flow: "orders".into(),
            name: "order_total".into(),
            expression: "sum(orders.total) == source.sum(x | y)".into(),
            outcome: None,
            error: Some(NASTY.into()),
            passed: false,
        }],
        finalize: vec![FinalizeCheck {
            flow: "orders".into(),
            table: "order_items".into(),
            object: "order_items_order_fk".into(),
            message: NASTY.into(),
            orphans: Some(2),
            orphan_keys: vec!["id=1|2".into(), "id=`3`\n".into()],
        }],
        dead_letters: DeadLetterCheck {
            rows: 2,
            reasons: vec![
                DeadLetters {
                    flow: "orders".into(),
                    target: Some("order_items".into()),
                    error: NASTY.into(),
                    count: 1,
                    example: NASTY.into(),
                },
                DeadLetters {
                    flow: "orders".into(),
                    target: None,
                    error: MARKUP.into(),
                    count: 1,
                    example: PADDED.into(),
                },
            ],
            error: None,
            passed: true,
        },
        problems: PROBLEMS.map(Into::into).to_vec(),
        started: now,
        finished: now,
    }
}

/// A cell or list item's content as a reader sees it: text, code (marked
/// `code[...]`), `<br>` as a newline, strong as `strong[...]`.
#[derive(Debug, Default)]
struct Doc {
    /// Per table, its rows (the header first), each a list of cells.
    tables: Vec<Vec<Vec<String>>>,
    /// Every list item's content.
    items: Vec<String>,
    headings: usize,
    lists: usize,
}

fn parse(md: &str) -> Doc {
    let mut doc = Doc::default();
    let mut cell: Option<String> = None;
    let mut item: Option<String> = None;
    let options = Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH;
    for event in Parser::new_ext(md, options) {
        let into = cell.as_mut().or(item.as_mut());
        match event {
            Event::Start(Tag::Table(_)) => doc.tables.push(vec![]),
            Event::Start(Tag::TableHead | Tag::TableRow) => {
                doc.tables.last_mut().unwrap().push(vec![]);
            }
            Event::Start(Tag::TableCell) => cell = Some(String::new()),
            Event::End(TagEnd::TableCell) => {
                let row = doc.tables.last_mut().unwrap().last_mut().unwrap();
                row.push(cell.take().unwrap());
            }
            Event::Start(Tag::Item) => item = Some(String::new()),
            Event::End(TagEnd::Item) => doc.items.push(item.take().unwrap()),
            Event::Start(Tag::Heading { .. }) => doc.headings += 1,
            Event::Start(Tag::List(_)) => doc.lists += 1,
            Event::Start(Tag::Strong) => {
                if let Some(s) = into {
                    s.push_str("strong[");
                }
            }
            Event::End(TagEnd::Strong) => {
                if let Some(s) = into {
                    s.push(']');
                }
            }
            Event::Text(t) => {
                if let Some(s) = into {
                    s.push_str(&t);
                }
            }
            Event::Code(c) => {
                if let Some(s) = into {
                    s.push_str(&format!("code[{c}]"));
                }
            }
            Event::InlineHtml(h) if &*h == "<br>" => {
                if let Some(s) = into {
                    s.push('\n');
                }
            }
            Event::Html(h) | Event::InlineHtml(h) => panic!("HTML in the report: {h:?}\n{md}"),
            Event::Start(
                Tag::Link { .. }
                | Tag::Image { .. }
                | Tag::Emphasis
                | Tag::Strikethrough
                | Tag::BlockQuote(_)
                | Tag::CodeBlock(_),
            ) => {
                panic!("markup from a value: {event:?}\n{md}")
            }
            _ => {}
        }
    }
    doc
}

/// The table whose header is `header`.
fn table<'a>(doc: &'a Doc, header: &[&str]) -> &'a [Vec<String>] {
    let table = doc
        .tables
        .iter()
        .find(|t| t[0] == header)
        .unwrap_or_else(|| panic!("no table {header:?}: {doc:#?}"));
    &table[1..]
}

/// `NASTY` as text in a cell: its line breaks are `<br>`, so `\r\n` reads
/// as one.
fn nasty_text() -> String {
    NASTY.replace("\r\n", "\n")
}

#[test]
fn a_dead_letter_with_pipes_newlines_and_backticks_keeps_every_row_of_its_table() {
    let md = Report::new(&verification()).to_markdown();
    let doc = parse(&md);
    let rows = table(
        &doc,
        &[
            "Flow",
            "Target",
            "Error",
            "Records",
            "Example code[source_id]",
        ],
    );
    assert_eq!(
        rows,
        [
            vec![
                "orders".to_owned(),
                "order_items".into(),
                nasty_text(),
                "1".into(),
                format!("code[{NASTY_CODE}]"),
            ],
            vec![
                "orders".into(),
                "".into(),
                MARKUP.into(),
                "1".into(),
                format!("code[{PADDED}]"),
            ],
        ],
        "{md}"
    );
}

#[test]
fn a_failed_assertion_and_finalize_stay_visible_whatever_their_errors_hold() {
    let md = Report::new(&verification()).to_markdown();
    let doc = parse(&md);
    let rows = table(&doc, &["Flow", "Name", "Assertion", "Values", "Result"]);
    assert_eq!(
        rows,
        [vec![
            "orders".to_owned(),
            "order_total".into(),
            "code[sum(orders.total) == source.sum(x | y)]".into(),
            format!("error: {}", nasty_text()),
            "strong[fail]".into(),
        ]],
        "{md}"
    );
    let rows = table(
        &doc,
        &["Flow", "Table", "Object", "Problem", "Orphaned rows"],
    );
    assert_eq!(
        rows,
        [vec![
            "orders".to_owned(),
            "order_items".into(),
            "order_items_order_fk".into(),
            nasty_text(),
            "2: id=1|2; id=`3`\n".into(),
        ]],
        "{md}"
    );
    // The tables that follow are still tables.
    table(
        &doc,
        &[
            "Table",
            "Committed",
            "Written",
            "Read back",
            "Checksum",
            "Result",
        ],
    );
}

#[test]
fn problems_stay_one_list_item_each() {
    let md = Report::new(&verification()).to_markdown();
    let doc = parse(&md);
    // After the run's six details, each problem as it was written. A
    // problem that is only a line break leaves an empty item.
    let problems: Vec<String> = PROBLEMS
        .iter()
        .map(|p| p.trim_start_matches('\n').to_owned())
        .collect();
    assert_eq!(doc.items[6..], problems, "{md}");
    // The report's own: the title and its sections. None from a problem.
    let sections = md.lines().filter(|l| l.starts_with('#')).count();
    assert_eq!(doc.headings, sections, "{md}");
    // The run's details and the problems: no list nested in an item.
    assert_eq!(doc.lists, 2, "{md}");
}

#[test]
fn long_values_are_cut_short_with_an_ellipsis() {
    let mut v = verification();
    let long = "x".repeat(1_000);
    v.dead_letters.reasons[0].error = long.clone();
    v.dead_letters.reasons[0].example = long.clone();
    let md = Report::new(&v).to_markdown();
    let doc = parse(&md);
    let rows = table(
        &doc,
        &[
            "Flow",
            "Target",
            "Error",
            "Records",
            "Example code[source_id]",
        ],
    );
    let cut = format!("{}…", "x".repeat(199));
    assert_eq!(rows[0][2], cut);
    assert_eq!(rows[0][4], format!("code[{cut}]"));
    // Exactly 200 characters is shown whole; report.json keeps every value
    // whole.
    let whole = "y".repeat(200);
    v.dead_letters.reasons[0].error = whole.clone();
    let doc = parse(&Report::new(&v).to_markdown());
    let rows = table(
        &doc,
        &[
            "Flow",
            "Target",
            "Error",
            "Records",
            "Example code[source_id]",
        ],
    );
    assert_eq!(rows[0][2], whole);
    assert!(Report::new(&v).to_json().contains(&long));
}

#[test]
fn values_are_cut_between_characters_not_inside_one() {
    for ch in ['é', '👍'] {
        let mut v = verification();
        v.dead_letters.reasons[0].error = ch.to_string().repeat(300);
        v.dead_letters.reasons[0].example = format!("{}{ch}", "x".repeat(199));
        let doc = parse(&Report::new(&v).to_markdown());
        let rows = table(
            &doc,
            &[
                "Flow",
                "Target",
                "Error",
                "Records",
                "Example code[source_id]",
            ],
        );
        assert_eq!(rows[0][2], format!("{}…", ch.to_string().repeat(199)));
        // 200 characters, the last of them wide: whole.
        assert_eq!(rows[0][4], format!("code[{}{ch}]", "x".repeat(199)));
    }
}

#[test]
fn a_secret_in_a_value_is_redacted_before_it_is_escaped() {
    // Escaped, `pa|ss_` would read `pa\|ss\_` and slip past a redactor
    // looking for the secret as written.
    let secret = "pa|ss_";
    let mut v = verification();
    v.dead_letters.reasons[0].error = format!("auth failed for {secret}");
    v.finalize[0].message = format!("{secret}\n");
    let dir = tempfile::tempdir().unwrap();
    let (_, md) = Report::new(&v)
        .write(dir.path(), &Redactor::new([secret]))
        .unwrap();
    let md = std::fs::read_to_string(md).unwrap();
    assert!(!md.contains("pa|ss") && !md.contains("pa\\|ss"), "{md}");
    assert!(md.contains("auth failed for \\*\\*\\*"), "{md}");
}
