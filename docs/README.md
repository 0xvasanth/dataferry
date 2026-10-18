# docs/

- `src/`: the user guide, an [mdBook](https://rust-lang.github.io/mdBook/)
  published at <https://0xvasanth.github.io/dataferry/> by
  `.github/workflows/docs.yml`. `src/SUMMARY.md` is its table of contents.
- `decisions/`: the architecture decision records. The guide includes
  them as they are (`src/reference/decisions/`).
- `config-reference.md`: every config key. A test
  (`tests/config_reference.rs` of the CLI crate) checks it against
  `schema`'s output; the guide includes it (`src/reference/config.md`).

## Building the guide

```sh
cargo install mdbook --locked --version 0.5.4   # the version docs.yml pins
docs/scripts/build-book.sh                      # writes docs/book/
mdbook serve docs                               # live preview
```

`build-book.sh` fails on any mdbook warning or error (a missing
`{{#include}}` is only logged by mdbook) and on any broken relative link or
`#anchor` in the built HTML.

## Keeping it true

Nothing the guide shows is typed by hand where the binary can produce it:

- YAML is never written inline: every ```` ```yaml ```` block
  `{{#include}}`s a file from a config directory that is validated, under
  `src/examples/` (the guide's own examples) or `examples/`.
- Command output, table contents and diagnostics are included from files
  the binary wrote (`src/examples/**/expected.txt`, `validate.txt`,
  `validate.json`, `src/reference/cli/*.txt`), except in the quickstart,
  whose output blocks follow each command. The exception is the
  interrupt-and-resume transcript in `src/running.md` (where an interrupt
  lands depends on timing): it says so, and is re-captured by hand.

`docs/scripts/check-snippets.sh` checks all of it against a built binary,
and with `--live` runs the quickstart as written and every worked example
(`src/examples/<name>/load.sh`) on Docker. `--update` rewrites the
generated files after a deliberate change; review the diff. Its header
says what each step checks.

A new worked example is a directory under `src/examples/` with a `ferry/`
config, its variables in `src/examples/ci.env`, and, to run it, a
`load.sh` (see an existing one) and optionally a `check.sql` whose output
is appended to `expected.txt`.
