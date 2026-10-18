//! Expression errors, pointing into the expression and back at the config
//! file it was written in.

use std::fmt;

use crate::config::{ConfigError, ErrorKind, FieldPath, Location, SourceFile};

/// An expression that doesn't parse, names something that isn't in scope,
/// or doesn't type-check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExprError {
    /// [`ErrorKind::Expr`] or [`ErrorKind::Type`].
    pub kind: ErrorKind,
    pub message: String,
    pub help: Option<String>,
    /// The expression as written.
    pub text: std::sync::Arc<str>,
    /// Where the expression starts in its config file.
    pub location: Location,
    /// The offending part, as char offsets into `text`, when known.
    pub span: Option<(usize, usize)>,
}

impl ExprError {
    pub(crate) fn new(kind: ErrorKind, text: &str, location: &Location, message: String) -> Self {
        Self {
            kind,
            message,
            help: None,
            text: text.into(),
            location: location.clone(),
            span: None,
        }
    }

    pub(crate) fn span(mut self, span: Option<(usize, usize)>) -> Self {
        self.span = span;
        self
    }

    pub(crate) fn help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    /// The expression with the offending part underlined.
    pub fn snippet(&self) -> String {
        let Some((start, end)) = self.span else {
            return format!("   | {}", self.text.trim_end());
        };
        // Show the line of the expression the span starts on.
        let chars: Vec<char> = self.text.chars().collect();
        let line_start = chars[..start.min(chars.len())]
            .iter()
            .rposition(|c| *c == '\n')
            .map_or(0, |i| i + 1);
        let line_end = chars[line_start..]
            .iter()
            .position(|c| *c == '\n')
            .map_or(chars.len(), |i| line_start + i);
        let line: String = chars[line_start..line_end].iter().collect();
        let width = end.min(line_end).saturating_sub(start).max(1);
        format!(
            "   | {}\n   | {}{}",
            line.trim_end(),
            " ".repeat(start - line_start),
            "^".repeat(width)
        )
    }

    /// The error as a config error at `path`. With the config's source
    /// files, the location is the offending part itself rather than the
    /// start of the expression.
    pub fn to_config_error(&self, path: &FieldPath, sources: &[SourceFile]) -> ConfigError {
        let location = match self.span {
            Some((start, _)) => pinpoint(&self.location, &self.text, start, sources),
            None => self.location.clone(),
        };
        ConfigError::new(self.kind, format!("{}\n{}", self.message, self.snippet()))
            .at(&location)
            .path(path)
            .maybe_help(self.help.clone())
    }
}

impl fmt::Display for ExprError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}\n{}", self.location, self.message, self.snippet())?;
        if let Some(help) = &self.help {
            write!(f, "\n  help: {help}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ExprError {}

/// The file location of char `offset` of an expression that starts at
/// `start`. Works when the expression is written verbatim from there: a
/// plain scalar, or a quoted one with no escapes before `offset`. Anything
/// else (folded block scalars, escapes) keeps the start.
fn pinpoint(start: &Location, text: &str, offset: usize, sources: &[SourceFile]) -> Location {
    let Some(source) = sources.iter().find(|s| *s.path == *start.file) else {
        return start.clone();
    };
    let Some(line) = source.text.lines().nth(start.line as usize - 1) else {
        return start.clone();
    };
    let written: String = line.chars().skip(start.column as usize - 1).collect();
    let prefix: String = text.chars().take(offset).collect();
    if prefix.contains('\n') {
        return start.clone();
    }
    let quoted = written.starts_with(['"', '\'']);
    let body = if quoted { &written[1..] } else { &written[..] };
    if !body.starts_with(&prefix) || (quoted && prefix.contains(['\\', '\''])) {
        return start.clone();
    }
    let shift = offset + usize::from(quoted);
    Location::new(
        start.file.clone(),
        start.line,
        start.column + u32::try_from(shift).unwrap_or(0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FileKind;

    fn source(text: &str) -> SourceFile {
        SourceFile {
            path: "flows/f.yaml".into(),
            absolute: "/x/flows/f.yaml".into(),
            kind: FileKind::Flows,
            text: text.into(),
        }
    }

    #[test]
    fn pinpoints_into_plain_and_quoted_scalars() {
        let sources = [source("a: lower(stauts)\nb: \"lower(stauts)\"\n")];
        let plain = Location::new("flows/f.yaml", 1, 4);
        assert_eq!(pinpoint(&plain, "lower(stauts)", 6, &sources).column, 10);
        let quoted = Location::new("flows/f.yaml", 2, 4);
        assert_eq!(pinpoint(&quoted, "lower(stauts)", 6, &sources).column, 11);
    }

    #[test]
    fn keeps_the_start_when_the_text_is_not_verbatim() {
        let sources = [source("a: >\n  lower(\n  x)\n")];
        let start = Location::new("flows/f.yaml", 1, 4);
        assert_eq!(pinpoint(&start, "lower( x)", 7, &sources), start);
    }

    #[test]
    fn snippet_underlines_the_span() {
        let mut e = ExprError::new(
            ErrorKind::Expr,
            "lower(stauts)",
            &Location::new("f", 1, 1),
            "no field".into(),
        );
        e.span = Some((6, 12));
        assert_eq!(e.snippet(), "   | lower(stauts)\n   |       ^^^^^^");
    }
}
