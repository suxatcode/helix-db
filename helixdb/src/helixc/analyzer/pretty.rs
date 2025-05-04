//! Lightweight pretty-printer for `Diagnostic`.
//
//! Produces "rustc-style" output, e.g.
//! ```text
//! error: unknown node type `Post`
//!   ┌─ schema.hx:12:7
//!   │
//! 12 │     To: Post,
//!    │       ^^^^ declare `N::Post` above
//! ```

use super::analyzer::Diagnostic;
use super::analyzer::DiagnosticSeverity;
use colored::*;

/// Render a single diagnostic plus a code snippet.
///
/// * `src` – the entire text parsed by `HelixParser::parse_source`.
/// * `filename` – label in the left gutter (e.g. `"query.hx"`).
pub fn render(diag: &Diagnostic, src: &str, filename: &str) -> String {
    // 1-based → 0-based index
    let line_idx = diag.location.start.line.saturating_sub(1);
    let code_line = src.lines().nth(line_idx).unwrap_or("");

    let caret_pad = " ".repeat(diag.location.start.column.saturating_sub(1));

    let mut out = String::new();

    // Color the error/warning label based on severity
    let severity_str = match diag.severity {
        DiagnosticSeverity::Error => "error".red().bold(),
        DiagnosticSeverity::Warning => "warning".yellow().bold(),
        DiagnosticSeverity::Info => "info".blue().bold(),
        DiagnosticSeverity::Hint => "hint".green().bold(),
        DiagnosticSeverity::Empty => "note".normal(),
    };

    out.push_str(&format!("{}: {}\n", severity_str, diag.message));

    // Color the location line in red for errors, yellow for warnings
    let location_color = match diag.severity {
        DiagnosticSeverity::Error => format!(
            "{}{}",
            "  ┌─ ".red(),
            format!(
                "{filename}:{line}:{col}",
                filename = filename,
                line = diag.location.start.line,
                col = diag.location.start.column
            )
            .bold(),
        ),
        DiagnosticSeverity::Warning => format!(
            "{}{}",
            "  ┌─ ".yellow(),
            format!(
                "{filename}:{line}:{col}",
                filename = filename,
                line = diag.location.start.line,
                col = diag.location.start.column
            )
            .bold(),
        ),
        _ => format!(
            "{}{}",
            "  ┌─ ".normal(),
            format!(
                "{filename}:{line}:{col}",
                filename = filename,
                line = diag.location.start.line,
                col = diag.location.start.column
            )
            .bold(),
        ),
    };

    out.push_str(&format!("{}\n", location_color));

    // Color the vertical bars and line numbers based on severity
    let (line_num_color, vertical_bar) = match diag.severity {
        DiagnosticSeverity::Error => (format!("{:>2}", diag.location.start.line).red(), "│".red()),
        DiagnosticSeverity::Warning => (
            format!("{:>2}", diag.location.start.line).yellow(),
            "│".yellow(),
        ),
        _ => (
            format!("{:>2}", diag.location.start.line).normal(),
            "│".normal(),
        ),
    };

    out.push_str(&format!("  {} \n", vertical_bar));
    out.push_str(&format!(
        "{} {} {}\n",
        line_num_color, vertical_bar, code_line
    ));
    out.push_str(&format!("  {} {caret_pad}^\n", vertical_bar));

    // Color the hint in green
    if let Some(ref hint) = diag.hint {
        out.push_str(&format!(
            "  = {}: {}\n",
            "help".green().bold(),
            hint.green()
        ));
    }

    out
}
