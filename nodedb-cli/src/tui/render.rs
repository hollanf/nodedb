//! TUI rendering with ratatui.
//!
//! Clean layout: status bar, results, prompt. Syntax highlighting in input,
//! completion popup, search mode prompt, scroll indicator.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::highlight;

use super::app::App;

pub fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();

    let input_height = if app.search_mode {
        2u16
    } else {
        (input_line_count(app) + 1) as u16
    };

    let layout = Layout::vertical([
        Constraint::Length(1),            // status bar
        Constraint::Length(1),            // top separator
        Constraint::Min(1),               // results
        Constraint::Length(1),            // bottom separator
        Constraint::Length(input_height), // input
    ])
    .split(area);

    render_status_bar(frame, layout[0], app);
    render_separator(frame, layout[1]);
    render_results(frame, layout[2], app);
    render_separator(frame, layout[3]);

    if app.search_mode {
        render_search_prompt(frame, layout[4], app);
    } else {
        render_input(frame, layout[4], app);
    }

    // Completion popup (floating, above input).
    if app.completion.active {
        render_completion_popup(frame, layout[4], app);
    }
}

fn render_status_bar(frame: &mut Frame, area: Rect, app: &App) {
    let style = Style::default().bg(Color::DarkGray).fg(Color::White);

    let fmt = match app.output_format {
        crate::args::OutputFormat::Table => {
            if app.expanded_mode {
                "expanded"
            } else {
                "table"
            }
        }
        crate::args::OutputFormat::Json => "json",
        crate::args::OutputFormat::Csv => "csv",
    };

    let timing = if let Some(d) = app.last_query_time {
        let ms = d.as_secs_f64() * 1000.0;
        if ms >= 60_000.0 {
            format!(" {}m{}s", d.as_secs() / 60, d.as_secs() % 60)
        } else if ms >= 1000.0 {
            format!(" {:.1}s", d.as_secs_f64())
        } else {
            format!(" {:.1}ms", ms)
        }
    } else {
        String::new()
    };

    let watch = if app.watch_interval.is_some() {
        " [watching]"
    } else {
        ""
    };

    let left = format!(" NodeDB CLI | {}:{}", app.host, app.port);
    let right = format!("fmt:{}{}{} ", fmt, timing, watch);
    let pad = area
        .width
        .saturating_sub(left.len() as u16 + right.len() as u16);

    let line = Line::from(vec![
        Span::styled(&left, style.add_modifier(Modifier::BOLD)),
        Span::styled(" ".repeat(pad as usize), style),
        Span::styled(&right, style),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn render_results(frame: &mut Frame, area: Rect, app: &App) {
    let p = if let Some(ref err) = app.error_message {
        Paragraph::new(err.as_str())
            .style(Style::default().fg(Color::Red))
            .wrap(Wrap { trim: false })
    } else if let Some(ref output) = app.result_output {
        Paragraph::new(output.as_str())
            .wrap(Wrap { trim: false })
            .scroll((app.scroll_offset, 0))
    } else {
        Paragraph::new("Type SQL ending with ; and press Enter. Tab to complete. \\? for help.")
            .style(Style::default().fg(Color::DarkGray))
    };
    frame.render_widget(p, area);

    // Scroll indicator.
    if let Some(ref output) = app.result_output {
        let total_lines = output.lines().count() as u16;
        let visible = area.height;
        if total_lines > visible {
            let indicator = format!(
                " {}-{}/{} ",
                app.scroll_offset + 1,
                (app.scroll_offset + visible).min(total_lines),
                total_lines
            );
            let x = area.x + area.width.saturating_sub(indicator.len() as u16);
            let indicator_area = Rect::new(x, area.y + area.height - 1, indicator.len() as u16, 1);
            frame.render_widget(
                Paragraph::new(indicator).style(Style::default().fg(Color::DarkGray)),
                indicator_area,
            );
        }
    }
}

fn render_separator(frame: &mut Frame, area: Rect) {
    let line = "\u{2500}".repeat(area.width as usize);
    let p = Paragraph::new(line).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(p, area);
}

fn render_input(frame: &mut Frame, area: Rect, app: &App) {
    let prompt = Style::default()
        .fg(Color::Green)
        .add_modifier(Modifier::BOLD);
    let buf = app.input.buffer();

    let mut lines: Vec<Line> = Vec::new();
    if buf.is_empty() {
        lines.push(Line::from(Span::styled("ndb> ", prompt)));
    } else {
        for (i, text) in buf.split('\n').enumerate() {
            let pfx = if i == 0 { "ndb> " } else { "  -> " };
            let mut spans = vec![Span::styled(pfx, prompt)];
            spans.extend(highlight::highlight_line(text));
            lines.push(Line::from(spans));
        }
    }
    frame.render_widget(Paragraph::new(lines), area);

    let cx = cursor_col(buf, app.input.cursor());
    let cy = cursor_row(buf, app.input.cursor());
    frame.set_cursor_position((area.x + 5 + cx as u16, area.y + cy as u16));
}

fn render_search_prompt(frame: &mut Frame, area: Rect, app: &App) {
    let prompt_style = Style::default().fg(Color::Yellow);
    let query_style = Style::default()
        .fg(Color::White)
        .add_modifier(Modifier::BOLD);

    let line = Line::from(vec![
        Span::styled("(reverse-i-search)`", prompt_style),
        Span::styled(&app.search_query, query_style),
        Span::styled("': ", prompt_style),
        Span::raw(app.input.buffer()),
    ]);
    frame.render_widget(Paragraph::new(line), area);

    // Cursor at end of search query.
    let cx = 19 + app.search_query.len() as u16 + 3 + app.input.buffer().len() as u16;
    frame.set_cursor_position((area.x + cx.min(area.width - 1), area.y));
}

fn render_completion_popup(frame: &mut Frame, input_area: Rect, app: &App) {
    let comp = &app.completion;
    if comp.candidates.is_empty() {
        return;
    }

    let max_visible = 8usize.min(comp.candidates.len());
    let max_width = comp
        .candidates
        .iter()
        .map(|c| c.len())
        .max()
        .unwrap_or(10)
        .min(40)
        + 4; // padding

    // Position: above input area, at cursor column.
    let cursor_col = comp.prefix_start as u16 + 5; // +5 for prompt
    let popup_x = input_area.x + cursor_col;
    let popup_y = input_area.y.saturating_sub(max_visible as u16 + 2);
    let popup_w = (max_width as u16).min(input_area.width.saturating_sub(cursor_col));
    let popup_h = max_visible as u16 + 2; // +2 for borders

    let popup_area = Rect::new(popup_x, popup_y, popup_w, popup_h);

    // Clear background.
    frame.render_widget(Clear, popup_area);

    // Build list items.
    let mut lines: Vec<Line> = Vec::new();
    let start = if comp.selected >= max_visible {
        comp.selected - max_visible + 1
    } else {
        0
    };

    for (i, candidate) in comp
        .candidates
        .iter()
        .enumerate()
        .skip(start)
        .take(max_visible)
    {
        let style = if i == comp.selected {
            Style::default().bg(Color::Blue).fg(Color::White)
        } else {
            Style::default().fg(Color::White)
        };
        lines.push(Line::styled(format!(" {candidate} "), style));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .style(Style::default().bg(Color::Black));

    frame.render_widget(Paragraph::new(lines).block(block), popup_area);
}

fn input_line_count(app: &App) -> usize {
    app.input.buffer().split('\n').count().clamp(1, 6)
}

fn cursor_col(buffer: &str, byte_pos: usize) -> usize {
    let before = &buffer[..byte_pos];
    match before.rfind('\n') {
        Some(nl) => before.len() - nl - 1,
        None => before.len(),
    }
}

fn cursor_row(buffer: &str, byte_pos: usize) -> usize {
    buffer[..byte_pos].matches('\n').count()
}
