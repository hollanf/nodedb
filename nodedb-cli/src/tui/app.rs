//! TUI application state and event loop.

use std::io::Stdout;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use nodedb_client::NativeClient;

use crate::args::OutputFormat;
use crate::completion::{CompletionState, Completor};
use crate::format;
use crate::history::History;
use crate::metacommand;

use super::input::InputState;
use super::render;

/// Actions that require terminal access (can't be done inside handle_key).
enum UiAction {
    None,
    OpenEditor,
}

/// Main TUI application.
pub struct App {
    pub client: NativeClient,
    pub input: InputState,
    pub result_output: Option<String>,
    pub error_message: Option<String>,
    pub output_format: OutputFormat,
    pub last_query_time: Option<Duration>,
    pub host: String,
    pub port: u16,
    pub scroll_offset: u16,
    pub show_timing: bool,
    pub expanded_mode: bool,
    pub last_query: Option<String>,
    pub output_file: Option<PathBuf>,
    pub history: History,
    pub running: bool,
    // Completion
    pub completor: Completor,
    pub completion: CompletionState,
    // History search (Ctrl+R)
    pub search_mode: bool,
    pub search_query: String,
    pub search_match_index: usize,
    // Watch mode
    pub watch_interval: Option<Duration>,
    pub watch_last_exec: Option<Instant>,
}

impl App {
    pub fn new(
        client: NativeClient,
        format: OutputFormat,
        host: String,
        port: u16,
        timing: bool,
        output_file: Option<PathBuf>,
    ) -> Self {
        Self {
            client,
            input: InputState::new(),
            result_output: None,
            error_message: None,
            output_format: format,
            last_query_time: None,
            host,
            port,
            scroll_offset: 0,
            show_timing: timing,
            expanded_mode: false,
            last_query: None,
            output_file,
            history: History::load(),
            running: true,
            completor: Completor::new(),
            completion: CompletionState::empty(),
            search_mode: false,
            search_query: String::new(),
            search_match_index: 0,
            watch_interval: None,
            watch_last_exec: None,
        }
    }

    /// Run the TUI event loop.
    pub async fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> crate::error::CliResult<()> {
        // Refresh collection names for tab completion.
        self.completor.refresh_collections(&self.client).await;

        while self.running {
            terminal.draw(|frame| render::render(frame, self))?;

            // Watch mode: re-execute query on interval.
            if let (Some(interval), Some(last)) = (self.watch_interval, self.watch_last_exec)
                && last.elapsed() >= interval
                && let Some(query) = self.last_query.clone()
            {
                self.execute_sql(&query).await;
                self.watch_last_exec = Some(Instant::now());
            }

            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
            {
                let action = self.handle_key(key).await;
                if let UiAction::OpenEditor = action {
                    self.open_external_editor(terminal)?;
                }
            }
        }

        self.history.save();
        Ok(())
    }

    async fn handle_key(&mut self, key: KeyEvent) -> UiAction {
        // Cancel watch mode on any key.
        if self.watch_interval.is_some() {
            self.watch_interval = None;
            self.watch_last_exec = None;
        }

        // Search mode handles keys differently.
        if self.search_mode {
            self.handle_search_key(key);
            return UiAction::None;
        }

        // Completion popup handles some keys.
        if self.completion.active {
            match key.code {
                KeyCode::Tab => {
                    self.completion.next();
                    self.apply_completion();
                    return UiAction::None;
                }
                KeyCode::BackTab => {
                    self.completion.prev();
                    self.apply_completion();
                    return UiAction::None;
                }
                KeyCode::Esc => {
                    self.completion = CompletionState::empty();
                    return UiAction::None;
                }
                KeyCode::Enter => {
                    self.apply_completion();
                    self.completion = CompletionState::empty();
                    // Don't execute — let user continue typing.
                    return UiAction::None;
                }
                _ => {
                    // Dismiss completion on any other key, then process normally.
                    self.completion = CompletionState::empty();
                }
            }
        }

        match (key.modifiers, key.code) {
            // Quit.
            (KeyModifiers::CONTROL, KeyCode::Char('c')) => {
                if self.input.is_empty() {
                    self.running = false;
                } else {
                    self.input.take();
                }
            }
            (KeyModifiers::CONTROL, KeyCode::Char('d')) => {
                if self.input.is_empty() {
                    self.running = false;
                }
            }

            // Execute.
            (KeyModifiers::NONE | KeyModifiers::SHIFT, KeyCode::Enter) => {
                if self.input.buffer().trim_start().starts_with('\\')
                    || self.input.ends_with_semicolon()
                {
                    return self.execute_input().await;
                } else {
                    self.input.newline();
                }
            }
            (KeyModifiers::CONTROL, KeyCode::Enter) => {
                if !self.input.is_empty() {
                    return self.execute_input().await;
                }
            }

            // Tab completion.
            (KeyModifiers::NONE, KeyCode::Tab) => {
                self.trigger_completion();
            }

            // History search.
            (KeyModifiers::CONTROL, KeyCode::Char('r')) => {
                self.search_mode = true;
                self.search_query.clear();
                self.search_match_index = 0;
            }

            // Line editing.
            (KeyModifiers::NONE, KeyCode::Backspace) => self.input.backspace(),
            (KeyModifiers::NONE, KeyCode::Delete) => self.input.delete(),
            (KeyModifiers::NONE, KeyCode::Left) => self.input.move_left(),
            (KeyModifiers::NONE, KeyCode::Right) => self.input.move_right(),
            (KeyModifiers::NONE, KeyCode::Home) => self.input.home(),
            (KeyModifiers::NONE, KeyCode::End) => self.input.end(),
            (KeyModifiers::CONTROL, KeyCode::Char('a')) => self.input.home(),
            (KeyModifiers::CONTROL, KeyCode::Char('e')) => self.input.end(),
            (KeyModifiers::CONTROL, KeyCode::Char('k')) => self.input.kill_to_end(),
            (KeyModifiers::CONTROL, KeyCode::Char('w')) => self.input.delete_word(),
            (KeyModifiers::CONTROL, KeyCode::Char('u')) => {
                self.input.take();
            }

            // History navigation.
            (KeyModifiers::NONE, KeyCode::Up) => self.input.history_up(&self.history),
            (KeyModifiers::NONE, KeyCode::Down) => self.input.history_down(&self.history),

            // Scroll results.
            (KeyModifiers::NONE, KeyCode::PageUp) => {
                self.scroll_offset = self.scroll_offset.saturating_sub(10);
            }
            (KeyModifiers::NONE, KeyCode::PageDown) => {
                self.scroll_offset = self.scroll_offset.saturating_add(10);
            }

            // Character input.
            (KeyModifiers::NONE | KeyModifiers::SHIFT, KeyCode::Char(c)) => {
                self.input.insert(c);
            }

            _ => {}
        }

        UiAction::None
    }

    fn trigger_completion(&mut self) {
        let state = self
            .completor
            .complete(self.input.buffer(), self.input.cursor());
        if state.active {
            self.completion = state;
            self.apply_completion();
        } else {
            // No completions — insert spaces as fallback.
            self.input.insert(' ');
            self.input.insert(' ');
        }
    }

    fn apply_completion(&mut self) {
        if let Some(candidate) = self.completion.selected_candidate() {
            let start = self.completion.prefix_start;
            let end = start + self.completion.prefix_len;
            let replacement = candidate.to_string();

            // Replace the prefix in the buffer with the selected candidate.
            let buf = self.input.take();
            let new_buf = format!("{}{}{}", &buf[..start], replacement, &buf[end..]);
            let new_cursor = start + replacement.len();
            self.input.set(&new_buf);
            // Position cursor after the replacement.
            self.input.home();
            for _ in 0..new_cursor {
                self.input.move_right();
            }
        }
    }

    // ─── Search mode ───────────────────────────────────────────

    fn handle_search_key(&mut self, key: KeyEvent) {
        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL, KeyCode::Char('r')) => {
                // Next match.
                self.search_match_index += 1;
                self.do_search();
            }
            (_, KeyCode::Enter) => {
                // Accept match.
                self.search_mode = false;
            }
            (_, KeyCode::Esc) => {
                // Cancel search.
                self.search_mode = false;
                self.input.set("");
            }
            (_, KeyCode::Backspace) => {
                self.search_query.pop();
                self.search_match_index = 0;
                self.do_search();
            }
            (_, KeyCode::Char(c)) => {
                self.search_query.push(c);
                self.search_match_index = 0;
                self.do_search();
            }
            _ => {}
        }
    }

    fn do_search(&mut self) {
        if let Some((_idx, entry)) = self
            .history
            .search(&self.search_query, self.search_match_index)
        {
            self.input.set(entry);
        }
    }

    // ─── Execution ─────────────────────────────────────────────

    async fn execute_input(&mut self) -> UiAction {
        let input = self.input.take();
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return UiAction::None;
        }

        self.history.add(trimmed);
        self.error_message = None;
        self.scroll_offset = 0;

        // Metacommand?
        if trimmed.starts_with('\\') {
            return self.handle_metacommand(trimmed).await;
        }

        // SQL execution.
        let sql = trimmed.trim_end_matches(';');
        self.last_query = Some(sql.to_string());
        self.execute_sql(sql).await;

        // Refresh completions after DDL.
        let upper = sql.to_uppercase();
        if upper.starts_with("CREATE ") || upper.starts_with("DROP ") {
            self.completor.refresh_collections(&self.client).await;
        }

        UiAction::None
    }

    async fn execute_sql(&mut self, sql: &str) {
        let start = Instant::now();
        match self.client.query(sql).await {
            Ok(result) => {
                self.last_query_time = Some(start.elapsed());
                let output = if self.expanded_mode {
                    format::expanded::format(&result)
                } else {
                    format::format_result(&result, self.output_format)
                };
                self.result_output = Some(output.clone());
                self.error_message = None;

                // Write to output file if set.
                if let Some(ref path) = self.output_file {
                    let _ = std::fs::write(path, &output);
                }
            }
            Err(e) => {
                self.last_query_time = Some(start.elapsed());
                self.error_message = Some(format!("ERROR: {e}"));
                self.result_output = None;
            }
        }
    }

    async fn handle_metacommand(&mut self, input: &str) -> UiAction {
        match metacommand::parse(input) {
            metacommand::MetaAction::Sql(sql) => {
                self.last_query = Some(sql.clone());
                self.execute_sql(&sql).await;
            }
            metacommand::MetaAction::SetFormat(f) => match f.as_str() {
                "table" | "t" => self.output_format = OutputFormat::Table,
                "json" | "j" => self.output_format = OutputFormat::Json,
                "csv" | "c" => self.output_format = OutputFormat::Csv,
                _ => {
                    self.error_message =
                        Some(format!("Unknown format '{f}'. Use: table, json, csv"));
                }
            },
            metacommand::MetaAction::ToggleTiming => {
                self.show_timing = !self.show_timing;
                self.result_output = Some(format!(
                    "Timing is {}.",
                    if self.show_timing { "on" } else { "off" }
                ));
            }
            metacommand::MetaAction::ToggleExpanded => {
                self.expanded_mode = !self.expanded_mode;
                self.result_output = Some(format!(
                    "Expanded display is {}.",
                    if self.expanded_mode { "on" } else { "off" }
                ));
            }
            metacommand::MetaAction::ConnInfo => {
                let auth = if self.client.ping().await.is_ok() {
                    "connected"
                } else {
                    "disconnected"
                };
                self.result_output = Some(format!(
                    "Host: {}\nPort: {}\nStatus: {auth}",
                    self.host, self.port
                ));
                self.error_message = None;
            }
            metacommand::MetaAction::ExternalEditor => {
                return UiAction::OpenEditor;
            }
            metacommand::MetaAction::ExecuteFile(path) => match std::fs::read_to_string(&path) {
                Ok(sql) => {
                    for stmt in sql.split(';') {
                        let t = stmt.trim();
                        if !t.is_empty() {
                            self.execute_sql(t).await;
                        }
                    }
                }
                Err(e) => {
                    self.error_message = Some(format!("Failed to read '{path}': {e}"));
                }
            },
            metacommand::MetaAction::SaveResults(path) => {
                if let Some(ref output) = self.result_output {
                    match std::fs::write(&path, output) {
                        Ok(()) => {
                            self.result_output = Some(format!("Results saved to {path}"));
                        }
                        Err(e) => {
                            self.error_message = Some(format!("Failed to write '{path}': {e}"));
                        }
                    }
                } else {
                    self.error_message = Some("No results to save.".into());
                }
            }
            metacommand::MetaAction::Watch(n) => {
                if let Some(ref query) = self.last_query.clone() {
                    self.watch_interval = Some(Duration::from_secs(n));
                    self.watch_last_exec = Some(Instant::now());
                    self.execute_sql(query).await;
                    self.result_output = Some(format!(
                        "{}\n\n(watching every {n}s, press any key to stop)",
                        self.result_output.as_deref().unwrap_or("")
                    ));
                } else {
                    self.error_message = Some("No previous query to watch.".into());
                }
            }
            metacommand::MetaAction::Help => {
                self.result_output = Some(metacommand::help_text().to_string());
                self.error_message = None;
            }
            metacommand::MetaAction::Quit => {
                self.running = false;
            }
            metacommand::MetaAction::Unknown(cmd) => {
                self.error_message = Some(format!("Unknown command: {cmd}. Type \\? for help."));
            }
        }

        UiAction::None
    }

    // ─── External editor ───────────────────────────────────────

    fn open_external_editor(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> crate::error::CliResult<()> {
        let editor = std::env::var("VISUAL")
            .or_else(|_| std::env::var("EDITOR"))
            .unwrap_or_else(|_| "vi".into());

        // Write current input to temp file.
        let tmp = tempfile::Builder::new().suffix(".sql").tempfile()?;
        std::fs::write(tmp.path(), self.input.buffer())?;

        // Exit TUI mode.
        crossterm::terminal::disable_raw_mode()?;
        crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;

        // Spawn editor.
        let status = std::process::Command::new(&editor).arg(tmp.path()).status();

        // Re-enter TUI mode.
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(terminal.backend_mut(), EnterAlternateScreen)?;
        // Force full redraw.
        terminal.clear()?;

        match status {
            Ok(s) if s.success() => {
                if let Ok(content) = std::fs::read_to_string(tmp.path()) {
                    let trimmed = content.trim_end();
                    if !trimmed.is_empty() {
                        self.input.set(trimmed);
                    }
                }
            }
            Ok(s) => {
                self.error_message = Some(format!("Editor exited with status: {s}"));
            }
            Err(e) => {
                self.error_message = Some(format!("Failed to launch editor '{editor}': {e}"));
            }
        }

        Ok(())
    }
}
