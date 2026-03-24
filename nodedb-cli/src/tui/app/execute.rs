//! SQL execution and metacommand dispatch.

use std::time::{Duration, Instant};

use crate::args::OutputFormat;
use crate::format;
use crate::metacommand;

use super::{App, UiAction};

impl App {
    pub(super) async fn execute_input(&mut self) -> UiAction {
        let input = self.input.take();
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return UiAction::None;
        }

        self.history.add(trimmed);
        self.error_message = None;
        self.scroll_offset = 0;

        if trimmed.starts_with('\\') {
            return self.handle_metacommand(trimmed).await;
        }

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

    pub(super) async fn execute_sql(&mut self, sql: &str) {
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
}
