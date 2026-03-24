//! External editor support (\e command).

use std::io::Stdout;

use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use super::App;

impl App {
    pub(super) fn open_external_editor(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> crate::error::CliResult<()> {
        let editor = std::env::var("VISUAL")
            .or_else(|_| std::env::var("EDITOR"))
            .unwrap_or_else(|_| "vi".into());

        let tmp = tempfile::Builder::new().suffix(".sql").tempfile()?;
        std::fs::write(tmp.path(), self.input.buffer())?;

        // Exit TUI mode.
        crossterm::terminal::disable_raw_mode()?;
        crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;

        let status = std::process::Command::new(&editor).arg(tmp.path()).status();

        // Re-enter TUI mode.
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(terminal.backend_mut(), EnterAlternateScreen)?;
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
