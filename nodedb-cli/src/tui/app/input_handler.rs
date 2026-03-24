//! Keyboard input handling and tab completion.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::completion::CompletionState;

use super::{App, UiAction};

impl App {
    pub(super) async fn handle_key(&mut self, key: KeyEvent) -> UiAction {
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
                    return UiAction::None;
                }
                _ => {
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

    pub(super) fn trigger_completion(&mut self) {
        let state = self
            .completor
            .complete(self.input.buffer(), self.input.cursor());
        if state.active {
            self.completion = state;
            self.apply_completion();
        } else {
            self.input.insert(' ');
            self.input.insert(' ');
        }
    }

    pub(super) fn apply_completion(&mut self) {
        if let Some(candidate) = self.completion.selected_candidate() {
            let start = self.completion.prefix_start;
            let end = start + self.completion.prefix_len;
            let replacement = candidate.to_string();

            let buf = self.input.take();
            let new_buf = format!("{}{}{}", &buf[..start], replacement, &buf[end..]);
            let new_cursor = start + replacement.len();
            self.input.set(&new_buf);
            self.input.home();
            for _ in 0..new_cursor {
                self.input.move_right();
            }
        }
    }
}
