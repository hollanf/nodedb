//! Ctrl+R reverse history search.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::App;

impl App {
    pub(super) fn handle_search_key(&mut self, key: KeyEvent) {
        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL, KeyCode::Char('r')) => {
                self.search_match_index += 1;
                self.do_search();
            }
            (_, KeyCode::Enter) => {
                self.search_mode = false;
            }
            (_, KeyCode::Esc) => {
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
}
