mod args;
mod batch;
mod completion;
mod config;
mod connect;
mod error;
mod format;
mod highlight;
mod history;
mod keywords;
mod metacommand;
mod tui;

use std::io;

use clap::Parser;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use args::CliArgs;
use config::CliConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load config file and resolve args (CLI > env > config > defaults).
    let config = CliConfig::load();
    let mut args = CliArgs::parse().resolve(&config);
    let client = connect::build_client(&mut args)?;

    // Non-interactive: -e flag.
    if let Some(ref sql) = args.execute {
        batch::run(&client, sql, args.format, args.output.as_deref()).await?;
        return Ok(());
    }

    // Non-interactive: -f flag (file input).
    if let Some(ref path) = args.file {
        let sql = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
        batch::run(&client, &sql, args.format, args.output.as_deref()).await?;
        return Ok(());
    }

    // Non-interactive: piped stdin.
    if !atty_is_stdin() {
        let sql = io::read_to_string(io::stdin())?;
        batch::run(&client, &sql, args.format, args.output.as_deref()).await?;
        return Ok(());
    }

    // Interactive TUI.
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let timing = config.timing.unwrap_or(true);
    let mut app = tui::app::App::new(
        client,
        args.format,
        args.host.clone(),
        args.port,
        timing,
        args.output.clone(),
    );
    let result = app.run(&mut terminal).await;

    // Restore terminal.
    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;

    result?;
    Ok(())
}

/// Check if stdin is a terminal (not piped).
fn atty_is_stdin() -> bool {
    std::io::IsTerminal::is_terminal(&io::stdin())
}
