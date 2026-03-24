//! Connection helper: build a NativeClient from CLI args.

use crossterm::event::{self, Event, KeyCode};
use nodedb_client::NativeClient;
use nodedb_client::native::pool::PoolConfig;
use nodedb_types::protocol::AuthMethod;

use crate::args::CliArgs;
use crate::error::CliResult;

/// Connect to the server using CLI arguments.
pub fn build_client(args: &mut CliArgs) -> CliResult<NativeClient> {
    // If -W given without value, prompt for password interactively.
    if args.password.as_deref() == Some("") {
        args.password = Some(prompt_password()?);
    }

    let auth = if let Some(ref pw) = args.password {
        AuthMethod::Password {
            username: args.user.clone(),
            password: pw.clone(),
        }
    } else {
        AuthMethod::Trust {
            username: args.user.clone(),
        }
    };

    let config = PoolConfig {
        addr: args.addr(),
        max_size: 2,
        auth,
        ..Default::default()
    };

    Ok(NativeClient::new(config))
}

/// Prompt for password without echoing to the terminal.
fn prompt_password() -> CliResult<String> {
    eprint!("Password: ");

    // We're not in raw mode yet (called before TUI starts), so enter it briefly.
    crossterm::terminal::enable_raw_mode()?;
    let mut pw = String::new();

    loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => break,
                KeyCode::Char(c) => pw.push(c),
                KeyCode::Backspace => {
                    pw.pop();
                }
                _ => {}
            }
        }
    }

    crossterm::terminal::disable_raw_mode()?;
    eprintln!(); // newline after password
    Ok(pw)
}
