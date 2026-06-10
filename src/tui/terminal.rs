#![allow(unexpected_cfgs)]

use crate::tui::{TuiError, TuiResult};

/// RAII guard for raw mode and alternate-screen terminal state.
///
/// The enabled implementation owns the Ratatui terminal and restores the
/// original terminal state in `Drop`, including panic and early-return paths.
#[cfg(feature = "tui")]
pub struct TerminalSession {
    terminal: ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
}

#[cfg(feature = "tui")]
impl TerminalSession {
    /// Enter raw mode and switch to the alternate screen.
    pub fn enter() -> TuiResult<Self> {
        use crossterm::{
            execute,
            terminal::{EnterAlternateScreen, enable_raw_mode},
        };

        enable_raw_mode().map_err(|error| TuiError::Terminal(error.to_string()))?;

        let mut stdout = std::io::stdout();
        if let Err(error) = execute!(stdout, EnterAlternateScreen) {
            let _ = crossterm::terminal::disable_raw_mode();
            return Err(TuiError::Terminal(error.to_string()));
        }

        let backend = ratatui::backend::CrosstermBackend::new(stdout);
        let terminal = ratatui::Terminal::new(backend)
            .map_err(|error| TuiError::Terminal(error.to_string()))?;

        Ok(Self { terminal })
    }

    /// Access the underlying Ratatui terminal for drawing.
    pub fn terminal(
        &mut self,
    ) -> &mut ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>> {
        &mut self.terminal
    }
}

#[cfg(feature = "tui")]
impl Drop for TerminalSession {
    fn drop(&mut self) {
        use crossterm::{
            execute,
            terminal::{LeaveAlternateScreen, disable_raw_mode},
        };

        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

/// Stub guard used when the `tui` feature is absent.
#[cfg(not(feature = "tui"))]
#[derive(Debug, Default)]
pub struct TerminalSession;

#[cfg(not(feature = "tui"))]
impl TerminalSession {
    /// Return a clear error instead of touching terminal state.
    pub fn enter() -> TuiResult<Self> {
        Err(TuiError::FeatureDisabled("tui"))
    }
}
