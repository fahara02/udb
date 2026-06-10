#![allow(unexpected_cfgs)]

use std::path::PathBuf;
use std::time::Duration;

#[cfg(feature = "tui")]
use crate::tui::TerminalSession;
use crate::tui::{TuiError, TuiResult};

/// High-level action requested by the UI chrome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitAction {
    Back,
    Next,
    Preview,
    Apply,
    Cancel,
}

/// Abstract input event consumed by the init state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitEvent {
    Start,
    Next,
    Back,
    Preview,
    Apply,
    Cancel,
    Tick,
    Resize { columns: u16, rows: u16 },
    Input(InitInput),
    External(String),
}

/// Key-like input that is independent of Crossterm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitInput {
    Up,
    Down,
    Left,
    Right,
    Enter,
    Tab,
    BackTab,
    Esc,
    CtrlC,
    Char(char),
}

/// State marker for the step list rendered by the TUI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitStepStatus {
    Pending,
    Active,
    Complete,
    Warning,
    Error,
}

/// One row in the init wizard step list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitStep {
    pub id: String,
    pub label: String,
    pub status: InitStepStatus,
    pub validation_count: usize,
}

/// Presentation snapshot produced by the future init HSM/planner layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitSnapshot {
    pub title: String,
    pub cwd: Option<PathBuf>,
    pub mode: String,
    pub warnings: Vec<String>,
    pub steps: Vec<InitStep>,
    pub facts: Vec<String>,
    pub selections: Vec<String>,
    pub plan_summary: Vec<String>,
    pub apply_log: Vec<String>,
    pub action_bar: Vec<InitAction>,
    pub status_line: String,
}

impl Default for InitSnapshot {
    fn default() -> Self {
        Self {
            title: "UDB init".to_string(),
            cwd: None,
            mode: "interactive".to_string(),
            warnings: Vec::new(),
            steps: Vec::new(),
            facts: Vec::new(),
            selections: Vec::new(),
            plan_summary: Vec::new(),
            apply_log: Vec::new(),
            action_bar: vec![
                InitAction::Back,
                InitAction::Next,
                InitAction::Preview,
                InitAction::Apply,
                InitAction::Cancel,
            ],
            status_line: "Waiting for init state".to_string(),
        }
    }
}

/// Driver owned by the init core. The TUI only renders snapshots and sends
/// abstract events through this trait.
pub trait InitTuiDriver {
    type Output;

    fn snapshot(&self) -> InitSnapshot;
    fn handle_event(&mut self, event: InitEvent) -> TuiResult<()>;
    fn should_quit(&self) -> bool;
    fn finish(self) -> TuiResult<Self::Output>;
}

/// Event loop shell for the future full-screen init wizard.
#[derive(Debug)]
#[cfg_attr(not(feature = "tui"), allow(dead_code))]
pub struct InitTuiApp<D> {
    driver: D,
    tick_rate: Duration,
}

impl<D> InitTuiApp<D>
where
    D: InitTuiDriver,
{
    pub fn new(driver: D) -> Self {
        Self {
            driver,
            tick_rate: Duration::from_millis(250),
        }
    }

    pub fn with_tick_rate(mut self, tick_rate: Duration) -> Self {
        self.tick_rate = tick_rate;
        self
    }

    /// Run the feature-enabled Ratatui event loop.
    #[cfg(feature = "tui")]
    pub fn run(mut self) -> TuiResult<D::Output> {
        let mut session = TerminalSession::enter()?;
        self.driver.handle_event(InitEvent::Start)?;

        while !self.driver.should_quit() {
            let snapshot = self.driver.snapshot();
            session
                .terminal()
                .draw(|frame| render_snapshot(frame, &snapshot))
                .map_err(|error| TuiError::Render(error.to_string()))?;

            if crossterm::event::poll(self.tick_rate)
                .map_err(|error| TuiError::Event(error.to_string()))?
            {
                let event =
                    crossterm::event::read().map_err(|error| TuiError::Event(error.to_string()))?;
                if let Some(event) = map_crossterm_event(event) {
                    if matches!(
                        event,
                        InitEvent::Cancel | InitEvent::Input(InitInput::Esc | InitInput::CtrlC)
                    ) {
                        self.driver.handle_event(InitEvent::Cancel)?;
                    } else {
                        self.driver.handle_event(event)?;
                    }
                }
            } else {
                self.driver.handle_event(InitEvent::Tick)?;
            }
        }

        self.driver.finish()
    }

    /// Return a clear error when the full-screen TUI is not compiled in.
    #[cfg(not(feature = "tui"))]
    pub fn run(self) -> TuiResult<D::Output> {
        let _ = self;
        Err(TuiError::FeatureDisabled("tui"))
    }
}

#[cfg(feature = "tui")]
fn map_crossterm_event(event: crossterm::event::Event) -> Option<InitEvent> {
    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};

    match event {
        Event::Key(KeyEvent {
            code: KeyCode::Char('c'),
            modifiers,
            ..
        }) if modifiers.contains(KeyModifiers::CONTROL) => Some(InitEvent::Cancel),
        Event::Key(KeyEvent {
            code: KeyCode::Esc, ..
        }) => Some(InitEvent::Cancel),
        Event::Key(KeyEvent {
            code: KeyCode::Enter,
            ..
        }) => Some(InitEvent::Next),
        Event::Key(KeyEvent {
            code: KeyCode::BackTab,
            ..
        }) => Some(InitEvent::Input(InitInput::BackTab)),
        Event::Key(KeyEvent {
            code: KeyCode::Tab, ..
        }) => Some(InitEvent::Input(InitInput::Tab)),
        Event::Key(KeyEvent {
            code: KeyCode::Up, ..
        }) => Some(InitEvent::Input(InitInput::Up)),
        Event::Key(KeyEvent {
            code: KeyCode::Down,
            ..
        }) => Some(InitEvent::Input(InitInput::Down)),
        Event::Key(KeyEvent {
            code: KeyCode::Left,
            ..
        }) => Some(InitEvent::Back),
        Event::Key(KeyEvent {
            code: KeyCode::Right,
            ..
        }) => Some(InitEvent::Next),
        Event::Key(KeyEvent {
            code: KeyCode::Char('p'),
            ..
        }) => Some(InitEvent::Preview),
        Event::Key(KeyEvent {
            code: KeyCode::Char('a'),
            ..
        }) => Some(InitEvent::Apply),
        Event::Key(KeyEvent {
            code: KeyCode::Char(ch),
            ..
        }) => Some(InitEvent::Input(InitInput::Char(ch))),
        Event::Resize(columns, rows) => Some(InitEvent::Resize { columns, rows }),
        _ => None,
    }
}

#[cfg(feature = "tui")]
fn render_snapshot(frame: &mut ratatui::Frame<'_>, snapshot: &InitSnapshot) {
    use ratatui::{
        layout::{Constraint, Direction, Layout},
        style::{Modifier, Style},
        text::{Line, Span},
        widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
    };

    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let cwd = snapshot
        .cwd
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "workspace pending".to_string());
    let warning_text = match snapshot.warnings.len() {
        0 => "no warnings".to_string(),
        1 => "1 warning".to_string(),
        count => format!("{count} warnings"),
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            snapshot.title.clone(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(" | "),
        Span::raw(cwd),
        Span::raw(" | "),
        Span::raw(snapshot.mode.clone()),
        Span::raw(" | "),
        Span::raw(warning_text),
    ]))
    .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, root[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(24),
            Constraint::Percentage(46),
            Constraint::Percentage(30),
        ])
        .split(root[1]);

    let steps = snapshot.steps.iter().map(|step| {
        let marker = match step.status {
            InitStepStatus::Pending => " ",
            InitStepStatus::Active => ">",
            InitStepStatus::Complete => "x",
            InitStepStatus::Warning => "!",
            InitStepStatus::Error => "!",
        };
        let suffix = if step.validation_count == 0 {
            String::new()
        } else {
            format!(" ({})", step.validation_count)
        };
        ListItem::new(format!("{marker} {}{suffix}", step.label))
    });
    frame.render_widget(
        List::new(steps).block(Block::default().title("Steps").borders(Borders::ALL)),
        body[0],
    );

    let center_lines = snapshot
        .selections
        .iter()
        .chain(snapshot.plan_summary.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    frame.render_widget(
        Paragraph::new(center_lines)
            .block(Block::default().title("Current").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        body[1],
    );

    let right_lines = snapshot
        .facts
        .iter()
        .chain(snapshot.warnings.iter())
        .chain(snapshot.apply_log.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    frame.render_widget(
        Paragraph::new(right_lines)
            .block(Block::default().title("Workspace").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        body[2],
    );

    let actions = snapshot
        .action_bar
        .iter()
        .map(|action| match action {
            InitAction::Back => "Back",
            InitAction::Next => "Next",
            InitAction::Preview => "Preview",
            InitAction::Apply => "Apply",
            InitAction::Cancel => "Cancel",
        })
        .collect::<Vec<_>>()
        .join(" | ");
    let footer = Paragraph::new(format!("{actions}\n{}", snapshot.status_line))
        .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, root[2]);
}
