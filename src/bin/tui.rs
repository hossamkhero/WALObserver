use chrono::{DateTime, Utc};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use pg_wal_visualizer::charts_data::{
    ChartsData, SeriesPoint, SlotRetentionChart, StandbyReplayChart, WalActivityChart, WalAmplificationChart, build_charts,
};
use pg_wal_visualizer::events::{DbRole, EventSnapshot};
use pg_wal_visualizer::readers::main_log::{MainLogRecord, MaterializedTickState, ReadResult, apply_tick_snapshot, read_all_default};
use pg_wal_visualizer::tick::StoredSettingRow;

use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols::{self, border},
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
};
use sqlx::postgres::PgPoolOptions;
use std::{collections::BTreeMap, env, time::{Duration as StdDuration, Instant}};
use tokio::runtime::Builder;
use tokio::time::{Duration, timeout};

const DEFAULT_VIEWPORT_CAP_MS: f64 = 5.0 * 60_000.0;

struct OverviewData {
    role: String,
    connection: String,
    connected: bool,
    status: String,
    latest_tick: String,
    wal_dir: String,
    wal_position: String,
    settings: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChartSlot {
    WalActivity,
    SlotRetention,
    WalAmplification,
    StandbyReplay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FocusMoveDirection {
    Next,
    Previous,
    Up,
    Down,
}

struct ChartsState {
    // On first render, snap every strip to the newest visible window.
    start_at_latest: bool,

    focused: ChartSlot,

    wal_activity: TwoStripState,
    slot_retention: TwoStripState,
    wal_amplification: ThreeStripState,
    standby_replay: TwoStripState,
}

struct TwoStripState {
    focused_strip: usize,
    x_offsets: [f64; 2],
    max_offsets: [f64; 2],
}

struct ThreeStripState {
    focused_strip: usize,
    x_offsets: [f64; 3],
    max_offsets: [f64; 3],
}

struct App {
    overview: OverviewData,
    charts: ChartsData,
    charts_state: ChartsState,
    last_poll_at: Instant,
}

impl App {
    fn new(mut overview: OverviewData, charts: ChartsData) -> Self {
        overview.connected = false;
        overview.connection = "connecting...".to_string();

        Self {
            overview,
            charts,
            charts_state: ChartsState {
                start_at_latest: true,
                focused: ChartSlot::WalActivity,
                wal_activity: TwoStripState { focused_strip: 0, x_offsets: [0.0; 2], max_offsets: [0.0; 2] },
                slot_retention: TwoStripState { focused_strip: 0, x_offsets: [0.0; 2], max_offsets: [0.0; 2] },
                wal_amplification: ThreeStripState { focused_strip: 0, x_offsets: [0.0; 3], max_offsets: [0.0; 3] },
                standby_replay: TwoStripState { focused_strip: 0, x_offsets: [0.0; 2], max_offsets: [0.0; 2] },
            },
            last_poll_at: Instant::now(),
        }
    }
}

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let ReadResult { records, progress: _progress } = read_all_default()?;

    let mut app = build_app(&records);

    let mut terminal = ratatui::init();
    let result = run(&mut terminal, &mut app);
    ratatui::restore();

    result?;
    Ok(())
}

fn build_app(records: &[MainLogRecord]) -> App {
    let mut latest_tick = MaterializedTickState::default();
    let mut latest_tick_ts = None;
    let mut latest_settings_ts = None;
    let mut settings = BTreeMap::<String, StoredSettingRow>::new();
    let mut connected = false;

    for record in records {
        match record {
            MainLogRecord::Tick { header, payload } => {
                apply_tick_snapshot(&mut latest_tick, payload);
                latest_tick_ts = Some(header.timestamp_ms);
                connected = true;
            }
            MainLogRecord::Settings { header, payload } => {
                latest_settings_ts = Some(header.timestamp_ms);

                for row in &payload.settings {
                    settings.insert(row.name.clone(), row.clone());
                }
            }
            MainLogRecord::Event { header: _header, payload } => match payload {
                EventSnapshot::Disconnected => connected = false,
                EventSnapshot::Reconnected => connected = true,
                EventSnapshot::RoleChanged { .. } => {}
            },
        }
    }

    let overview = build_overview(connected, latest_tick_ts, latest_settings_ts, &latest_tick, &settings);

    let charts = build_charts(records);

    App::new(overview, charts)
}

fn build_overview(
    connected: bool,
    latest_tick_ts: Option<u64>,
    latest_settings_ts: Option<u64>,
    tick_state: &MaterializedTickState,
    settings: &BTreeMap<String, StoredSettingRow>,
) -> OverviewData {
    let role = match tick_state.wal_functions.as_ref().map(current_role) {
        Some(DbRole::Primary) => "primary".to_string(),
        Some(DbRole::Standby) => "standby".to_string(),
        None => "-".to_string(),
    };

    let connection = if connected { "connected" } else { "disconnected" }.to_string();

    let status = current_status(tick_state);

    let latest_tick = latest_tick_ts
        .and_then(|ts| DateTime::<Utc>::from_timestamp_millis(ts as i64).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()))
        .unwrap_or_else(|| "-".to_string());

    let settings = latest_settings_ts.map(|ts| format!("{ts} ({} tracked)", settings.len())).unwrap_or_else(|| "-".to_string());

    let wal_dir = tick_state.wal_dir.as_ref().map(|wal_dir| format!("{} files", wal_dir.n_files)).unwrap_or_else(|| "-".to_string());

    let wal_position = tick_state
        .wal_functions
        .as_ref()
        .map(|wal| {
            wal.current_wal_lsn
                .clone()
                .or_else(|| wal.last_wal_replay_lsn.clone())
                .or_else(|| wal.last_wal_receive_lsn.clone())
                .unwrap_or_else(|| "-".to_string())
        })
        .unwrap_or_else(|| "-".to_string());

    OverviewData { role, connection, connected, status, latest_tick, wal_dir, wal_position, settings }
}

fn current_role(wal_functions: &pg_wal_visualizer::tick::StoredWalFunctionsRow) -> DbRole {
    if wal_functions.is_in_recovery { DbRole::Standby } else { DbRole::Primary }
}

fn current_status(tick_state: &MaterializedTickState) -> String {
    match tick_state.wal_functions.as_ref().map(current_role) {
        Some(DbRole::Standby) => match &tick_state.pg_stat_wal_receiver {
            Some(Some(receiver)) => receiver.status.clone(),
            Some(None) => "inactive".to_string(),
            None => "-".to_string(),
        },
        Some(DbRole::Primary) => match &tick_state.pg_stat_replication {
            Some(rows) if rows.is_empty() => "no replicas".to_string(),
            Some(rows) if rows.len() == 1 => rows[0].state.clone(),
            Some(rows) => {
                let first_state = &rows[0].state;

                if rows.iter().all(|row| row.state == *first_state) {
                    format!("{first_state} x{}", rows.len())
                } else {
                    format!("{} replicas", rows.len())
                }
            }
            None => "-".to_string(),
        },
        None => "-".to_string(),
    }
}

fn run(terminal: &mut DefaultTerminal, app: &mut App) -> std::io::Result<()> {
    loop {
        poll_connection_status(app);
        poll_chart_data(app);
        terminal.draw(|frame| render(frame, app))?;

        if !event::poll(StdDuration::from_millis(200))? {
            continue;
        }

        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }

            match key.code {
                KeyCode::Char('q') => break Ok(()),
                KeyCode::Tab => move_chart_slot_focus(app, FocusMoveDirection::Next),
                KeyCode::BackTab => move_chart_slot_focus(app, FocusMoveDirection::Previous),
                KeyCode::Char('j') => move_chart_strip_focus(app, FocusMoveDirection::Down),
                KeyCode::Char('k') => move_chart_strip_focus(app, FocusMoveDirection::Up),
                KeyCode::Char('h') => scroll_focused_chart(app, -0.25),
                KeyCode::Char('l') => scroll_focused_chart(app, 0.25),
                _ => {}
            }
        }
    }
}

// Periodically reload stored snapshots and rebuild the chart-facing data.
fn poll_chart_data(app: &mut App) {
    if app.last_poll_at.elapsed() < StdDuration::from_millis(1000) {
        return;
    }

    let ReadResult { records, progress: _progress } = match read_all_default() {
        Ok(read_result) => read_result,
        Err(_) => {
            app.last_poll_at = Instant::now();
            return;
        }
    };

    let connection = app.overview.connection.clone();
    let connected = app.overview.connected;
    let mut new_app = build_app(&records);

    new_app.overview.connection = connection;
    new_app.overview.connected = connected;
    app.overview = new_app.overview;
    app.charts = new_app.charts;
    app.last_poll_at = Instant::now();
}

// Poll the actual database connection and update only the live connection box.
fn poll_connection_status(app: &mut App) {
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://postgres@127.0.0.1:5433/pg_wal_visualizer".to_string());

    let connected = Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()
        .and_then(|runtime| {
            runtime.block_on(async {
                timeout(Duration::from_millis(150), PgPoolOptions::new().max_connections(1).connect(&database_url))
                    .await
                    .ok()
                    .and_then(Result::ok)
            })
        })
        .is_some();

    app.overview.connected = connected;
    app.overview.connection = if connected { "connected".to_string() } else { "disconnected".to_string() };
}

// Move focus between the outer chart panels.
// The standby panel is only part of this routing when it is actually rendered.
fn move_chart_slot_focus(app: &mut App, direction: FocusMoveDirection) {
    let slots = active_chart_slots(app);
    let current_idx = slots.iter().position(|slot| *slot == app.charts_state.focused).unwrap_or(0);

    app.charts_state.focused = match direction {
        FocusMoveDirection::Next => slots[(current_idx + 1) % slots.len()],
        FocusMoveDirection::Previous => slots[(current_idx + slots.len() - 1) % slots.len()],
        FocusMoveDirection::Down | FocusMoveDirection::Up => app.charts_state.focused,
    };
}

// Return the chart slots that are actually present in the current UI.
// On primary this omits `StandbyReplay`; on standby it includes it.
fn active_chart_slots(app: &App) -> &'static [ChartSlot] {
    const PRIMARY_SLOTS: &[ChartSlot] = &[ChartSlot::WalActivity, ChartSlot::SlotRetention, ChartSlot::WalAmplification];
    const STANDBY_SLOTS: &[ChartSlot] =
        &[ChartSlot::WalActivity, ChartSlot::SlotRetention, ChartSlot::WalAmplification, ChartSlot::StandbyReplay];

    if app.charts.standby_replay.is_some() { STANDBY_SLOTS } else { PRIMARY_SLOTS }
}

// Move focus between the inner strips of the currently focused panel.
// Up/down only affect the strip index inside that one panel.
fn move_chart_strip_focus(app: &mut App, direction: FocusMoveDirection) {
    let strip_count = focused_chart_strip_count(app);
    let focused_strip = focused_chart_strip_mut(&mut app.charts_state);

    *focused_strip = match direction {
        FocusMoveDirection::Down => (*focused_strip + 1).min(strip_count.saturating_sub(1)),
        FocusMoveDirection::Up => focused_strip.saturating_sub(1),
        FocusMoveDirection::Next | FocusMoveDirection::Previous => *focused_strip,
    };
}

// Scroll the focused chart horizontally by a fraction of its current viewport width.
// Negative delta moves left, positive delta moves right.
fn scroll_focused_chart(app: &mut App, delta: f64) {
    let step = focused_chart_viewport_width(app) * delta;
    let (min_offset, max_offset) = focused_chart_scroll_bounds(app);
    let offset = focused_chart_offset_mut(&mut app.charts_state);
    *offset = (*offset + step).clamp(min_offset, max_offset);
}

// Return a mutable reference to the x offset field for the currently focused chart.
fn focused_chart_offset_mut(charts_state: &mut ChartsState) -> &mut f64 {
    match (charts_state.focused, focused_strip(charts_state)) {
        (ChartSlot::WalActivity, idx) => &mut charts_state.wal_activity.x_offsets[idx],
        (ChartSlot::SlotRetention, idx) => &mut charts_state.slot_retention.x_offsets[idx],
        (ChartSlot::WalAmplification, idx) => &mut charts_state.wal_amplification.x_offsets[idx],
        (ChartSlot::StandbyReplay, idx) => &mut charts_state.standby_replay.x_offsets[idx],
    }
}

// Return a mutable reference to the strip index for the currently focused panel.
fn focused_chart_strip_mut(charts_state: &mut ChartsState) -> &mut usize {
    match charts_state.focused {
        ChartSlot::WalActivity => &mut charts_state.wal_activity.focused_strip,
        ChartSlot::SlotRetention => &mut charts_state.slot_retention.focused_strip,
        ChartSlot::WalAmplification => &mut charts_state.wal_amplification.focused_strip,
        ChartSlot::StandbyReplay => &mut charts_state.standby_replay.focused_strip,
    }
}

// Return how many focusable strips the current panel has.
fn focused_chart_strip_count(app: &App) -> usize {
    match app.charts_state.focused {
        ChartSlot::WalActivity => 2,
        ChartSlot::SlotRetention => 2,
        ChartSlot::WalAmplification => 3,
        ChartSlot::StandbyReplay => 2,
    }
}

fn focused_strip(charts_state: &ChartsState) -> usize {
    match charts_state.focused {
        ChartSlot::WalActivity => charts_state.wal_activity.focused_strip,
        ChartSlot::SlotRetention => charts_state.slot_retention.focused_strip,
        ChartSlot::WalAmplification => charts_state.wal_amplification.focused_strip,
        ChartSlot::StandbyReplay => charts_state.standby_replay.focused_strip,
    }
}

// Return the legal horizontal scroll range for the focused chart as:
//
// - `.0` => minimum allowed x offset
// - `.1` => maximum allowed x offset
//
// These bounds are computed from the full content width and the current viewport width.
fn focused_chart_scroll_bounds(app: &App) -> (f64, f64) {
    let max_offset = match (app.charts_state.focused, focused_strip(&app.charts_state)) {
        (ChartSlot::WalActivity, idx) => app.charts_state.wal_activity.max_offsets[idx],
        (ChartSlot::SlotRetention, idx) => app.charts_state.slot_retention.max_offsets[idx],
        (ChartSlot::WalAmplification, idx) => app.charts_state.wal_amplification.max_offsets[idx],
        (ChartSlot::StandbyReplay, idx) => app.charts_state.standby_replay.max_offsets[idx],
    };

    (0.0, max_offset)
}

// Return the current viewport width for the focused chart.
// This is the visible x-span in the same x-space as the chart data.
fn focused_chart_viewport_width(app: &App) -> f64 {
    match (app.charts_state.focused, focused_strip(&app.charts_state)) {
        (ChartSlot::WalActivity, 0) => chart_viewport_width_from_offsets(
            app.charts_state.wal_activity.max_offsets[0],
            &app.charts.wal_activity.wal_bytes_per_sec,
        ),
        (ChartSlot::WalActivity, _) => chart_viewport_width_from_offsets(
            app.charts_state.wal_activity.max_offsets[1],
            &app.charts.wal_activity.wal_records_per_sec,
        ),
        (ChartSlot::SlotRetention, 0) => chart_viewport_width_from_offsets(
            app.charts_state.slot_retention.max_offsets[0],
            &app.charts.slot_retention.worst_slot_lag_bytes,
        ),
        (ChartSlot::SlotRetention, _) => chart_viewport_width_from_offsets(
            app.charts_state.slot_retention.max_offsets[1],
            &app.charts.slot_retention.wal_dir_file_count,
        ),
        (ChartSlot::WalAmplification, 0) => chart_viewport_width_from_offsets(
            app.charts_state.wal_amplification.max_offsets[0],
            &app.charts.wal_amplification.wal_bytes_per_record,
        ),
        (ChartSlot::WalAmplification, 1) => chart_viewport_width_from_offsets(
            app.charts_state.wal_amplification.max_offsets[1],
            &app.charts.wal_amplification.updates_per_sec,
        ),
        (ChartSlot::WalAmplification, _) => chart_viewport_width_from_offsets(
            app.charts_state.wal_amplification.max_offsets[2],
            &app.charts.wal_amplification.hot_update_ratio,
        ),
        (ChartSlot::StandbyReplay, 0) => match &app.charts.standby_replay {
            Some(chart) => chart_viewport_width_from_offsets(
                app.charts_state.standby_replay.max_offsets[0],
                &chart.receive_lsn_progress,
            ),
            None => 1.0,
        },
        (ChartSlot::StandbyReplay, _) => match &app.charts.standby_replay {
            Some(chart) => chart_viewport_width_from_offsets(
                app.charts_state.standby_replay.max_offsets[1],
                &chart.replay_lsn_progress,
            ),
            None => 1.0,
        },
    }
}

fn chart_viewport_width_from_offsets(max_offset: f64, series: &[SeriesPoint]) -> f64 {
    let content_width = series_content_width(&[("series", series, Color::Reset)]).unwrap_or(1.0);

    (content_width - max_offset).max(1.0)
}

fn render(frame: &mut Frame, app: &mut App) {
    // Our layout.
    // basically a vertical layout with two sections. first is 3 (idk but whatever vertical unit
    // they using), and it's basically the header.
    // the second one just fills the rest of the space, and it's whatever we hav that has the "main
    // frame". cuz apparently it's not a frame that stuff goes into, it's stuff that has the frame
    // around it.
    let chunks = Layout::vertical([Constraint::Length(3), Constraint::Min(1)]).split(frame.area());

    //////// ==================================== //////////////////////
    // Boxes up there.
    //////// ==================================== //////////////////////
    let OverviewData { role, connection, connected, status, latest_tick, wal_dir, wal_position, settings: _settings } = &app.overview;

    let overview_chunks = Layout::horizontal([
        Constraint::Min(16),
        Constraint::Min(16),
        Constraint::Min(16),
        Constraint::Min(16),
        Constraint::Min(16),
        Constraint::Min(16),
    ])
    .split(chunks[0]);

    render_inline_box(frame, overview_chunks[0], "Role", Line::from(role.clone()).fg(Color::Cyan));
    render_inline_box(
        frame,
        overview_chunks[1],
        "Connection",
        Line::from(vec![
            Span::raw(connection.clone()),
            Span::styled(
                " ●",
                Style::default().fg(if connection == "connecting..." {
                    Color::Yellow
                } else if *connected {
                    Color::Green
                } else {
                    Color::Red
                }),
            ),
        ]),
    );
    render_inline_box(frame, overview_chunks[2], "Status", Line::from(status.clone()).fg(Color::LightGreen));
    render_inline_box(frame, overview_chunks[3], "Latest Tick", Line::from(latest_tick.clone()).fg(Color::Yellow));
    render_inline_box(frame, overview_chunks[4], "WAL Dir", Line::from(wal_dir.clone()).fg(Color::Magenta));
    render_inline_box(frame, overview_chunks[5], "WAL Pos", Line::from(wal_position.clone()).fg(Color::Blue));

    //////// ==================================== //////////////////////
    // Main frame.
    //////// ==================================== //////////////////////

    // title (centered, top edge of main frame)
    let title = Line::from(" WAL Visualizer ".bold());

    // insutctions (centered, bottom edge of main frame)
    let instructions = Line::from(vec![
        " Panels ".into(),
        "<Tab>".blue().bold(),
        " Strips ".into(),
        "<J/K>".blue().bold(),
        " Scroll ".into(),
        "<H/L>".blue().bold(),
        " Quit ".into(),
        "<Q>".blue().bold(),
    ]);

    // The the frame around the 2x2 grid
    let main_block =
        Block::default().borders(Borders::ALL).title(title.centered()).title_bottom(instructions.centered()).border_set(border::THICK);

    let inner = main_block.inner(chunks[1]);
    frame.render_widget(main_block, chunks[1]);

    // The 2x2 grid.
    let chart_rows = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)]).split(inner);
    let top_row = Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).split(chart_rows[0]);

    let focused = app.charts_state.focused;
    let start_at_latest = app.charts_state.start_at_latest;

    render_wal_activity_chart(
        frame,
        top_row[0],
        &app.charts.wal_activity,
        &mut app.charts_state.wal_activity,
        focused == ChartSlot::WalActivity,
        start_at_latest,
    );
    render_slot_retention_chart(
        frame,
        top_row[1],
        &app.charts.slot_retention,
        &mut app.charts_state.slot_retention,
        focused == ChartSlot::SlotRetention,
        start_at_latest,
    );

    // if we are in standby mode, we have the 2 charts on bottom row, if not, we amke the
    // `wal_amplification` chart take the whole bottom row.
    if let Some(standby_replay) = &app.charts.standby_replay {
        let bottom_row = Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).split(chart_rows[1]);

        render_wal_amplification_chart(
            frame,
            bottom_row[0],
            &app.charts.wal_amplification,
            &mut app.charts_state.wal_amplification,
            focused == ChartSlot::WalAmplification,
            start_at_latest,
        );
        render_standby_replay_chart(
            frame,
            bottom_row[1],
            standby_replay,
            &mut app.charts_state.standby_replay,
            focused == ChartSlot::StandbyReplay,
            start_at_latest,
        );
    } else {
        render_wal_amplification_chart(
            frame,
            chart_rows[1],
            &app.charts.wal_amplification,
            &mut app.charts_state.wal_amplification,
            focused == ChartSlot::WalAmplification,
            start_at_latest,
        );
    }

    app.charts_state.start_at_latest = false;
}

fn series_max_offset(area: Rect, series: &[(&str, &[SeriesPoint], Color)]) -> f64 {
    let content_width = series_content_width(series).unwrap_or(1.0);
    let viewport_width = chart_viewport_width(area, series);
    (content_width - viewport_width).max(0.0)
}

fn render_inline_box(frame: &mut Frame, area: Rect, title: &str, value: Line<'static>) {
    let mut spans = vec![Span::styled(format!("{title}: "), Style::default().fg(Color::DarkGray).bold())];
    spans.extend(value.spans);

    let widget = Paragraph::new(Line::from(spans)).block(Block::default().borders(Borders::ALL));
    frame.render_widget(widget, area);
}

fn render_wal_activity_chart(
    frame: &mut Frame,
    area: Rect,
    chart: &WalActivityChart,
    state: &mut TwoStripState,
    focused: bool,
    start_at_latest: bool,
) {
    // These two metrics have different units, so keep one outer panel but split its inner
    // space into two borderless strips.
    let TwoStripState { focused_strip, x_offsets, max_offsets } = state;

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if focused { Color::Yellow } else { Color::Reset }))
        .title("WAL Activity");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let vertical = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)]).split(inner);
    sync_strip_offset(
        &mut x_offsets[0],
        &mut max_offsets[0],
        series_max_offset(vertical[0], &[("wal_bytes/sec", &chart.wal_bytes_per_sec, Color::Yellow)]),
        start_at_latest,
    );
    sync_strip_offset(
        &mut x_offsets[1],
        &mut max_offsets[1],
        series_max_offset(vertical[1], &[("wal_records/sec", &chart.wal_records_per_sec, Color::Cyan)]),
        start_at_latest,
    );

    render_series_chart(
        frame,
        vertical[0],
        Some("WAL Bytes / Sec"),
        Some(Color::Yellow),
        &[("wal_bytes/sec", &chart.wal_bytes_per_sec, Color::Yellow)],
        x_offsets[0],
        focused && *focused_strip == 0,
    );

    render_series_chart(
        frame,
        vertical[1],
        Some("WAL Records / Sec"),
        Some(Color::Cyan),
        &[("wal_records/sec", &chart.wal_records_per_sec, Color::Cyan)],
        x_offsets[1],
        focused && *focused_strip == 1,
    );
}

fn render_slot_retention_chart(
    frame: &mut Frame,
    area: Rect,
    chart: &SlotRetentionChart,
    state: &mut TwoStripState,
    focused: bool,
    start_at_latest: bool,
) {
    // Retention mixes lag bytes and WAL dir file count, so render them as separate strips.
    let TwoStripState { focused_strip, x_offsets, max_offsets } = state;

    let title = match (&chart.latest_worst_slot_name, chart.latest_worst_slot_active) {
        (Some(name), Some(true)) => format!("Pinned WAL [{name} active]"),
        (Some(name), Some(false)) => format!("Pinned WAL [{name} inactive]"),
        (Some(name), None) => format!("Pinned WAL [{name}]"),
        _ => "Pinned WAL".to_string(),
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if focused { Color::Yellow } else { Color::Reset }))
        .title("Slot Retention");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let vertical = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)]).split(inner);
    sync_strip_offset(
        &mut x_offsets[0],
        &mut max_offsets[0],
        series_max_offset(vertical[0], &[("worst_slot_lag_bytes", &chart.worst_slot_lag_bytes, Color::Magenta)]),
        start_at_latest,
    );
    sync_strip_offset(
        &mut x_offsets[1],
        &mut max_offsets[1],
        series_max_offset(vertical[1], &[("wal_dir.n_files", &chart.wal_dir_file_count, Color::Blue)]),
        start_at_latest,
    );

    render_series_chart(
        frame,
        vertical[0],
        Some(title.as_str()),
        Some(Color::Magenta),
        &[("worst_slot_lag_bytes", &chart.worst_slot_lag_bytes, Color::Magenta)],
        x_offsets[0],
        focused && *focused_strip == 0,
    );

    render_series_chart(
        frame,
        vertical[1],
        Some("WAL Dir Files"),
        Some(Color::Blue),
        &[("wal_dir.n_files", &chart.wal_dir_file_count, Color::Blue)],
        x_offsets[1],
        focused && *focused_strip == 1,
    );
}

fn render_wal_amplification_chart(
    frame: &mut Frame,
    area: Rect,
    chart: &WalAmplificationChart,
    state: &mut ThreeStripState,
    focused: bool,
    start_at_latest: bool,
) {
    // These three metrics have different units, so keep one outer panel but split its inner
    // space into three borderless strips instead of overlaying them on a shared axis.
    let ThreeStripState { focused_strip, x_offsets, max_offsets } = state;

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if focused { Color::Yellow } else { Color::Reset }))
        .title("WAL Amplification / HOT");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let vertical = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1), Constraint::Fill(1)]).split(inner);
    sync_strip_offset(
        &mut x_offsets[0],
        &mut max_offsets[0],
        series_max_offset(vertical[0], &[("wal_bytes/record", &chart.wal_bytes_per_record, Color::Yellow)]),
        start_at_latest,
    );
    sync_strip_offset(
        &mut x_offsets[1],
        &mut max_offsets[1],
        series_max_offset(vertical[1], &[("updates/sec", &chart.updates_per_sec, Color::Green)]),
        start_at_latest,
    );
    sync_strip_offset(
        &mut x_offsets[2],
        &mut max_offsets[2],
        series_max_offset(vertical[2], &[("hot_update_ratio", &chart.hot_update_ratio, Color::Cyan)]),
        start_at_latest,
    );

    render_series_chart(
        frame,
        vertical[0],
        Some("WAL Bytes / Record"),
        Some(Color::Yellow),
        &[("wal_bytes/record", &chart.wal_bytes_per_record, Color::Yellow)],
        x_offsets[0],
        focused && *focused_strip == 0,
    );

    render_series_chart(
        frame,
        vertical[1],
        Some("Updates / Sec"),
        Some(Color::Green),
        &[("updates/sec", &chart.updates_per_sec, Color::Green)],
        x_offsets[1],
        focused && *focused_strip == 1,
    );

    render_series_chart(
        frame,
        vertical[2],
        Some("HOT Update Ratio"),
        Some(Color::Cyan),
        &[("hot_update_ratio", &chart.hot_update_ratio, Color::Cyan)],
        x_offsets[2],
        focused && *focused_strip == 2,
    );
}

fn render_standby_replay_chart(
    frame: &mut Frame,
    area: Rect,
    chart: &StandbyReplayChart,
    state: &mut TwoStripState,
    focused: bool,
    start_at_latest: bool,
) {
    // Receive and replay progression get their own strips for the same reason as the other panels.
    let TwoStripState { focused_strip, x_offsets, max_offsets } = state;

    let title = format!("Standby Replay / Receiver [{}]", chart.latest_receiver_status);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if focused { Color::Yellow } else { Color::Reset }))
        .title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let vertical = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)]).split(inner);
    sync_strip_offset(
        &mut x_offsets[0],
        &mut max_offsets[0],
        series_max_offset(vertical[0], &[("receive_lsn_progress", &chart.receive_lsn_progress, Color::LightBlue)]),
        start_at_latest,
    );
    sync_strip_offset(
        &mut x_offsets[1],
        &mut max_offsets[1],
        series_max_offset(vertical[1], &[("replay_lsn_progress", &chart.replay_lsn_progress, Color::LightGreen)]),
        start_at_latest,
    );

    render_series_chart(
        frame,
        vertical[0],
        Some("Receive Progress"),
        Some(Color::LightBlue),
        &[("receive_lsn_progress", &chart.receive_lsn_progress, Color::LightBlue)],
        x_offsets[0],
        focused && *focused_strip == 0,
    );

    render_series_chart(
        frame,
        vertical[1],
        Some("Replay Progress"),
        Some(Color::LightGreen),
        &[("replay_lsn_progress", &chart.replay_lsn_progress, Color::LightGreen)],
        x_offsets[1],
        focused && *focused_strip == 1,
    );
}

fn render_series_chart(
    frame: &mut Frame,
    area: Rect,
    label: Option<&str>,
    label_color: Option<Color>,
    series: &[(&str, &[SeriesPoint], Color)],
    x_offset: f64,
    focused: bool,
) {
    // Callers own the outer border/title. This helper only lays out optional text rows
    // above the plot and renders the chart content itself.
    let chart_area = match label {
        Some(label) => {
            let chunks = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(area);
            frame.render_widget(Paragraph::new(label).style(Style::default().fg(label_color.unwrap_or(Color::DarkGray))), chunks[0]);
            chunks[1]
        }
        None => area,
    };

    // Plot against absolute tick timestamps on x and metric values on y.
    let plotted: Vec<Vec<(f64, f64)>> =
        series.iter().map(|(_, points, _)| points.iter().map(|point| (point.ts_ms as f64, point.value)).collect()).collect();

    let datasets: Vec<Dataset> = series
        .iter()
        .zip(plotted.iter())
        .map(|((_, _, color), data)| {
            Dataset::default().graph_type(GraphType::Line).marker(symbols::Marker::Braille).style(Style::default().fg(*color)).data(data)
        })
        .collect();

    // Bounds come from the actual samples across all provided series so irregular collection
    // spacing still shows up correctly in the chart.
    let x_min =
        series.iter().flat_map(|(_, points, _)| points.iter().map(|point| point.ts_ms as f64)).min_by(|a, b| a.partial_cmp(b).unwrap());

    let x_max =
        series.iter().flat_map(|(_, points, _)| points.iter().map(|point| point.ts_ms as f64)).max_by(|a, b| a.partial_cmp(b).unwrap());

    let y_min = series.iter().flat_map(|(_, points, _)| points.iter().map(|point| point.value)).min_by(|a, b| a.partial_cmp(b).unwrap());

    let y_max = series.iter().flat_map(|(_, points, _)| points.iter().map(|point| point.value)).max_by(|a, b| a.partial_cmp(b).unwrap());

    // Then fallback handling
    let x_min = x_min.unwrap_or(0.0);
    let mut x_max = x_max.unwrap_or(1.0);
    let y_min = y_min.unwrap_or(0.0);
    let mut y_max = y_max.unwrap_or(1.0);

    if (x_max - x_min).abs() < f64::EPSILON {
        x_max += 1.0;
    }

    if (y_max - y_min).abs() < f64::EPSILON {
        y_max += 1.0;
    }

    let viewport_width = chart_viewport_width(chart_area, series);
    let (x_min, x_max) = viewport_x_bounds(x_min, x_max, x_offset, viewport_width);
    let x_mid = x_min + ((x_max - x_min) / 2.0);
    let y_mid = y_min + ((y_max - y_min) / 2.0);

    let chart = Chart::new(datasets)
        .x_axis(
            Axis::default()
                .style(Style::default().fg(if focused { Color::White } else { Color::DarkGray }))
                .bounds([x_min, x_max])
                .labels(vec![Line::from(format_ts_label(x_min)), Line::from(format_ts_label(x_mid)), Line::from(format_ts_label(x_max))]),
        )
        .y_axis(
            Axis::default().style(Style::default().fg(if focused { Color::White } else { Color::DarkGray })).bounds([y_min, y_max]).labels(
                vec![
                    Line::from(format_metric_label(y_min)),
                    Line::from(format_metric_label(y_mid)),
                    Line::from(format_metric_label(y_max)),
                ],
            ),
        );

    frame.render_widget(chart, chart_area);
}

fn sync_strip_offset(x_offset: &mut f64, max_offset: &mut f64, new_max_offset: f64, start_at_latest: bool) {
    let previous_max_offset = *max_offset;
    *max_offset = new_max_offset;

    if start_at_latest {
        *x_offset = new_max_offset;
        return;
    }

    let right_gap = (previous_max_offset - *x_offset).max(0.0);
    *x_offset = (new_max_offset - right_gap).clamp(0.0, new_max_offset);
}

fn format_ts_label(ts_ms: f64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ts_ms as i64).map(|dt| dt.format("%H:%M:%S").to_string()).unwrap_or_else(|| "-".to_string())
}

fn format_metric_label(value: f64) -> String {
    let abs = value.abs();

    if abs >= 1_000_000_000.0 {
        format!("{:.1}B", value / 1_000_000_000.0)
    } else if abs >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if abs >= 1_000.0 {
        format!("{:.1}K", value / 1_000.0)
    } else if abs >= 100.0 {
        format!("{value:.0}")
    } else if abs >= 10.0 {
        format!("{value:.1}")
    } else if abs >= 1.0 {
        format!("{value:.2}")
    } else {
        format!("{value:.3}")
    }
}

// Compute the full x-range of the available data.
// This is based on real timestamps, before any viewporting is applied.
fn series_content_x_bounds(series: &[(&str, &[SeriesPoint], Color)]) -> Option<(f64, f64)> {
    let x_min =
        series.iter().flat_map(|(_, points, _)| points.iter().map(|point| point.ts_ms as f64)).min_by(|a, b| a.partial_cmp(b).unwrap())?;

    let x_max =
        series.iter().flat_map(|(_, points, _)| points.iter().map(|point| point.ts_ms as f64)).max_by(|a, b| a.partial_cmp(b).unwrap())?;

    Some((x_min, x_max))
}

// Convenience wrapper around the bounds helper.
// Returns the width of the full data span.
fn series_content_width(series: &[(&str, &[SeriesPoint], Color)]) -> Option<f64> {
    let (x_min, x_max) = series_content_x_bounds(series)?;
    Some((x_max - x_min).max(0.0))
}

// Convert the available chart width on screen into a visible x-span.
// The return value is the width of the viewport in chart x-space.
fn chart_viewport_width(area: Rect, series: &[(&str, &[SeriesPoint], Color)]) -> f64 {
    let visible_columns = area.width.saturating_sub(2).max(1) as f64;
    let avg_step = average_x_step(series).unwrap_or(1.0);
    let content_width = series_content_width(series).unwrap_or(avg_step);
    let viewport_width = (visible_columns * avg_step).min(content_width).max(avg_step);

    viewport_width.min(DEFAULT_VIEWPORT_CAP_MS).max(avg_step)
}

fn average_x_step(series: &[(&str, &[SeriesPoint], Color)]) -> Option<f64> {
    for (_, points, _) in series {
        if points.len() < 2 {
            continue;
        }

        let mut total = 0.0;
        let mut count = 0usize;

        for pair in points.windows(2) {
            let prev = pair[0].ts_ms as f64;
            let curr = pair[1].ts_ms as f64;
            let delta = curr - prev;

            if delta > 0.0 {
                total += delta;
                count += 1;
            }
        }

        if count > 0 {
            return Some(total / count as f64);
        }
    }

    None
}

// Convert the full content range plus a horizontal offset into the visible x window.
//
// Returns:
//
// - `.0` => viewport x minimum
// - `.1` => viewport x maximum
fn viewport_x_bounds(content_x_min: f64, content_x_max: f64, x_offset: f64, viewport_width: f64) -> (f64, f64) {
    let content_width = (content_x_max - content_x_min).max(0.0);
    let max_offset = (content_width - viewport_width).max(0.0);
    let clamped_offset = x_offset.clamp(0.0, max_offset);

    let viewport_x_min = content_x_min + clamped_offset;
    let mut viewport_x_max = (viewport_x_min + viewport_width).min(content_x_max);

    if (viewport_x_max - viewport_x_min).abs() < f64::EPSILON {
        viewport_x_max = viewport_x_min + 1.0;
    }

    (viewport_x_min, viewport_x_max)
}
