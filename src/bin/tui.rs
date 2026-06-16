use chrono::{DateTime, Utc};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use pg_wal_visualizer::events::{DbRole, EventSnapshot};
use pg_wal_visualizer::readers::main_log::{
    MainLogRecord, MaterializedTickState, ReadResult, apply_tick_snapshot, read_all_default,
};
use pg_wal_visualizer::tick::StoredSettingRow;
use pg_wal_visualizer::charts_data::{
    ChartsData, SeriesPoint, SlotRetentionChart, StandbyReplayChart, WalActivityChart,
    WalAmplificationChart, build_charts,
};

use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols::{self, border},
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, ListState, Paragraph},
};
use std::collections::BTreeMap;

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

struct App {
    rows: Vec<String>,
    list_state: ListState,
    overview: OverviewData,
    charts: ChartsData,
}

impl App {
    fn new(rows: Vec<String>, overview: OverviewData, charts: ChartsData) -> Self {
        let mut list_state = ListState::default();
        if !rows.is_empty() {
            list_state.select(Some(0));
        }

        Self {
            rows,
            list_state,
            overview,
            charts,
        }
    }

    fn select_next(&mut self) {
        if self.rows.is_empty() {
            return;
        }

        let current = self.list_state.selected().unwrap_or(0);
        let next = (current + 1).min(self.rows.len() - 1);
        self.list_state.select(Some(next));
    }

    fn select_previous(&mut self) {
        if self.rows.is_empty() {
            return;
        }

        let current = self.list_state.selected().unwrap_or(0);
        self.list_state.select(Some(current.saturating_sub(1)));
    }
}

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let ReadResult {
        records,
        progress: _progress,
    } = read_all_default()?;

    let mut app = build_app(&records);

    let mut terminal = ratatui::init();
    let result = run(&mut terminal, &mut app);
    ratatui::restore();

    result?;
    Ok(())
}

fn build_app(records: &[MainLogRecord]) -> App {
    let mut rows = Vec::with_capacity(records.len());
    let mut latest_tick = MaterializedTickState::default();
    let mut latest_tick_ts = None;
    let mut latest_settings_ts = None;
    let mut settings = BTreeMap::<String, StoredSettingRow>::new();
    let mut connected = false;

    for record in records {
        match record {
            MainLogRecord::Tick { header, payload } => {
                rows.push(format!(
                    "{}  T  mask={:014b}",
                    header.timestamp_ms, payload.changed_mask
                ));

                apply_tick_snapshot(&mut latest_tick, payload);
                latest_tick_ts = Some(header.timestamp_ms);
                connected = true;
            }
            MainLogRecord::Settings { header, payload } => {
                rows.push(format!(
                    "{}  S  mask={:06b}",
                    header.timestamp_ms, payload.changed_mask
                ));

                latest_settings_ts = Some(header.timestamp_ms);

                for row in &payload.settings {
                    settings.insert(row.name.clone(), row.clone());
                }
            }
            MainLogRecord::Event { header, payload } => {
                rows.push(format!("{}  E  {:?}", header.timestamp_ms, payload));

                match payload {
                    EventSnapshot::Disconnected => connected = false,
                    EventSnapshot::Reconnected => connected = true,
                    EventSnapshot::RoleChanged { .. } => {}
                }
            }
        }
    }

    let overview = build_overview(
        connected,
        latest_tick_ts,
        latest_settings_ts,
        &latest_tick,
        &settings,
    );

    let charts = build_charts(records);

    App::new(rows, overview, charts)
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

    let connection = if connected {
        "connected"
    } else {
        "disconnected"
    }
    .to_string();

    let status = current_status(tick_state);

    let latest_tick = latest_tick_ts
        .map(|ts| ts.to_string())
        .unwrap_or_else(|| "-".to_string());

    let settings = latest_settings_ts
        .map(|ts| format!("{ts} ({} tracked)", settings.len()))
        .unwrap_or_else(|| "-".to_string());

    let wal_dir = tick_state
        .wal_dir
        .as_ref()
        .map(|wal_dir| format!("{} files", wal_dir.n_files))
        .unwrap_or_else(|| "-".to_string());

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

    OverviewData {
        role,
        connection,
        connected,
        status,
        latest_tick,
        wal_dir,
        wal_position,
        settings,
    }
}

fn current_role(wal_functions: &pg_wal_visualizer::tick::StoredWalFunctionsRow) -> DbRole {
    if wal_functions.is_in_recovery {
        DbRole::Standby
    } else {
        DbRole::Primary
    }
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
        terminal.draw(|frame| render(frame, app))?;

        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }

            match key.code {
                KeyCode::Char('q') => break Ok(()),
                KeyCode::Down | KeyCode::Char('j') => app.select_next(),
                KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
                _ => {}
            }
        }
    }
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
    let OverviewData {
        role,
        connection,
        connected,
        status,
        latest_tick,
        wal_dir,
        wal_position,
        settings: _settings,
    } = &app.overview;

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
    render_inline_box(frame, overview_chunks[1], "Connection",
        Line::from(vec![
            Span::raw(connection.clone()),
            Span::styled(
                " ●",
                Style::default().fg(if *connected { Color::Green } else { Color::Red }),
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
        " Move ".into(),
        "<Up/Down>".blue().bold(),
        " Quit ".into(),
        "<Q>".blue().bold(),
        " Settings ".into(),
        app.overview.settings.clone().yellow().bold(),
    ]);

    // The the frame around the 2x2 grid
    let main_block = Block::default()
        .borders(Borders::ALL)
        .title(title.centered())
        .title_bottom(instructions.centered())
        .border_set(border::THICK);

    let inner = main_block.inner(chunks[1]);
    frame.render_widget(main_block, chunks[1]);

    // The 2x2 grid.
    let chart_rows = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)]).split(inner);
    let top_row = Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).split(chart_rows[0]);

    // All charts renderers
    render_wal_activity_chart(frame, top_row[0], &app.charts.wal_activity);
    render_slot_retention_chart(frame, top_row[1], &app.charts.slot_retention);


    // if we are in standby mode, we have the 2 charts on bottom row, if not, we amke the
    // `wal_amplification` chart take the whole bottom row.
    if let Some(standby_replay) = &app.charts.standby_replay {
        let bottom_row = Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).split(chart_rows[1]);

        render_wal_amplification_chart(frame, bottom_row[0], &app.charts.wal_amplification);
        render_standby_replay_chart(frame, bottom_row[1], standby_replay);
    } else {
        render_wal_amplification_chart(frame, chart_rows[1], &app.charts.wal_amplification);
    }
}

fn render_inline_box(frame: &mut Frame, area: Rect, title: &str, value: Line<'static>) {
    let mut spans = vec![Span::styled(
        format!("{title}: "),
        Style::default().fg(Color::DarkGray).bold(),
    )];
    spans.extend(value.spans);

    let widget = Paragraph::new(Line::from(spans)).block(Block::default().borders(Borders::ALL));
    frame.render_widget(widget, area);
}

fn render_wal_activity_chart(frame: &mut Frame, area: Rect, chart: &WalActivityChart) {
    // These two metrics have different units, so keep one outer panel but split its inner
    // space into two borderless strips.
    let block = Block::default().borders(Borders::ALL).title("WAL Activity");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let vertical = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)]).split(inner);

    render_series_chart(frame, vertical[0], Some("WAL Bytes / Sec"), Some(Color::Yellow), &[
        ("wal_bytes/sec", &chart.wal_bytes_per_sec, Color::Yellow),
    ]);

    render_series_chart(frame, vertical[1], Some("WAL Records / Sec"), Some(Color::Cyan), &[
        ("wal_records/sec", &chart.wal_records_per_sec, Color::Cyan),
    ]);
}

fn render_slot_retention_chart(frame: &mut Frame, area: Rect, chart: &SlotRetentionChart) {
    // Retention mixes lag bytes and WAL dir file count, so render them as separate strips.
    let title = match &chart.latest_worst_slot_name {
        Some(name) => format!("Replication Slot Retention [{name}]"),
        None => "Replication Slot Retention".to_string(),
    };

    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let vertical = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)]).split(inner);

    render_series_chart(frame, vertical[0], Some("Worst Slot Lag"), Some(Color::Magenta), &[
        ("worst_slot_lag_bytes", &chart.worst_slot_lag_bytes, Color::Magenta),
    ]);

    render_series_chart(frame, vertical[1], Some("WAL Dir Files"), Some(Color::Blue), &[
        ("wal_dir.n_files", &chart.wal_dir_file_count, Color::Blue),
    ]);
}

fn render_wal_amplification_chart(frame: &mut Frame, area: Rect, chart: &WalAmplificationChart) {
    // These three metrics have different units, so keep one outer panel but split its inner
    // space into three borderless strips instead of overlaying them on a shared axis.
    let block = Block::default().borders(Borders::ALL).title("WAL Amplification / HOT");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let vertical = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1), Constraint::Fill(1)]).split(inner);

    render_series_chart(frame, vertical[0], Some("WAL Bytes / Record"), Some(Color::Yellow), &[
        ("wal_bytes/record", &chart.wal_bytes_per_record, Color::Yellow),
    ]);

    render_series_chart(frame, vertical[1], Some("Updates / Sec"), Some(Color::Green), &[
        ("updates/sec", &chart.updates_per_sec, Color::Green),
    ]);

    render_series_chart(frame, vertical[2], Some("HOT Update Ratio"), Some(Color::Cyan), &[
        ("hot_update_ratio", &chart.hot_update_ratio, Color::Cyan),
    ]);
}

fn render_standby_replay_chart(frame: &mut Frame, area: Rect, chart: &StandbyReplayChart) {
    // Receive and replay progression get their own strips for the same reason as the other panels.
    let title = format!("Standby Replay / Receiver [{}]", chart.latest_receiver_status);
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let vertical = Layout::vertical([Constraint::Fill(1), Constraint::Fill(1)]).split(inner);

    render_series_chart(frame, vertical[0], Some("Receive Progress"), Some(Color::LightBlue), &[
        ("receive_lsn_progress", &chart.receive_lsn_progress, Color::LightBlue),
    ]);

    render_series_chart(frame, vertical[1], Some("Replay Progress"), Some(Color::LightGreen), &[
        ("replay_lsn_progress", &chart.replay_lsn_progress, Color::LightGreen),
    ]);
}

fn render_series_chart(
    frame: &mut Frame,
    area: Rect,
    label: Option<&str>,
    label_color: Option<Color>,
    series: &[(&str, &[SeriesPoint], Color)],
) {
    // Callers own the outer border/title. This helper only lays out optional text rows
    // above the plot and renders the chart content itself.
    let chart_area = match label {
        Some(label) => {
            let chunks = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(area);
            frame.render_widget(
                Paragraph::new(label).style(Style::default().fg(label_color.unwrap_or(Color::DarkGray))),
                chunks[0],
            );
            chunks[1]
        }
        None => area,
    };

    // Plot against absolute tick timestamps on x and metric values on y.
    let plotted: Vec<Vec<(f64, f64)>> = series
        .iter()
        .map(|(_, points, _)| {
            points
                .iter()
                .map(|point| (point.ts_ms as f64, point.value))
                .collect()
        })
        .collect();

    let datasets: Vec<Dataset> = series
        .iter()
        .zip(plotted.iter())
        .map(|((_, _, color), data)| {
            Dataset::default()
                .graph_type(GraphType::Line)
                .marker(symbols::Marker::Braille)
                .style(Style::default().fg(*color))
                .data(data)
        })
        .collect();

    // Bounds come from the actual samples across all provided series so irregular collection
    // spacing still shows up correctly in the chart.
    let x_min = series
        .iter()
        .flat_map(|(_, points, _)| points.iter().map(|point| point.ts_ms as f64))
        .min_by(|a, b| a.partial_cmp(b).unwrap());

    let x_max = series
        .iter()
        .flat_map(|(_, points, _)| points.iter().map(|point| point.ts_ms as f64))
        .max_by(|a, b| a.partial_cmp(b).unwrap());

    let y_min = series
        .iter()
        .flat_map(|(_, points, _)| points.iter().map(|point| point.value))
        .min_by(|a, b| a.partial_cmp(b).unwrap());

    let y_max = series
        .iter()
        .flat_map(|(_, points, _)| points.iter().map(|point| point.value))
        .max_by(|a, b| a.partial_cmp(b).unwrap());

    
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

    let x_mid = x_min + ((x_max - x_min) / 2.0);
    let y_mid = y_min + ((y_max - y_min) / 2.0);

    let chart = Chart::new(datasets)
        .x_axis(
            Axis::default()
                .bounds([x_min, x_max])
                .labels(vec![
                    Line::from(format_ts_label(x_min)),
                    Line::from(format_ts_label(x_mid)),
                    Line::from(format_ts_label(x_max)),
                ]),
        )
        .y_axis(
            Axis::default()
                .bounds([y_min, y_max])
                .labels(vec![
                    Line::from(format_metric_label(y_min)),
                    Line::from(format_metric_label(y_mid)),
                    Line::from(format_metric_label(y_max)),
                ]),
        );

    frame.render_widget(chart, chart_area);
}

fn format_ts_label(ts_ms: f64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ts_ms as i64)
        .map(|dt| dt.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "-".to_string())
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
