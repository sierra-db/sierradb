use std::{io, time::Duration};

use base64::prelude::*;
use chrono::SecondsFormat;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::{
    layout::Size,
    prelude::*,
    symbols::border,
    widgets::{block::*, *},
};
use tokio::sync::watch::Receiver;
use tui_scrollview::{ScrollView, ScrollViewState};

use crate::tui;

#[derive(Debug)]
pub struct App {
    state: AppState<'static>,
    scroll_view_state: ScrollViewState,
    events_rx: Receiver<Vec<eventus::Event<'static>>>,
    focused: Option<serde_hjson::Value>,
    stream_filter: Option<String>,
    local_tz: bool,
    exit: bool,
}

impl App {
    pub fn new(events_rx: Receiver<Vec<eventus::Event<'static>>>) -> Self {
        App {
            state: AppState {
                table: TableState::default(),
                rows: vec![],
                widths: [
                    Constraint::Length(1),
                    Constraint::Length(1),
                    Constraint::Length(1),
                    Constraint::Length(1),
                    Constraint::Length(1),
                    Constraint::Max(26), // 2024-06-19T21:52:09+10:00
                ],
            },
            scroll_view_state: ScrollViewState::new(),
            events_rx,
            focused: None,
            stream_filter: None,
            local_tz: true,
            exit: false,
        }
    }

    /// runs the application's main loop until the user quits
    pub fn run(&mut self, terminal: &mut tui::Tui) -> anyhow::Result<()> {
        while !self.exit {
            if self.events_rx.has_changed()? {
                self.update_rows();
            }
            terminal.draw(|frame| self.render_frame(frame))?;
            self.handle_events(Duration::from_millis(100))?;
        }
        Ok(())
    }

    fn render_frame(&mut self, frame: &mut Frame) {
        frame.render_stateful_widget(AppWidget, frame.size(), &mut self.state);
        if let Some(value) = &self.focused {
            let value = value.to_string();

            let area = frame.size();
            let area = Rect {
                x: 8,
                y: 4,
                width: area.width.saturating_sub(16),
                height: area.height.saturating_sub(8),
            };
            frame.render_stateful_widget(
                EventDataWidget { value },
                area,
                &mut self.scroll_view_state,
            );
            // let area = frame.size();
            // let area = Rect {
            //     x: 8,
            //     y: 4,
            //     width: area.width.saturating_sub(16),
            //     height: area.height.saturating_sub(8),
            // };
            // let lines = value.split('\n').count() as u16 + 2;
            // // dbg!(lines);
            // let mut scroll_view = ScrollView::new(Size::new(area.width, lines));
            // scroll_view.render_widget(
            //     EventDataWidget { value },
            //     Rect {
            //         x: 0,
            //         y: 0,
            //         width: area.width - 1,
            //         height: lines,
            //     },
            // );
            // frame.render_stateful_widget(scroll_view, area, &mut self.scroll_view_state);
        }
    }

    /// updates the application's state based on user input
    fn handle_events(&mut self, timeout: Duration) -> io::Result<()> {
        if event::poll(timeout)? {
            match event::read()? {
                // it's important to check that the event is a key press event as
                // crossterm also emits key release and repeat events on Windows.
                Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                    self.handle_key_event(key_event)
                }
                _ => {}
            }
            self.handle_events(Duration::ZERO)?;
        }
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Up | KeyCode::Char('k') | KeyCode::Char('K') => {
                if self.focused.is_none() || key_event.modifiers.contains(KeyModifiers::SHIFT) {
                    self.prev_row();
                } else {
                    self.scroll_view_state.scroll_up();
                }
            }
            KeyCode::Down | KeyCode::Char('j') | KeyCode::Char('J') => {
                if self.focused.is_none() || key_event.modifiers.contains(KeyModifiers::SHIFT) {
                    self.next_row();
                } else {
                    self.scroll_view_state.scroll_down();
                }
            }
            KeyCode::Char('f') => self.focus_row(),
            KeyCode::Char('s') => self.focus_stream(),
            KeyCode::Char('t') => self.toggle_local_timezone(),
            KeyCode::Char('b') => self.last_row(),
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }

    fn update_rows(&mut self) {
        let mut new_events = self.events_rx.borrow_and_update().clone();
        if let Some(stream_filter) = &self.stream_filter {
            new_events = new_events
                .into_iter()
                .filter(|event| &event.stream_id == stream_filter)
                .collect();
        }
        self.state.widths[0] = Constraint::Length(
            new_events
                .iter()
                .map(|event| digits(event.id))
                .max()
                .unwrap_or(0)
                + 1,
        );
        self.state.widths[1] = Constraint::Max(
            new_events
                .iter()
                .map(|event| event.stream_id.len())
                .max()
                .unwrap_or(0) as u16
                + 1,
        );
        self.state.widths[2] = Constraint::Length(
            new_events
                .iter()
                .map(|event| digits(event.stream_version))
                .max()
                .unwrap_or(0),
        );
        self.state.widths[4] = Constraint::Max(
            new_events
                .iter()
                .map(|event| event.event_name.len())
                .max()
                .unwrap_or(0) as u16
                + 1,
        );
        self.state.rows = new_events
            .into_iter()
            .map(|event| {
                let timestamp = if self.local_tz {
                    event
                        .timestamp
                        .with_timezone(&chrono::Local)
                        .to_rfc3339_opts(SecondsFormat::Secs, true)
                } else {
                    event
                        .timestamp
                        .with_timezone(&chrono::Utc)
                        .to_rfc3339_opts(SecondsFormat::Secs, true)
                };
                Row::new([
                    Cell::new(event.id.to_string()).style(Style::new().cyan().bold()),
                    Cell::new(event.stream_id.into_owned()).style(Style::new().gray()),
                    Cell::new(event.stream_version.to_string()).style(Style::new().yellow()),
                    Cell::new(">").style(Style::new().gray().dim()),
                    Cell::new(event.event_name.into_owned()).style(Style::new().yellow()),
                    Cell::new(timestamp).style(Style::new().gray().dim()),
                ])
            })
            .collect();
    }

    fn next_row(&mut self) {
        let selected = self.state.table.selected_mut();
        match selected {
            Some(current) => {
                *current = (*current + 1).min(self.state.rows.len().saturating_sub(1));
            }
            None => *selected = Some(0),
        }

        if self.focused.is_some() {
            self.update_focused_value();
        }
    }

    fn prev_row(&mut self) {
        let selected = self.state.table.selected_mut();
        match selected {
            Some(current) => {
                *current = current
                    .saturating_sub(1)
                    .min(self.state.rows.len().saturating_sub(1));
            }
            None => {}
        }

        if self.focused.is_some() {
            self.update_focused_value();
        }
    }

    fn last_row(&mut self) {
        let selected = self.state.table.selected_mut();
        *selected = self.state.rows.len().checked_sub(1);

        if self.focused.is_some() {
            self.update_focused_value();
        }
    }

    fn focus_row(&mut self) {
        match &mut self.focused {
            Some(_) => self.focused = None,
            None => {
                self.scroll_view_state.scroll_to_top();
                self.update_focused_value();
            }
        }
    }

    fn focus_stream(&mut self) {
        match &mut self.stream_filter {
            Some(_) => {
                self.stream_filter = None;
            }
            None => {
                if let Some(idx) = self.state.table.selected() {
                    if let Some(event) = self.events_rx.borrow().get(idx) {
                        self.stream_filter = Some(event.stream_id.to_string());
                    }
                }
            }
        }
        self.update_rows();
    }

    fn update_focused_value(&mut self) {
        if let Some(idx) = self.state.table.selected() {
            if let Some(event) = self.events_rx.borrow().get(idx) {
                let data: ciborium::Value =
                    ciborium::from_reader(event.event_data.as_ref()).unwrap();
                let hdata = ciborium_value_to_hjson_value(data);
                // let data: rmpv::Value = rmp_serde::from_slice(&event.event_data).unwrap();
                // let hdata = rmp_value_to_hjson_value(data);
                self.focused = Some(hdata);
            }
        }
    }

    fn toggle_local_timezone(&mut self) {
        self.local_tz = !self.local_tz;
        self.update_rows();
    }
}

struct AppWidget;

#[derive(Debug, Default)]
struct AppState<'a> {
    table: TableState,
    rows: Vec<Row<'a>>,
    widths: [Constraint; 6],
}

impl StatefulWidget for AppWidget {
    type State = AppState<'static>;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let title = Title::from(" Eventus Monitor ".bold());
        let instructions = Title::from(Line::from(vec![
            " Decrement ".into(),
            "<Left>".blue().bold(),
            " Increment ".into(),
            "<Right>".blue().bold(),
            " Quit ".into(),
            "<Q> ".blue().bold(),
        ]));
        let block = Block::default()
            .title(title.alignment(Alignment::Center))
            .title(
                instructions
                    .alignment(Alignment::Center)
                    .position(title::Position::Bottom),
            )
            .borders(Borders::ALL)
            .border_set(border::THICK);

        // let widths = [
        //     // 999,999,999
        //     Constraint::Length(state.id_width + 1),
        //     Constraint::Percentage(100),
        //     Constraint::Length(6),
        //     Constraint::Min(16),
        //     // Constraint::Fill(6),
        //     Constraint::Min(10),
        // ];
        let table = Table::new(state.rows.clone(), state.widths)
            // ...and they can be separated by a fixed spacing.
            .column_spacing(1)
            // You can set the style of the entire Table.
            .style(Style::new().blue())
            // It has an optional header, which is simply a Row always visible at the top.
            // .header(
            //     Row::new(vec![
            //         "ID",
            //         "Stream ID",
            //         "Ver",
            //         "Event",
            //         //"Data",
            //         "Timestamp",
            //     ])
            //     .style(Style::new().bold())
            //     // To add space between the header and the rest of the rows, specify the margin
            //     .bottom_margin(1),
            // )
            // It has an optional footer, which is simply a Row always visible at the bottom.
            // .footer(Row::new(vec!["Updated on Dec 28"]))
            // As any other widget, a Table can be wrapped in a Block.
            .block(block)
            // The selected row and its content can also be styled.
            .highlight_style(Style::new().bg(Color::Rgb(21, 36, 56)))
            // ...and potentially show a symbol in front of the selection.
            .highlight_symbol(">> ");

        StatefulWidget::render(table, area, buf, &mut state.table)
    }
}

struct EventDataWidget {
    value: String,
}

impl StatefulWidget for EventDataWidget {
    type State = ScrollViewState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State)
    where
        Self: Sized,
    {
        let lines = self.value.trim().split('\n').count() as u16 + 2;
        let mut scroll_view = ScrollView::new(Size::new(area.width, lines));

        let paragraph = Paragraph::new(self.value.trim().to_string())
            // .wrap(Wrap { trim: true })
            .style(Style::new().yellow())
            .block(
                Block::new()
                    .title("Inspect Event")
                    .title_style(Style::new().white().bold())
                    .borders(Borders::ALL)
                    .border_style(Style::new().red()),
            );
        scroll_view.render_widget(
            paragraph,
            Rect {
                x: 0,
                y: 0,
                width: area.width - 1,
                height: lines,
            },
        );
        StatefulWidget::render(scroll_view, area, buf, state);
    }
}

fn ciborium_value_to_hjson_value(value: ciborium::Value) -> serde_hjson::Value {
    match value {
        ciborium::Value::Integer(num) => {
            serde_hjson::Value::I64(i128::from(num).try_into().unwrap())
        }
        ciborium::Value::Bytes(bytes) => serde_hjson::Value::String(BASE64_STANDARD.encode(bytes)),
        ciborium::Value::Float(num) => serde_hjson::Value::F64(num),
        ciborium::Value::Text(s) => serde_hjson::Value::String(s),
        ciborium::Value::Bool(b) => serde_hjson::Value::Bool(b),
        ciborium::Value::Null => serde_hjson::Value::Null,
        ciborium::Value::Tag(_, _) => todo!(),
        ciborium::Value::Array(array) => serde_hjson::Value::Array(
            array
                .into_iter()
                .map(ciborium_value_to_hjson_value)
                .collect(),
        ),
        ciborium::Value::Map(map) => serde_hjson::Value::Object(serde_hjson::Map::from_iter(
            map.into_iter().map(|(k, v)| {
                (
                    match ciborium_value_to_hjson_value(k) {
                        serde_hjson::Value::String(s) => s,
                        value => value.to_string(),
                    },
                    ciborium_value_to_hjson_value(v),
                )
            }),
        )),
        _ => todo!(),
    }
}

fn rmp_value_to_hjson_value(value: rmpv::Value) -> serde_hjson::Value {
    match value {
        rmpv::Value::Nil => serde_hjson::Value::Null,
        rmpv::Value::Boolean(b) => serde_hjson::Value::Bool(b),
        rmpv::Value::Integer(int) => match int.as_i64() {
            Some(n) => serde_hjson::Value::I64(n),
            None => serde_hjson::Value::String(int.to_string()),
        },
        rmpv::Value::F32(n) => serde_hjson::Value::F64(n as f64),
        rmpv::Value::F64(n) => serde_hjson::Value::F64(n),
        rmpv::Value::String(s) => {
            serde_hjson::Value::String(s.into_str().unwrap_or_else(|| "<invalid utf8>".to_string()))
        }
        rmpv::Value::Binary(bytes) => {
            serde_hjson::Value::String(BASE64_STANDARD.encode(bytes))
            // serde_hjson::Value::String("<binary>".to_string())
        }
        rmpv::Value::Array(vals) => {
            serde_hjson::Value::Array(vals.into_iter().map(rmp_value_to_hjson_value).collect())
        }
        rmpv::Value::Map(map) => serde_hjson::Value::Object(serde_hjson::Map::from_iter(
            map.into_iter().map(|(k, v)| {
                (
                    match rmp_value_to_hjson_value(k) {
                        serde_hjson::Value::String(s) => s,
                        value => value.to_string(),
                    },
                    rmp_value_to_hjson_value(v),
                )
            }),
        )),
        rmpv::Value::Ext(_, _) => serde_hjson::Value::String("<unknown>".to_string()),
    }
}

fn digits(n: u64) -> u16 {
    const SIZE_TABLE: [u64; 10] = [
        9,
        99,
        999,
        9999,
        99999,
        999999,
        9999999,
        99999999,
        999999999,
        u64::MAX,
    ];
    SIZE_TABLE
        .iter()
        .enumerate()
        .find_map(|(i, st)| (n <= *st).then_some(i + 1))
        .unwrap() as u16
}
