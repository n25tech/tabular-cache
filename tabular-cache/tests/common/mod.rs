#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use arrow::array::{Date32Array, Float64Array, RecordBatch, StringArray};
use arrow::error::ArrowError;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use tabular_cache::{TimeseriesFetcher, ohlc_schema};

pub type Calls = Arc<Mutex<Vec<(DateTime<Utc>, DateTime<Utc>)>>>;

/// Produces one OHLC row per weekday in the requested window and records every call.
pub struct MockFetcher {
    pub calls: Calls,
    /// Return no rows, like a source asked about an exchange holiday.
    pub empty: bool,
    /// Fail on this (1-based) call number.
    pub fail_on_call: Option<usize>,
}

impl MockFetcher {
    pub fn new() -> (Self, Calls) {
        let calls: Calls = Arc::default();
        (
            Self {
                calls: calls.clone(),
                empty: false,
                fail_on_call: None,
            },
            calls,
        )
    }

    pub fn empty() -> (Self, Calls) {
        let (mut fetcher, calls) = Self::new();
        fetcher.empty = true;
        (fetcher, calls)
    }

    pub fn failing_on(call: usize) -> (Self, Calls) {
        let (mut fetcher, calls) = Self::new();
        fetcher.fail_on_call = Some(call);
        (fetcher, calls)
    }
}

impl TimeseriesFetcher for MockFetcher {
    fn fetch(
        &self,
        ticker: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<RecordBatch, ArrowError> {
        let call_number = {
            let mut calls = self.calls.lock().unwrap();
            calls.push((start, end));
            calls.len()
        };
        if self.fail_on_call == Some(call_number) {
            return Err(ArrowError::ComputeError("source unavailable".to_string()));
        }

        let days: Vec<NaiveDate> = if self.empty {
            Vec::new()
        } else {
            weekdays(start.date_naive(), end.date_naive())
        };
        let n = days.len();
        let closes: Vec<f64> = days.iter().map(|d| 100.0 + d.day() as f64).collect();

        RecordBatch::try_new(
            Arc::new(ohlc_schema()),
            vec![
                Arc::new(Date32Array::from(days.iter().map(|&d| days_since_epoch(d)).collect::<Vec<_>>())),
                Arc::new(StringArray::from(vec![ticker; n])),
                Arc::new(Float64Array::from(closes.clone())),
                Arc::new(Float64Array::from(closes.clone())),
                Arc::new(Float64Array::from(closes.clone())),
                Arc::new(Float64Array::from(closes)),
            ],
        )
    }
}

pub fn weekdays(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut out = Vec::new();
    let mut day = start;
    while day <= end {
        if !matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
            out.push(day);
        }
        day += Duration::days(1);
    }
    out
}

pub fn days_since_epoch(day: NaiveDate) -> i32 {
    (day - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
}

pub fn utc(y: i32, m: u32, d: u32) -> DateTime<Utc> {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

pub fn dates_of(batch: &RecordBatch) -> Vec<i32> {
    batch
        .column_by_name("date")
        .unwrap()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap()
        .values()
        .to_vec()
}
