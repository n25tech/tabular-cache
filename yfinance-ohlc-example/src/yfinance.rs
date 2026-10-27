use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, RecordBatch, StringArray};
use arrow::error::ArrowError;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use tabular_cache::{TimeseriesFetcher, ohlc_schema};

const CHART_URL: &str = "https://query1.finance.yahoo.com/v8/finance/chart";

pub struct YFinanceFetcher {
    client: reqwest::blocking::Client,
}

impl YFinanceFetcher {
    pub fn new() -> Result<Self, reqwest::Error> {
        let client = reqwest::blocking::Client::builder()
            .user_agent("Mozilla/5.0")
            .build()?;
        Ok(Self { client })
    }
}

impl TimeseriesFetcher for YFinanceFetcher {
    fn fetch(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<RecordBatch, ArrowError> {
        let url = format!(
            "{CHART_URL}/{symbol}?interval=1d&period1={}&period2={}",
            start.timestamp(),
            end.timestamp() + 86_400,
        );

        let body: Value = self
            .client
            .get(&url)
            .send()
            .and_then(|response| response.error_for_status())
            .and_then(|response| response.json())
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        parse_chart(symbol, &body, Utc::now())
    }
}

/// Builds one row per exchange-local trading day. Bars from a session that has not closed yet at
/// `now` are dropped, so a partial day is never cached as if it were final.
fn parse_chart(symbol: &str, body: &Value, now: DateTime<Utc>) -> Result<RecordBatch, ArrowError> {
    let result = body["chart"]["result"]
        .get(0)
        .ok_or_else(|| ArrowError::ParseError(format!("no chart data for {symbol}: {}", body["chart"]["error"])))?;

    let timestamps = result["timestamp"].as_array().cloned().unwrap_or_default();
    let quote = &result["indicators"]["quote"][0];
    let column = |name: &str| -> Vec<Option<f64>> {
        quote[name]
            .as_array()
            .map(|values| values.iter().map(Value::as_f64).collect())
            .unwrap_or_default()
    };
    let (opens, highs, lows, closes) = (column("open"), column("high"), column("low"), column("close"));

    // Bars are stamped with the session open in UTC; shift to exchange time before taking the date.
    let gmtoffset = result["meta"]["gmtoffset"].as_i64().unwrap_or(0);
    let session = &result["meta"]["currentTradingPeriod"]["regular"];
    let open_session_start = match (session["start"].as_i64(), session["end"].as_i64()) {
        (Some(start), Some(end)) if now.timestamp() < end => Some(start),
        _ => None,
    };

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch date");
    let mut days: Vec<i32> = Vec::new();
    let (mut open_values, mut high_values, mut low_values, mut close_values) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());

    for (i, timestamp) in timestamps.iter().enumerate() {
        let at = |column: &[Option<f64>]| column.get(i).copied().flatten();
        let (Some(seconds), Some(open), Some(high), Some(low), Some(close)) =
            (timestamp.as_i64(), at(&opens), at(&highs), at(&lows), at(&closes))
        else {
            continue;
        };
        if open_session_start.is_some_and(|start| seconds >= start) {
            continue;
        }

        let day = DateTime::from_timestamp(seconds + gmtoffset, 0)
            .ok_or_else(|| ArrowError::ParseError(format!("bad timestamp {seconds}")))?
            .date_naive();
        let day = (day - epoch).num_days() as i32;

        // Yahoo can repeat the latest day; keep its last bar so the batch has one row per date.
        if days.last() == Some(&day) {
            days.pop();
            open_values.pop();
            high_values.pop();
            low_values.pop();
            close_values.pop();
        }
        days.push(day);
        open_values.push(open);
        high_values.push(high);
        low_values.push(low);
        close_values.push(close);
    }

    let rows = days.len();
    RecordBatch::try_new(
        Arc::new(ohlc_schema()),
        vec![
            Arc::new(Date32Array::from(days)),
            Arc::new(StringArray::from(vec![symbol; rows])),
            Arc::new(Float64Array::from(open_values)),
            Arc::new(Float64Array::from(high_values)),
            Arc::new(Float64Array::from(low_values)),
            Arc::new(Float64Array::from(close_values)),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Date32Array;
    use serde_json::json;

    fn chart(timestamps: &[i64], session: (i64, i64)) -> Value {
        let prices: Vec<f64> = (0..timestamps.len()).map(|i| 100.0 + i as f64).collect();
        json!({"chart": {"result": [{
            "meta": {
                "gmtoffset": 36_000,
                "currentTradingPeriod": {"regular": {"start": session.0, "end": session.1}},
            },
            "timestamp": timestamps,
            "indicators": {"quote": [{
                "open": prices, "high": prices, "low": prices, "close": prices,
            }]},
        }]}})
    }

    fn dates(batch: &RecordBatch) -> Vec<NaiveDate> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .values()
            .iter()
            .map(|&d| epoch + chrono::Duration::days(d as i64))
            .collect()
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
    }

    #[test]
    fn dates_use_exchange_time_not_utc() {
        // A UTC+10 exchange opening at 00:00 UTC on Jun 1 is trading on Jun 1 local, and one
        // opening at 23:00 UTC on May 31 is also on Jun 1 local.
        let body = chart(&[at("2026-05-31T23:00:00Z").timestamp()], (0, 0));
        let batch = parse_chart("TEST", &body, at("2026-06-02T00:00:00Z")).unwrap();
        assert_eq!(dates(&batch), vec![NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()]);
    }

    #[test]
    fn bars_from_an_unfinished_session_are_dropped() {
        let (open, close) = (at("2026-06-02T00:00:00Z"), at("2026-06-02T06:00:00Z"));
        let body = chart(
            &[at("2026-06-01T00:00:00Z").timestamp(), open.timestamp()],
            (open.timestamp(), close.timestamp()),
        );

        let during = parse_chart("TEST", &body, at("2026-06-02T03:00:00Z")).unwrap();
        assert_eq!(during.num_rows(), 1);

        let after = parse_chart("TEST", &body, at("2026-06-02T07:00:00Z")).unwrap();
        assert_eq!(after.num_rows(), 2);
    }

    #[test]
    fn repeated_days_keep_the_last_bar() {
        let first = at("2026-06-01T00:00:00Z").timestamp();
        let body = chart(&[first, first + 3_600], (0, 0));
        let batch = parse_chart("TEST", &body, at("2026-06-03T00:00:00Z")).unwrap();

        let close = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(close.value(0), 101.0);
    }
}
