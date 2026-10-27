use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{BooleanArray, Date32Array, RecordBatch};
use arrow::compute::{concat_batches, filter_record_batch, sort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};

mod fetcher;
mod store;

pub use fetcher::TimeseriesFetcher;
pub use store::{CacheStore, FileCacheStore, FileFormat, InMemoryCacheStore};

pub const DATE_COLUMN: &str = "date";

/// Missing runs separated by at most this many cached trading days are fetched as one request.
pub const DEFAULT_COALESCE_GAP: usize = 5;

pub fn ohlc_schema() -> Schema {
    Schema::new(vec![
        Field::new(DATE_COLUMN, DataType::Date32, false),
        Field::new("ticker", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
    ])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchAction {
    /// Fetch the whole requested range, replacing any cached rows for those dates.
    All,
    /// Fetch only the uncached trading days in the requested range (see [`Cache::with_coalesce_gap`]).
    Missing,
}

pub struct Cache {
    store: Box<dyn CacheStore>,
    fetcher: Box<dyn TimeseriesFetcher>,
    coalesce_gap: usize,
}

impl Cache {
    pub fn new(store: Box<dyn CacheStore>, fetcher: Box<dyn TimeseriesFetcher>) -> Self {
        Self {
            store,
            fetcher,
            coalesce_gap: DEFAULT_COALESCE_GAP,
        }
    }

    /// Sets how many cached trading days may separate two missing runs before they are fetched
    /// as separate requests. Within the gap, the cached days are fetched again and replaced.
    /// `0` fetches each contiguous run of missing days on its own.
    pub fn with_coalesce_gap(mut self, trading_days: usize) -> Self {
        self.coalesce_gap = trading_days;
        self
    }

    /// Lists the weekdays in `[start, end]` that have no cached row for `ticker`.
    /// Weekends are never reported as missing.
    pub fn get_missing_dates(
        &self,
        ticker: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<DateTime<Utc>>, ArrowError> {
        let cached = self.store.read(ticker)?;
        let missing = missing_days(cached.as_ref(), start.date_naive(), end.date_naive())?;
        Ok(missing.into_iter().map(at_midnight).collect())
    }

    /// Returns the rows for `[start, end]`, fetching from the source as dictated by `action`.
    ///
    /// Each fetch is merged into the cache and written back before the next one starts, so a
    /// failure part-way keeps what was already fetched. Fetches that return no rows (for example
    /// an exchange holiday) are not written. A failed fetch returns the error rather than
    /// partial data.
    pub fn get_timeseries_data(
        &self,
        ticker: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        action: FetchAction,
    ) -> Result<RecordBatch, ArrowError> {
        let (start_day, end_day) = (start.date_naive(), end.date_naive());
        let mut table = self.store.read(ticker)?;

        let windows = match action {
            FetchAction::All if start_day <= end_day => vec![(start_day, end_day)],
            FetchAction::All => Vec::new(),
            FetchAction::Missing => {
                let missing = missing_days(table.as_ref(), start_day, end_day)?;
                fetch_windows(&missing, self.coalesce_gap)
            }
        };

        for (from, to) in windows {
            let fetched = self
                .fetcher
                .fetch(ticker, at_midnight(from), at_midnight(to))?;
            if fetched.num_rows() == 0 {
                continue;
            }

            let merged = match &table {
                Some(cached) => merge(cached, &fetched)?,
                None => fetched,
            };
            self.store.write(ticker, &merged)?;
            table = Some(merged);
        }

        match table {
            Some(table) => slice_range(&table, start_day, end_day),
            None => Ok(RecordBatch::new_empty(Arc::new(ohlc_schema()))),
        }
    }
}

fn epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch date")
}

fn from_days(days: i32) -> Option<NaiveDate> {
    epoch().checked_add_signed(Duration::days(days as i64))
}

fn at_midnight(day: NaiveDate) -> DateTime<Utc> {
    day.and_hms_opt(0, 0, 0).expect("midnight is valid").and_utc()
}

fn is_trading_day(day: NaiveDate) -> bool {
    !matches!(day.weekday(), Weekday::Sat | Weekday::Sun)
}

fn date_column(batch: &RecordBatch) -> Result<&Date32Array, ArrowError> {
    batch
        .column_by_name(DATE_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<Date32Array>())
        .ok_or_else(|| {
            ArrowError::SchemaError(format!("expected a Date32 column named '{DATE_COLUMN}'"))
        })
}

fn cached_days(batch: &RecordBatch) -> Result<HashSet<NaiveDate>, ArrowError> {
    Ok(date_column(batch)?
        .values()
        .iter()
        .filter_map(|&days| from_days(days))
        .collect())
}

fn missing_days(
    cached: Option<&RecordBatch>,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<NaiveDate>, ArrowError> {
    let present = match cached {
        Some(batch) => cached_days(batch)?,
        None => HashSet::new(),
    };

    let mut missing = Vec::new();
    let mut day = start;
    while day <= end {
        if is_trading_day(day) && !present.contains(&day) {
            missing.push(day);
        }
        day += Duration::days(1);
    }
    Ok(missing)
}

fn trading_days_between(from: NaiveDate, to: NaiveDate) -> usize {
    let mut count = 0;
    let mut day = from + Duration::days(1);
    while day < to {
        if is_trading_day(day) {
            count += 1;
        }
        day += Duration::days(1);
    }
    count
}

/// Groups sorted missing days into inclusive `(first, last)` windows, joining two runs when at most
/// `coalesce_gap` trading days separate them.
fn fetch_windows(missing: &[NaiveDate], coalesce_gap: usize) -> Vec<(NaiveDate, NaiveDate)> {
    let mut windows: Vec<(NaiveDate, NaiveDate)> = Vec::new();
    for &day in missing {
        match windows.last_mut() {
            Some((_, last)) if trading_days_between(*last, day) <= coalesce_gap => *last = day,
            _ => windows.push((day, day)),
        }
    }
    windows
}

fn same_columns(a: &Schema, b: &Schema) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields()
            .iter()
            .zip(b.fields().iter())
            .all(|(x, y)| x.name() == y.name() && x.data_type() == y.data_type())
}

/// Re-wraps `batch` under `schema`, ignoring nullability flags that a round-trip through a file may change.
fn align(batch: &RecordBatch, schema: Arc<Schema>) -> Result<RecordBatch, ArrowError> {
    if !same_columns(batch.schema_ref(), &schema) {
        return Err(ArrowError::SchemaError(
            "fetched batch columns do not match the cached schema".to_string(),
        ));
    }
    RecordBatch::try_new(schema, batch.columns().to_vec())
}

/// Combines cached and fetched rows. Fetched rows win on dates they share with the cache.
fn merge(cached: &RecordBatch, fetched: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let fetched = align(fetched, cached.schema())?;
    let fetched_days = cached_days(&fetched)?;

    let keep: Vec<bool> = date_column(cached)?
        .values()
        .iter()
        .map(|&days| !from_days(days).is_some_and(|day| fetched_days.contains(&day)))
        .collect();
    let kept = filter_record_batch(cached, &BooleanArray::from(keep))?;

    let combined = concat_batches(&cached.schema(), [&kept, &fetched])?;
    let order = sort_to_indices(date_column(&combined)?, None, None)?;
    take_record_batch(&combined, &order)
}

fn slice_range(batch: &RecordBatch, start: NaiveDate, end: NaiveDate) -> Result<RecordBatch, ArrowError> {
    let keep: Vec<bool> = date_column(batch)?
        .values()
        .iter()
        .map(|&days| from_days(days).is_some_and(|day| day >= start && day <= end))
        .collect();
    filter_record_batch(batch, &BooleanArray::from(keep))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use chrono::TimeZone;

    struct NoopFetcher;

    impl TimeseriesFetcher for NoopFetcher {
        fn fetch(&self, _: &str, _: DateTime<Utc>, _: DateTime<Utc>) -> Result<RecordBatch, ArrowError> {
            Ok(RecordBatch::new_empty(Arc::new(ohlc_schema())))
        }
    }

    fn to_days(day: NaiveDate) -> i32 {
        (day - epoch()).num_days() as i32
    }

    fn batch_for(days: &[NaiveDate], close: f64) -> RecordBatch {
        let n = days.len();
        RecordBatch::try_new(
            Arc::new(ohlc_schema()),
            vec![
                Arc::new(Date32Array::from(days.iter().map(|&d| to_days(d)).collect::<Vec<_>>())),
                Arc::new(StringArray::from(vec!["TEST"; n])),
                Arc::new(Float64Array::from(vec![close; n])),
                Arc::new(Float64Array::from(vec![close; n])),
                Arc::new(Float64Array::from(vec![close; n])),
                Arc::new(Float64Array::from(vec![close; n])),
            ],
        )
        .unwrap()
    }

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn utc(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
    }

    #[test]
    fn weekends_are_not_reported_missing() {
        let missing = missing_days(None, day(2026, 6, 1), day(2026, 6, 7)).unwrap();
        let expected: Vec<_> = (1..=5).map(|d| day(2026, 6, d)).collect();
        assert_eq!(missing, expected);
    }

    #[test]
    fn cached_days_are_not_reported_missing() {
        let cached = batch_for(&[day(2026, 6, 1), day(2026, 6, 3)], 1.0);
        let missing = missing_days(Some(&cached), day(2026, 6, 1), day(2026, 6, 5)).unwrap();
        assert_eq!(missing, vec![day(2026, 6, 2), day(2026, 6, 4), day(2026, 6, 5)]);
    }

    #[test]
    fn friday_and_monday_form_one_window() {
        let missing = [day(2026, 6, 5), day(2026, 6, 8)];
        assert_eq!(fetch_windows(&missing, 0), vec![(day(2026, 6, 5), day(2026, 6, 8))]);
    }

    #[test]
    fn windows_split_when_cached_days_exceed_the_gap() {
        let missing = [day(2026, 6, 1), day(2026, 6, 2), day(2026, 6, 15)];
        // Jun 3-5 and Jun 8-12 are cached: 8 trading days between Jun 2 and Jun 15.
        assert_eq!(
            fetch_windows(&missing, 7),
            vec![(day(2026, 6, 1), day(2026, 6, 2)), (day(2026, 6, 15), day(2026, 6, 15))]
        );
        assert_eq!(fetch_windows(&missing, 8), vec![(day(2026, 6, 1), day(2026, 6, 15))]);
    }

    #[test]
    fn merge_prefers_fetched_rows_and_sorts_by_date() {
        let cached = batch_for(&[day(2026, 6, 2), day(2026, 6, 1)], 1.0);
        let fetched = batch_for(&[day(2026, 6, 2), day(2026, 6, 3)], 2.0);
        let merged = merge(&cached, &fetched).unwrap();

        let dates = date_column(&merged).unwrap().values().to_vec();
        let closes = merged
            .column_by_name("close")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec();

        assert_eq!(dates, vec![to_days(day(2026, 6, 1)), to_days(day(2026, 6, 2)), to_days(day(2026, 6, 3))]);
        assert_eq!(closes, vec![1.0, 2.0, 2.0]);
    }

    #[test]
    fn fetch_all_with_reversed_range_does_not_fetch() {
        struct PanicFetcher;
        impl TimeseriesFetcher for PanicFetcher {
            fn fetch(&self, _: &str, _: DateTime<Utc>, _: DateTime<Utc>) -> Result<RecordBatch, ArrowError> {
                panic!("fetch should not be called");
            }
        }

        let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(PanicFetcher));
        let batch = cache
            .get_timeseries_data("TEST", utc(2026, 6, 5), utc(2026, 6, 1), FetchAction::All)
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn empty_cache_with_no_trading_days_returns_empty_batch() {
        let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(NoopFetcher));
        let batch = cache
            .get_timeseries_data("TEST", utc(2026, 6, 6), utc(2026, 6, 7), FetchAction::Missing)
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema_ref().fields().len(), 6);
    }
}
