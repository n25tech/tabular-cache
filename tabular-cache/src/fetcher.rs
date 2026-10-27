use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use chrono::{DateTime, Utc};

/// Fetches rows for one ticker from an external source.
///
/// [`Cache`](crate::Cache) calls this with the first and last day wanted, both at midnight UTC
/// and both inclusive. An implementation must:
///
/// - return the same columns for every call on a ticker, including a non-null `Date32`
///   [`DATE_COLUMN`](crate::DATE_COLUMN);
/// - return at most one row per date, dated by the exchange's local calendar date;
/// - leave out a session that has not closed yet, or it is cached as final;
/// - return `Ok` without a row for a date only when the source has no data for it, because the
///   cache remembers that day as empty; report failures as `Err` instead.
pub trait TimeseriesFetcher: Send + Sync {
    fn fetch(
        &self,
        ticker: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<RecordBatch, ArrowError>;
}
