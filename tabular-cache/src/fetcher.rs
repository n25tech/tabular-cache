use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use chrono::{DateTime, Utc};

pub trait TimeseriesFetcher: Send + Sync {
    fn fetch(
        &self,
        ticker: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<RecordBatch, ArrowError>;
}
