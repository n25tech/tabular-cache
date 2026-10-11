use chrono::{DateTime, Utc};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;

pub struct Cache {
    r#type: String,
}

pub fn new() -> Cache {
    let cache = Cache {
        r#type: String::from("file:csv"),
    };
    return cache;
}

impl Cache {
    pub fn get_timeseries_data(
        self,
        key: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
        fetcher: impl Fn(DateTime<Utc>, DateTime<Utc>, &str) -> Result<RecordBatch, ArrowError>,
    ) -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("close", DataType::Float64, false),
        ]));

        let tickers: ArrayRef = Arc::new(StringArray::from(vec!["AAPL", "MSFT", "GOOG"]));

        let closes: ArrayRef = Arc::new(Float64Array::from(vec![150.0, 320.0, 140.0]));
        let ticker = "IDFCFIRSTB";
        fetcher(start_date, end_date, ticker)
        //RecordBatch::try_new(schema, vec![tickers, closes])
    }

    pub fn add(left: u64, right: u64) -> u64 {
        left + right
    }
}
