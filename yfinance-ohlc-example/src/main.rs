use chrono::{DateTime, Utc};
use std::sync::Arc;
use tabular_cache;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;

mod yfinance;

fn fetch_yfinance_data(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    ticker: &str,
) -> Result<RecordBatch, ArrowError> {

    let ticker_symbol = "IDFCFIRSTB.NS";
    yfinance::get_nse_equity_for_ticker(ticker_symbol);

    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::Utf8, false),
        Field::new("close", DataType::Float64, false),
    ]));

    let tickers: ArrayRef = Arc::new(StringArray::from(vec!["AAPL", "MSFT", "GOOG"]));

    let closes: ArrayRef = Arc::new(Float64Array::from(vec![150.0, 320.0, 140.0]));

    return RecordBatch::try_new(schema, vec![tickers, closes]);
}

fn main() {
    /*
    let result = add(2, 2);
    println!("Hello, world!");
    println!("{}", result);
     */

    let cache = tabular_cache::new();
    let start_date = Utc::now();
    let end_date = Utc::now();
    let data = cache.get_timeseries_data("Timestamp", start_date, end_date, fetch_yfinance_data);
}
