use chrono::{Duration, Utc};
use tabular_cache::{Cache, FetchAction, FileCacheStore, FileFormat};

mod yfinance;

use yfinance::YFinanceFetcher;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let symbol = "IDFCFIRSTB.NS";
    let end = Utc::now();
    let start = end - Duration::days(30);

    let cache = Cache::new(
        Box::new(FileCacheStore::new("data/cache", FileFormat::Parquet)),
        Box::new(YFinanceFetcher::new()?),
    );

    let batch = cache.get_timeseries_data(symbol, start, end, FetchAction::Missing)?;
    println!("{symbol}: {} rows", batch.num_rows());

    arrow::csv::Writer::new(std::io::stdout()).write(&batch)?;
    Ok(())
}
