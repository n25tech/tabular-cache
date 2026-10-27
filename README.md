# tabular-cache

A local cache for daily time-series tables, such as OHLC price data, stored as
[Apache Arrow](https://arrow.apache.org/) `RecordBatch`es.

You ask the cache for a ticker and a date range. It returns what it already has
and fetches only the trading days it is missing from your data source. Then it
saves the merged table, so the next request for the same range needs no
network calls.

- **Pluggable source:** implement `TimeseriesFetcher` for any API.
- **Pluggable storage:** in memory, CSV, Parquet, or your own `CacheStore`.
- **Few requests:** nearby gaps are combined into a single request.
- **Weekends and holidays** are never fetched more than once.

The workspace has two crates:

| Crate | What it is |
| --- | --- |
| `tabular-cache` | The library. |
| `yfinance-ohlc-example` | A runnable example that caches daily prices from Yahoo Finance. |

## Quick start

Add the library and its Arrow and chrono dependencies:

```toml
[dependencies]
tabular-cache = { path = "../tabular-cache" }
arrow = "60"
chrono = "0.4"
```

A cache needs a **store** (where tables are kept) and a **fetcher** (where
missing data comes from):

```rust
use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, RecordBatch, StringArray};
use arrow::error::ArrowError;
use chrono::{DateTime, Datelike, NaiveDate, Utc, Weekday};
use tabular_cache::{Cache, FetchAction, InMemoryCacheStore, TimeseriesFetcher, ohlc_schema};

/// Stands in for a real API: returns a flat price for every weekday.
struct FlatPrices;

impl TimeseriesFetcher for FlatPrices {
    fn fetch(
        &self,
        ticker: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<RecordBatch, ArrowError> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days: Vec<i32> = start
            .date_naive()
            .iter_days()
            .take_while(|day| *day <= end.date_naive())
            .filter(|day| !matches!(day.weekday(), Weekday::Sat | Weekday::Sun))
            .map(|day| (day - epoch).num_days() as i32)
            .collect();
        let n = days.len();

        RecordBatch::try_new(
            Arc::new(ohlc_schema()),
            vec![
                Arc::new(Date32Array::from(days)),
                Arc::new(StringArray::from(vec![ticker; n])),
                Arc::new(Float64Array::from(vec![100.0; n])),
                Arc::new(Float64Array::from(vec![101.0; n])),
                Arc::new(Float64Array::from(vec![99.0; n])),
                Arc::new(Float64Array::from(vec![100.5; n])),
            ],
        )
    }
}

fn main() -> Result<(), ArrowError> {
    let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(FlatPrices));

    let start = "2026-06-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let end = "2026-06-07T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

    // The first call fetches Mon Jun 1 to Fri Jun 5. Weekends are never requested.
    let batch = cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing)?;
    assert_eq!(batch.num_rows(), 5);

    // The second call is served entirely from the cache.
    assert!(cache.get_missing_dates("AAPL", start, end)?.is_empty());
    let batch = cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing)?;
    assert_eq!(batch.num_rows(), 5);
    Ok(())
}
```

The returned batch holds only the rows from `start` to `end` (both inclusive),
sorted by date. Only the calendar date of `start` and `end` (in UTC) is used;
the time of day is ignored.

## Core concepts

### Fetching: `FetchAction`

| Action | What it fetches |
| --- | --- |
| `FetchAction::Missing` | Only the weekdays in the range that are not cached yet, minus days already known to have no data. This is the normal choice. |
| `FetchAction::All` | The whole range, in one request. Returned rows replace any cached rows for the same dates. Use it to refresh data that may have been corrected at the source. |

Use `Cache::get_missing_dates` to see what a `Missing` request would fetch, without
fetching anything.

### Combining gaps: `with_coalesce_gap`

Suppose you have Jun 8–12 cached and request Jun 1–19. Two runs of days are
missing, one on each side. By default (`DEFAULT_COALESCE_GAP = 5`), runs
separated by up to 5 cached trading days are fetched as **one** request, and
the cached days in between are fetched again and replaced. Raise the value to
make fewer, larger requests; set it to `0` to never fetch a day that is already
cached:

```rust
# use tabular_cache::*;
# struct F;
# impl TimeseriesFetcher for F {
#     fn fetch(&self, _: &str, _: chrono::DateTime<chrono::Utc>, _: chrono::DateTime<chrono::Utc>)
#         -> Result<arrow::array::RecordBatch, arrow::error::ArrowError> { unimplemented!() }
# }
# let fetcher = F;
let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(fetcher))
    .with_coalesce_gap(0);
```

Friday and the following Monday always count as adjacent, because the weekend
between them is never cached.

### Weekends, holidays and other days with no data

- **Weekends** are never treated as missing and never requested.
- **Holidays:** if a fetch succeeds but returns no row for a weekday, the cache
  records that day as *empty* for that ticker. `FetchAction::Missing` will not
  request it again. This also covers dates before a ticker was listed.
- **Today and later** are never recorded as empty, because the session may not
  have closed yet. The cutoff is the current date in UTC.
- **Correcting a mistake:** if a source wrongly returned nothing for a day,
  run `FetchAction::All` over that range. It requests the days again and
  forgets any that now have data.

### Failures

Each request's result is saved before the next request starts. If request 3 of 3
fails, the call returns the error, but the data from requests 1 and 2 stays
cached, and the next call fetches only what is still missing.

## Stores

| Store | Use it for |
| --- | --- |
| `InMemoryCacheStore` | Tests, or caching for the life of one process. |
| `FileCacheStore::new(dir, FileFormat::Parquet)` | Persistent caching. Column types are kept exactly. **Recommended.** |
| `FileCacheStore::new(dir, FileFormat::Csv)` | Persistent caching you want to read or edit by hand. |

`FileCacheStore` writes these files to `dir`, which it creates if needed:

```text
dir/
├── AAPL.parquet       # the ticker's table (or AAPL.csv)
└── AAPL.empty-days    # days known to have no data, one YYYY-MM-DD per line (only when there are some)
```

Each file is written to a temporary file and then renamed into place, so a
crash never leaves a half-written table. A ticker containing `/` or `\`, or one
that is empty, `.` or `..`, is rejected, so it can't write outside `dir`.

**CSV column types.** CSV files don't record types, so on read the store uses
the types from `ohlc_schema()` for the columns named there and infers the rest.
If your table has extra columns whose inferred type would be wrong, list their
types explicitly:

```rust
use arrow::datatypes::{DataType, Field, Schema};
use tabular_cache::{FileCacheStore, FileFormat};

let mut fields: Vec<Field> = tabular_cache::ohlc_schema()
    .fields()
    .iter()
    .map(|field| field.as_ref().clone())
    .collect();
fields.push(Field::new("volume", DataType::Float64, false));

let store = FileCacheStore::new("data/cache", FileFormat::Csv)
    .with_column_types(Schema::new(fields));
```

## Writing a fetcher

Implement `TimeseriesFetcher`. Its `fetch` method receives the ticker and the
first and last day wanted (as midnight UTC, both inclusive). The rules:

1. **Columns.** Every batch for a ticker must have the same column names and
   types, including a non-null `Date32` column named `date` (`DATE_COLUMN`).
   `ohlc_schema()` is a ready-made schema; you may use your own.
2. **One row per date.** Return at most one row for each date. Rows are matched
   by date, and rows you return replace cached rows for the same date.
3. **No data vs. failure.** Return `Ok` with an empty batch (or a batch without a
   given date) only when the source really has no data for those days. The cache
   will remember them as empty. For network errors, rate limits and the like,
   return `Err`. You can wrap any error in `ArrowError::ExternalError`.
4. **No unfinished days.** Leave out the bar for a session that is still
   trading. Otherwise the partial bar is cached as final and `Missing` never
   refreshes it.
5. **Exchange-local dates.** Date each row by the exchange's local calendar
   date, not the UTC date of its timestamp.

`yfinance-ohlc-example/src/yfinance.rs` is a complete fetcher for the Yahoo
Finance chart API that follows all of these rules.

## Writing a store

Implement `CacheStore` to keep tables somewhere else, such as a database or
object storage:

| Method | Must do |
| --- | --- |
| `read(ticker)` | Return the ticker's table, or `Ok(None)` if there isn't one. |
| `write(ticker, batch)` | Replace the ticker's table with `batch`. It is always the full table, not just new rows. |
| `read_empty_days(ticker)` | Return the recorded empty days, or an empty set. |
| `write_empty_days(ticker, days)` | Replace the recorded empty days with `days`. |

Stores must be `Send + Sync`.

## Running the Yahoo Finance example

```sh
cargo run -p yfinance-ohlc-example
```

This caches the last 30 days of `IDFCFIRSTB.NS` in `data/cache/` (relative to
the current directory) and prints them as CSV. Run it a second time and it makes
no requests for days already cached.

## Limitations

- **Weekdays only.** A trading day is any Monday to Friday. Exchanges that trade
  on weekends are not supported.
- **One process per ticker.** Two calls for the same ticker at the same time
  (threads or processes) each read, merge and write the whole table, so the
  last write can drop the other's new rows. Calls for different tickers are
  independent.
- **Whole table in memory.** Each call reads and rewrites a ticker's entire
  table. That's fine for daily data, even decades of it, but this is not
  designed for intraday data.

## Development

```sh
cargo test --workspace     # unit, integration and README examples
cargo clippy --workspace --all-targets
```
