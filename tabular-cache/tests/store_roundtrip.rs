mod common;

use std::path::PathBuf;

use common::{MockFetcher, dates_of, utc};
use tabular_cache::{Cache, CacheStore, FetchAction, FileCacheStore, FileFormat, TimeseriesFetcher};

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tabular-cache-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn round_trip(format: FileFormat, name: &str) {
    let dir = scratch_dir(name);
    let (fetcher, calls) = MockFetcher::new();
    let start = utc(2026, 6, 1);
    let end = utc(2026, 6, 7);

    let written = Cache::new(Box::new(FileCacheStore::new(&dir, format)), Box::new(fetcher))
        .get_timeseries_data("AAPL", start, end, FetchAction::Missing)
        .unwrap();
    assert_eq!(written.num_rows(), 5);

    // A fresh store over the same directory should serve the data without fetching again.
    let (fetcher, reread_calls) = MockFetcher::new();
    let reread = Cache::new(Box::new(FileCacheStore::new(&dir, format)), Box::new(fetcher))
        .get_timeseries_data("AAPL", start, end, FetchAction::Missing)
        .unwrap();

    assert!(reread_calls.lock().unwrap().is_empty());
    assert_eq!(dates_of(&reread), dates_of(&written));
    assert_eq!(calls.lock().unwrap().len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn parquet_store_round_trips_through_the_cache() {
    round_trip(FileFormat::Parquet, "parquet");
}

#[test]
fn csv_store_round_trips_through_the_cache() {
    round_trip(FileFormat::Csv, "csv");
}

#[test]
fn parquet_preserves_column_types() {
    let dir = scratch_dir("parquet-types");
    let store = FileCacheStore::new(&dir, FileFormat::Parquet);
    let (fetcher, _) = MockFetcher::new();
    let batch = fetcher.fetch("AAPL", utc(2026, 6, 1), utc(2026, 6, 2)).unwrap();

    store.write("AAPL", &batch).unwrap();
    let read = store.read("AAPL").unwrap().unwrap();

    assert_eq!(read.schema_ref().fields(), batch.schema_ref().fields());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn missing_ticker_reads_as_none() {
    let dir = scratch_dir("missing");
    let store = FileCacheStore::new(&dir, FileFormat::Csv);
    assert!(store.read("NOPE").unwrap().is_none());
}

#[test]
fn csv_cache_with_numeric_ticker_can_be_extended() {
    let dir = scratch_dir("numeric-ticker");
    let (fetcher, _) = MockFetcher::new();
    let cache = Cache::new(Box::new(FileCacheStore::new(&dir, FileFormat::Csv)), Box::new(fetcher));

    cache
        .get_timeseries_data("500180", utc(2026, 6, 1), utc(2026, 6, 3), FetchAction::Missing)
        .unwrap();
    let extended = cache
        .get_timeseries_data("500180", utc(2026, 6, 1), utc(2026, 6, 10), FetchAction::Missing)
        .unwrap();

    assert_eq!(extended.num_rows(), 8);
    let _ = std::fs::remove_dir_all(&dir);
}

fn empty_fetch_then_data(format: FileFormat, name: &str) {
    let dir = scratch_dir(name);
    let (start, end) = (utc(2026, 6, 1), utc(2026, 6, 5));

    let (empty, _) = MockFetcher::empty();
    let batch = Cache::new(Box::new(FileCacheStore::new(&dir, format)), Box::new(empty))
        .get_timeseries_data("AAPL", start, end, FetchAction::Missing)
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert!(!dir.join(format!("AAPL.{}", if format == FileFormat::Csv { "csv" } else { "parquet" })).exists());
    assert!(dir.join("AAPL.empty-days").exists());

    // A fresh cache over the same directory remembers the empty days and does not fetch them.
    let (fetcher, calls) = MockFetcher::new();
    let cache = Cache::new(Box::new(FileCacheStore::new(&dir, format)), Box::new(fetcher));
    let batch = cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert!(calls.lock().unwrap().is_empty());

    // FetchAction::All asks again, and forgets the days once they have data.
    let batch = cache.get_timeseries_data("AAPL", start, end, FetchAction::All).unwrap();
    assert_eq!(batch.num_rows(), 5);
    assert!(!dir.join("AAPL.empty-days").exists());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn csv_cache_remembers_an_empty_fetch_until_fetch_all() {
    empty_fetch_then_data(FileFormat::Csv, "csv-empty");
}

#[test]
fn parquet_cache_remembers_an_empty_fetch_until_fetch_all() {
    empty_fetch_then_data(FileFormat::Parquet, "parquet-empty");
}

#[test]
fn csv_merge_after_reading_a_header_only_file_works() {
    let dir = scratch_dir("header-only");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("AAPL.csv"), "date,ticker,open,high,low,close\n").unwrap();

    let (fetcher, _) = MockFetcher::new();
    let batch = Cache::new(Box::new(FileCacheStore::new(&dir, FileFormat::Csv)), Box::new(fetcher))
        .get_timeseries_data("AAPL", utc(2026, 6, 1), utc(2026, 6, 5), FetchAction::Missing)
        .unwrap();

    assert_eq!(batch.num_rows(), 5);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn writes_leave_no_temporary_files_behind() {
    for (format, name) in [(FileFormat::Csv, "tmp-csv"), (FileFormat::Parquet, "tmp-parquet")] {
        let dir = scratch_dir(name);
        let (fetcher, _) = MockFetcher::new();
        Cache::new(Box::new(FileCacheStore::new(&dir, format)), Box::new(fetcher))
            .get_timeseries_data("AAPL", utc(2026, 6, 1), utc(2026, 6, 5), FetchAction::Missing)
            .unwrap();

        let names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 1, "unexpected files: {names:?}");
        assert!(!names[0].ends_with(".tmp"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
