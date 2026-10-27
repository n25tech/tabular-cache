mod common;

use common::{MockFetcher, dates_of, days_since_epoch, utc};
use tabular_cache::{Cache, FetchAction, InMemoryCacheStore};

fn cache_with_mock() -> (Cache, common::Calls) {
    let (fetcher, calls) = MockFetcher::new();
    let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(fetcher));
    (cache, calls)
}

#[test]
fn missing_fetch_populates_cache_and_returns_trading_days_only() {
    let (cache, calls) = cache_with_mock();
    let start = utc(2026, 6, 1);
    let end = utc(2026, 6, 7);

    let batch = cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing).unwrap();

    assert_eq!(batch.num_rows(), 5);
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(
        dates_of(&batch),
        (1..=5).map(|d| days_since_epoch(chrono::NaiveDate::from_ymd_opt(2026, 6, d).unwrap())).collect::<Vec<_>>()
    );
}

#[test]
fn repeat_request_is_served_from_cache_without_fetching() {
    let (cache, calls) = cache_with_mock();
    let start = utc(2026, 6, 1);
    let end = utc(2026, 6, 7);

    cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing).unwrap();
    let batch = cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing).unwrap();

    assert_eq!(batch.num_rows(), 5);
    assert_eq!(calls.lock().unwrap().len(), 1);
}

#[test]
fn only_the_missing_span_is_fetched_when_range_extends() {
    let (cache, calls) = cache_with_mock();

    cache
        .get_timeseries_data("AAPL", utc(2026, 6, 1), utc(2026, 6, 5), FetchAction::Missing)
        .unwrap();
    let batch = cache
        .get_timeseries_data("AAPL", utc(2026, 6, 1), utc(2026, 6, 8), FetchAction::Missing)
        .unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1], (utc(2026, 6, 8), utc(2026, 6, 8)));
    assert_eq!(batch.num_rows(), 6);
}

#[test]
fn missing_dates_skip_weekends_and_shrink_as_cache_fills() {
    let (cache, _) = cache_with_mock();
    let start = utc(2026, 6, 1);
    let end = utc(2026, 6, 7);

    let before = cache.get_missing_dates("AAPL", start, end).unwrap();
    assert_eq!(before, (1..=5).map(|d| utc(2026, 6, d)).collect::<Vec<_>>());

    cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing).unwrap();
    assert!(cache.get_missing_dates("AAPL", start, end).unwrap().is_empty());
}

#[test]
fn fetch_all_refetches_the_whole_range() {
    let (cache, calls) = cache_with_mock();
    let start = utc(2026, 6, 1);
    let end = utc(2026, 6, 5);

    cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing).unwrap();
    cache.get_timeseries_data("AAPL", start, end, FetchAction::All).unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1], (start, end));
}

#[test]
fn tickers_are_cached_independently() {
    let (cache, calls) = cache_with_mock();
    let start = utc(2026, 6, 1);
    let end = utc(2026, 6, 5);

    cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing).unwrap();
    let msft = cache.get_timeseries_data("MSFT", start, end, FetchAction::Missing).unwrap();

    assert_eq!(calls.lock().unwrap().len(), 2);
    assert_eq!(msft.num_rows(), 5);
}

#[test]
fn missing_runs_are_fetched_separately_without_refetching_cached_days() {
    let (fetcher, calls) = MockFetcher::new();
    let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(fetcher)).with_coalesce_gap(0);

    cache
        .get_timeseries_data("AAPL", utc(2026, 6, 8), utc(2026, 6, 12), FetchAction::Missing)
        .unwrap();
    calls.lock().unwrap().clear();

    let batch = cache
        .get_timeseries_data("AAPL", utc(2026, 6, 1), utc(2026, 6, 19), FetchAction::Missing)
        .unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        vec![(utc(2026, 6, 1), utc(2026, 6, 5)), (utc(2026, 6, 15), utc(2026, 6, 19))]
    );
    assert_eq!(batch.num_rows(), 15);
}

#[test]
fn small_gaps_between_missing_runs_are_fetched_in_one_request() {
    let (fetcher, calls) = MockFetcher::new();
    let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(fetcher)).with_coalesce_gap(5);

    cache
        .get_timeseries_data("AAPL", utc(2026, 6, 8), utc(2026, 6, 12), FetchAction::Missing)
        .unwrap();
    calls.lock().unwrap().clear();

    cache
        .get_timeseries_data("AAPL", utc(2026, 6, 1), utc(2026, 6, 19), FetchAction::Missing)
        .unwrap();

    assert_eq!(*calls.lock().unwrap(), vec![(utc(2026, 6, 1), utc(2026, 6, 19))]);
}

#[test]
fn rows_fetched_before_a_failure_are_kept() {
    let (fetcher, calls) = MockFetcher::failing_on(3);
    let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(fetcher)).with_coalesce_gap(0);

    cache
        .get_timeseries_data("AAPL", utc(2026, 6, 8), utc(2026, 6, 12), FetchAction::Missing)
        .unwrap();

    // Call 2 (Jun 1-5) succeeds and is stored, call 3 (Jun 15-19) fails.
    let result = cache.get_timeseries_data("AAPL", utc(2026, 6, 1), utc(2026, 6, 19), FetchAction::Missing);
    assert!(result.is_err());
    assert_eq!(calls.lock().unwrap().len(), 3);

    let still_missing = cache.get_missing_dates("AAPL", utc(2026, 6, 1), utc(2026, 6, 19)).unwrap();
    assert_eq!(still_missing, (15..=19).map(|d| utc(2026, 6, d)).collect::<Vec<_>>());
}

#[test]
fn empty_fetch_returns_no_rows_and_leaves_the_days_missing() {
    let (fetcher, calls) = MockFetcher::empty();
    let cache = Cache::new(Box::new(InMemoryCacheStore::new()), Box::new(fetcher));
    let (start, end) = (utc(2026, 6, 1), utc(2026, 6, 5));

    let batch = cache.get_timeseries_data("AAPL", start, end, FetchAction::Missing).unwrap();

    assert_eq!(batch.num_rows(), 0);
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(cache.get_missing_dates("AAPL", start, end).unwrap().len(), 5);
}
