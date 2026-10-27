use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::csv::reader::Format;
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use chrono::NaiveDate;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Persists one table per ticker. Implement this to add a new storage format or backend.
pub trait CacheStore: Send + Sync {
    fn read(&self, ticker: &str) -> Result<Option<RecordBatch>, ArrowError>;
    fn write(&self, ticker: &str, batch: &RecordBatch) -> Result<(), ArrowError>;

    /// Days the source was asked about and had no data for (e.g. exchange holidays).
    fn read_empty_days(&self, ticker: &str) -> Result<BTreeSet<NaiveDate>, ArrowError>;
    fn write_empty_days(&self, ticker: &str, days: &BTreeSet<NaiveDate>) -> Result<(), ArrowError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    /// Column types come from the store's column types (see [`FileCacheStore::with_column_types`]);
    /// any other column is inferred from the text on read.
    Csv,
    /// Schema is stored in the file, so types are preserved exactly.
    Parquet,
}

impl FileFormat {
    fn extension(self) -> &'static str {
        match self {
            FileFormat::Csv => "csv",
            FileFormat::Parquet => "parquet",
        }
    }
}

#[derive(Default)]
pub struct InMemoryCacheStore {
    tables: Mutex<HashMap<String, RecordBatch>>,
    empty_days: Mutex<HashMap<String, BTreeSet<NaiveDate>>>,
}

impl InMemoryCacheStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CacheStore for InMemoryCacheStore {
    fn read(&self, ticker: &str) -> Result<Option<RecordBatch>, ArrowError> {
        Ok(self.tables.lock().unwrap().get(ticker).cloned())
    }

    fn write(&self, ticker: &str, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.tables
            .lock()
            .unwrap()
            .insert(ticker.to_string(), batch.clone());
        Ok(())
    }

    fn read_empty_days(&self, ticker: &str) -> Result<BTreeSet<NaiveDate>, ArrowError> {
        Ok(self.empty_days.lock().unwrap().get(ticker).cloned().unwrap_or_default())
    }

    fn write_empty_days(&self, ticker: &str, days: &BTreeSet<NaiveDate>) -> Result<(), ArrowError> {
        self.empty_days
            .lock()
            .unwrap()
            .insert(ticker.to_string(), days.clone());
        Ok(())
    }
}

pub struct FileCacheStore {
    dir: PathBuf,
    format: FileFormat,
    column_types: Schema,
}

impl FileCacheStore {
    /// CSV columns named in [`crate::ohlc_schema`] are read with those types.
    pub fn new(dir: impl Into<PathBuf>, format: FileFormat) -> Self {
        Self {
            dir: dir.into(),
            format,
            column_types: crate::ohlc_schema(),
        }
    }

    /// Overrides the known column types used when reading CSV, e.g. after adding indicator columns.
    /// Columns not listed here are inferred.
    pub fn with_column_types(mut self, column_types: Schema) -> Self {
        self.column_types = column_types;
        self
    }

    fn path(&self, ticker: &str) -> Result<PathBuf, ArrowError> {
        self.file(ticker, self.format.extension())
    }

    /// Empty days are kept beside the table as one ISO date per line, whatever the table format.
    fn empty_days_path(&self, ticker: &str) -> Result<PathBuf, ArrowError> {
        self.file(ticker, "empty-days")
    }

    fn file(&self, ticker: &str, extension: &str) -> Result<PathBuf, ArrowError> {
        if ticker.is_empty() || ticker == "." || ticker == ".." || ticker.contains(['/', '\\']) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "'{ticker}' cannot be used as a file name"
            )));
        }
        Ok(self.dir.join(format!("{ticker}.{extension}")))
    }

    /// Writes to a temporary file and renames it, so an interrupted write cannot leave a partial file.
    fn write_atomically(
        &self,
        path: &Path,
        write: impl FnOnce(&Path) -> Result<(), ArrowError>,
    ) -> Result<(), ArrowError> {
        fs::create_dir_all(&self.dir)?;

        // Unique per write, so concurrent writers never share (and corrupt) one temporary file.
        static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
        let mut temp = path.to_path_buf().into_os_string();
        temp.push(format!(
            ".{}-{}.tmp",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        let temp = PathBuf::from(temp);

        let result = write(&temp).and_then(|()| fs::rename(&temp, path).map_err(ArrowError::from));
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }
}

impl CacheStore for FileCacheStore {
    fn read(&self, ticker: &str) -> Result<Option<RecordBatch>, ArrowError> {
        let path = self.path(ticker)?;
        if !path.exists() {
            return Ok(None);
        }

        match self.format {
            FileFormat::Csv => read_csv(&path, &self.column_types).map(Some),
            FileFormat::Parquet => read_parquet(&path).map(Some),
        }
    }

    fn write(&self, ticker: &str, batch: &RecordBatch) -> Result<(), ArrowError> {
        let path = self.path(ticker)?;
        self.write_atomically(&path, |temp| write_file(self.format, temp, batch))
    }

    fn read_empty_days(&self, ticker: &str) -> Result<BTreeSet<NaiveDate>, ArrowError> {
        let path = self.empty_days_path(ticker)?;
        if !path.exists() {
            return Ok(BTreeSet::new());
        }

        fs::read_to_string(&path)?
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| {
                line.parse::<NaiveDate>().map_err(|e| {
                    ArrowError::ParseError(format!("bad date '{line}' in {}: {e}", path.display()))
                })
            })
            .collect()
    }

    /// An empty set removes the file rather than leaving an empty one behind.
    fn write_empty_days(&self, ticker: &str, days: &BTreeSet<NaiveDate>) -> Result<(), ArrowError> {
        let path = self.empty_days_path(ticker)?;
        if days.is_empty() {
            return match fs::remove_file(&path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
                _ => Ok(()),
            };
        }

        let text: String = days.iter().map(|day| format!("{day}\n")).collect();
        self.write_atomically(&path, |temp| {
            let file = File::create(temp)?;
            (&file).write_all(text.as_bytes())?;
            file.sync_all()?;
            Ok(())
        })
    }
}

fn write_file(format: FileFormat, path: &Path, batch: &RecordBatch) -> Result<(), ArrowError> {
    let file = File::create(path)?;
    match format {
        FileFormat::Csv => {
            let mut writer = arrow::csv::Writer::new(file);
            writer.write(batch)?;
            writer.into_inner().sync_all()?;
        }
        FileFormat::Parquet => {
            let mut writer = ArrowWriter::try_new(file, batch.schema(), None).map_err(parquet_error)?;
            writer.write(batch).map_err(parquet_error)?;
            writer.into_inner().map_err(parquet_error)?.sync_all()?;
        }
    }
    Ok(())
}

fn read_csv(path: &Path, column_types: &Schema) -> Result<RecordBatch, ArrowError> {
    let mut file = File::open(path)?;
    let (inferred, _) = Format::default().with_header(true).infer_schema(&mut file, None)?;
    file.seek(SeekFrom::Start(0))?;

    let fields: Vec<Field> = inferred
        .fields()
        .iter()
        .map(|field| match column_types.field_with_name(field.name()) {
            Ok(known) => known.clone(),
            Err(_) => field.as_ref().clone(),
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let reader = arrow::csv::ReaderBuilder::new(schema.clone())
        .with_header(true)
        .build(file)?;
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    concat_batches(&schema, &batches)
}

fn read_parquet(path: &Path) -> Result<RecordBatch, ArrowError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?).map_err(parquet_error)?;
    let schema = builder.schema().clone();
    let reader = builder.build().map_err(parquet_error)?;
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    concat_batches(&schema, &batches)
}

fn parquet_error(error: parquet::errors::ParquetError) -> ArrowError {
    ArrowError::ExternalError(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_store_round_trips() {
        let store = InMemoryCacheStore::new();
        assert!(store.read("AAPL").unwrap().is_none());

        let batch = RecordBatch::new_empty(Arc::new(crate::ohlc_schema()));
        store.write("AAPL", &batch).unwrap();
        assert_eq!(store.read("AAPL").unwrap().unwrap().num_rows(), 0);
    }

    #[test]
    fn file_store_rejects_tickers_that_escape_the_directory() {
        let store = FileCacheStore::new(std::env::temp_dir().join("tabular-cache-unit"), FileFormat::Csv);
        let batch = RecordBatch::new_empty(Arc::new(crate::ohlc_schema()));

        for ticker in ["", ".", "..", "../evil", "a/b", "a\\b"] {
            assert!(store.read(ticker).is_err(), "read accepted {ticker:?}");
            assert!(store.write(ticker, &batch).is_err(), "write accepted {ticker:?}");
            assert!(store.read_empty_days(ticker).is_err(), "read_empty_days accepted {ticker:?}");
            assert!(
                store.write_empty_days(ticker, &BTreeSet::new()).is_err(),
                "write_empty_days accepted {ticker:?}"
            );
        }
    }

    #[test]
    fn file_store_round_trips_empty_days_and_removes_the_file_when_cleared() {
        let dir = std::env::temp_dir().join(format!("tabular-cache-unit-empty-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = FileCacheStore::new(&dir, FileFormat::Parquet);
        let days: BTreeSet<NaiveDate> = ["2026-12-25", "2026-01-01"]
            .iter()
            .map(|d| d.parse().unwrap())
            .collect();

        assert!(store.read_empty_days("AAPL").unwrap().is_empty());
        store.write_empty_days("AAPL", &days).unwrap();
        assert_eq!(store.read_empty_days("AAPL").unwrap(), days);
        assert_eq!(fs::read_to_string(dir.join("AAPL.empty-days")).unwrap(), "2026-01-01\n2026-12-25\n");

        store.write_empty_days("AAPL", &BTreeSet::new()).unwrap();
        assert!(!dir.join("AAPL.empty-days").exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
