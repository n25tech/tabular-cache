use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::csv::reader::Format;
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Persists one table per ticker. Implement this to add a new storage format or backend.
pub trait CacheStore: Send + Sync {
    fn read(&self, ticker: &str) -> Result<Option<RecordBatch>, ArrowError>;
    fn write(&self, ticker: &str, batch: &RecordBatch) -> Result<(), ArrowError>;
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
        if ticker.is_empty() || ticker == "." || ticker == ".." || ticker.contains(['/', '\\']) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "'{ticker}' cannot be used as a file name"
            )));
        }
        Ok(self.dir.join(format!("{ticker}.{}", self.format.extension())))
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

    /// Writes to a temporary file and renames it, so an interrupted write cannot leave a partial file.
    fn write(&self, ticker: &str, batch: &RecordBatch) -> Result<(), ArrowError> {
        let path = self.path(ticker)?;
        fs::create_dir_all(&self.dir)?;

        // Unique per write, so concurrent writers never share (and corrupt) one temporary file.
        static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
        let mut temp = path.clone().into_os_string();
        temp.push(format!(
            ".{}-{}.tmp",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        let temp = PathBuf::from(temp);

        let result = write_file(self.format, &temp, batch)
            .and_then(|()| fs::rename(&temp, &path).map_err(ArrowError::from));
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
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
        }
    }
}
