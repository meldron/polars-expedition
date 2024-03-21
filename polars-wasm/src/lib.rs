mod utils;

use std::{collections::HashMap, io::Cursor};

use chrono::{format::ParseErrorKind, NaiveDate, NaiveDateTime, ParseResult};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use serde_wasm_bindgen::from_value;
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Stats {
    pub len: usize,
    pub null_values: usize,
    pub unique_values: Option<usize>,
    pub min: Option<f64>,
    pub median: Option<f64>,
    pub mean: Option<f64>,
    pub max: Option<f64>,
}

#[wasm_bindgen]
pub fn count_csv_lines(csv_data: &str) -> Result<usize, String> {
    let cursor = Cursor::new(csv_data.as_bytes());
    let df_result = CsvReader::new(cursor).infer_schema(None).finish();

    match df_result {
        Ok(df) => Ok(df.height()),
        Err(err) => Err(err.to_string()),
    }
}

pub fn parse_date(date_str: &str, format: &str) -> ParseResult<NaiveDateTime> {
    match NaiveDateTime::parse_from_str(date_str, format) {
        Ok(datetime) => Ok(datetime),
        Err(e) => match e.kind() {
            ParseErrorKind::NotEnough => {
                // If the error is because of missing time component, parse as NaiveDate and set time to midnight
                let date = NaiveDate::parse_from_str(date_str, format)?;
                Ok(date.and_hms(0, 0, 0))
            }
            _ => Err(e), // Propagate other errors
        },
    }
}

pub fn parse_as_date_series(series: &Series, date_format: &str) -> Result<Series, String> {
    Ok(series
        .str()
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|opt_str| {
            opt_str
                .and_then(|str_val| parse_date(str_val, date_format).ok())
                .map(|dt| dt.and_utc().timestamp_millis())
        })
        .collect::<Int64Chunked>()
        .into_series()
        .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
        .map_err(|e| e.to_string())?)
}

pub fn series_stats(series: &Series) -> Stats {
    let len = series.len();
    let null_values = series.null_count();
    let unique_values = series.n_unique().ok();

    let min: Option<f64> = series.min().unwrap_or(None);
    let mean = series.mean();
    let median = series.median();
    let max: Option<f64> = series.max().unwrap_or(None);

    Stats {
        len,
        null_values,
        unique_values,
        min,
        median,
        mean,
        max,
    }
}

#[wasm_bindgen]
pub fn describe(csv_data: &str, date_cols_js: JsValue) -> Result<String, String> {
    let cursor = Cursor::new(csv_data.as_bytes());
    let df_result = CsvReader::new(cursor).infer_schema(None).finish();

    let df = match df_result {
        Ok(df) => df,
        Err(err) => return Err(err.to_string()),
    };

    let date_cols: HashMap<String, String> = from_value(date_cols_js).map_err(|e| e.to_string())?;

    let mut map: HashMap<String, Stats> = HashMap::new();

    for series in df.get_columns() {
        let name = series.name().to_owned();

        let mut date_series: Option<Series> = None;

        if let Some(format) = date_cols.get(series.name()) {
            date_series = Some(parse_as_date_series(series, format)?);
        }

        let series = match date_series.as_ref() {
            Some(s) => s,
            None => series,
        };

        let stats = series_stats(series);

        map.insert(name, stats);
    }

    let result = to_string(&map).map_err(|e| e.to_string())?;

    Ok(result)
}
