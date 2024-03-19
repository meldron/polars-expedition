mod utils;

use std::{collections::HashMap, io::Cursor};

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Stats {
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

#[wasm_bindgen]
pub fn describe(csv_data: &str) -> Result<String, String> {
    let cursor = Cursor::new(csv_data.as_bytes());
    let df_result = CsvReader::new(cursor).infer_schema(None).finish();

    let df = match df_result {
        Ok(df) => df,
        Err(err) => return Err(err.to_string()),
    };

    let mut map: HashMap<String, Stats> = HashMap::new();

    for series in df.get_columns() {
        let name = series.name();

        let len = series.len();
        let null_values = series.null_count();
        let unique_values = series.n_unique().ok();

        let min: Option<f64> = series.min().unwrap_or(None);
        let mean = series.mean();
        let median = series.median();
        let max: Option<f64> = series.max().unwrap_or(None);

        let stats = Stats {
            len,
            null_values,
            unique_values,
            min,
            median,
            mean,
            max,
        };

        map.insert(name.to_owned(), stats.clone());
    }

    let result = to_string(&map).map_err(|e| e.to_string())?;

    Ok(result)
}
