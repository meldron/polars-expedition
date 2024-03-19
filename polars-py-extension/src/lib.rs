use argon2::Argon2;
use hex::{decode, encode};
use polars::prelude::*;
use polars_core::utils::accumulate_dataframes_vertical;
use polars_lazy::frame::IntoLazy;
use polars_lazy::prelude::LazyFrame;
use pyo3::prelude::*;
use pyo3_polars::error::PyPolarsErr;
use pyo3_polars::{PyDataFrame, PyLazyFrame};
use rand::prelude::*;
use rayon::prelude::*;
use xsalsa20poly1305::aead::generic_array::GenericArray;
use xsalsa20poly1305::aead::Aead;
use xsalsa20poly1305::{KeyInit, XSalsa20Poly1305};

// Code parts from https://github.com/pola-rs/pyo3-polars/blob/main/example/extend_polars_python_dispatch/
// License MIT Copyright (c) 2020 Ritchie Vink

/// Create `n` splits so that we can slice a polars data structure
/// and process the chunks in parallel
fn split_offsets(len: usize, n: usize) -> Vec<(usize, usize)> {
    if n == 1 {
        vec![(0, len)]
    } else {
        let chunk_size = len / n;

        (0..n)
            .map(|partition| {
                let offset = partition * chunk_size;
                let len = if partition == (n - 1) {
                    len - offset
                } else {
                    chunk_size
                };
                (partition * chunk_size, len)
            })
            .collect()
    }
}

const SECRET_LENGTH: usize = 32;
const NONCE_LENGTH: usize = 24;

fn create_nonce() -> [u8; NONCE_LENGTH] {
    let mut rng = thread_rng();
    let mut nonce_raw = [0u8; NONCE_LENGTH];
    rng.fill(&mut nonce_raw);

    return nonce_raw;
}

fn generate_key_from_password(password: &str) -> Vec<u8> {
    let salt = b"Q88pmcJzbz8hvnd0ISZ2eF0V3xwcBTHCF4Hj8tsOcX";

    let mut output_key_material = [0u8; SECRET_LENGTH];
    Argon2::default()
        .hash_password_into(password.as_bytes(), salt, &mut output_key_material)
        .unwrap();

    return output_key_material.to_vec();
}

fn decrypt_with_nonces(nonces: &Series, encrypted: &Series, key: &[u8]) -> PolarsResult<DataFrame> {
    let mut decrypted_messages: Vec<Option<String>> = Vec::new();
    let mut error_messages: Vec<Option<String>> = Vec::new();

    let nonces = nonces.cast(&DataType::String)?;
    let encrypted = encrypted.cast(&DataType::String)?;

    // Assuming nonces and encrypted are already cast to string dtype and are lists
    let nonces = nonces.str()?;
    let encrypted = encrypted.str()?;

    for (nonce_hex, cipher_message_hex) in nonces.into_iter().zip(encrypted.into_iter()) {
        match (nonce_hex, cipher_message_hex) {
            (Some(nonce_hex), Some(cipher_message_hex)) => {
                // Decode the nonce and cipher text; push None if decoding fails
                if let (Ok(nonce), Ok(cipher_message)) =
                    (decode(nonce_hex), decode(cipher_message_hex))
                {
                    let key_input = GenericArray::from_slice(key);
                    let salsa = XSalsa20Poly1305::new(key_input);

                    let nonce_generic = GenericArray::clone_from_slice(&nonce);
                    // Attempt to decrypt and push None if decryption fails
                    match salsa.decrypt(&nonce_generic, cipher_message.as_slice()) {
                        Ok(decrypted_message) => {
                            // Convert decrypted bytes to string; push None if conversion fails
                            match String::from_utf8(decrypted_message) {
                                Ok(message) => {
                                    decrypted_messages.push(Some(message));
                                    error_messages.push(None);
                                }
                                Err(_) => {
                                    decrypted_messages.push(None);
                                    error_messages.push(Some(
                                        "Failed to convert decrypted bytes to string".into(),
                                    ));
                                }
                            }
                        }
                        Err(_) => {
                            decrypted_messages.push(None);
                            error_messages.push(Some("Decryption failed".into()));
                        }
                    }
                } else {
                    decrypted_messages.push(None);
                    error_messages.push(Some("Decoding nonce or cipher text failed".into()));
                }
            }
            _ => {
                decrypted_messages.push(None);
                error_messages.push(Some("Nonce or cipher text is missing".into()));
            }
        }
    }

    let decrypted_series_name = format!("{}_decrypted", encrypted.name());
    let decrypted_series = Series::new(&decrypted_series_name, &decrypted_messages);

    let error_series_name = format!("{}_decryption_errors", encrypted.name());
    let error_series = Series::new(&error_series_name, &error_messages);

    DataFrame::new(vec![decrypted_series, error_series])
}

fn encrypt_series(series: &Series, key: &[u8]) -> PolarsResult<DataFrame> {
    let crypt_key = format!("{}_encrypted", series.name());
    let nonce_key = format!("{}_nonce", series.name());

    let mut nonces: Vec<Option<String>> = Vec::new();
    let mut encrypted: Vec<Option<String>> = Vec::new();

    let series = series.cast(&DataType::String)?;

    if let Ok(ca) = series.str() {
        for value in ca.into_iter() {
            match value {
                Some(v) => {
                    let nonce = create_nonce();
                    let nonce_hex = encode(nonce);

                    let key_input = GenericArray::from_slice(key);
                    let salsa = XSalsa20Poly1305::new(&*key_input);

                    let nonce_generic = GenericArray::clone_from_slice(&nonce);

                    let cipher_message = salsa.encrypt(&nonce_generic, v.as_bytes()).unwrap();
                    let cipher_message_hex = encode(cipher_message);

                    nonces.push(Some(nonce_hex));
                    encrypted.push(Some(cipher_message_hex));
                }
                None => {
                    nonces.push(None);
                    encrypted.push(None);
                }
            }
        }
    }

    df!(
        crypt_key.as_str() => encrypted,
        nonce_key.as_str() => nonces
    )
}

fn parallel_encrypt_data_frame(
    df: DataFrame,
    col: &str,
    password: &str,
) -> PolarsResult<DataFrame> {
    let offsets = split_offsets(df.height(), rayon::current_num_threads());

    let key = generate_key_from_password(password);

    let dfs = offsets
        .par_iter()
        .map(|(offset, len)| {
            let sub_df = df.slice(*offset as i64, *len);
            let series = sub_df.column(col)?;

            let out = encrypt_series(series, &key)?;

            Ok(out)
        })
        .collect::<PolarsResult<Vec<_>>>()?;
    accumulate_dataframes_vertical(dfs)
}

fn parallel_decrypt_data_frame(
    df: DataFrame,
    encrypted_col: &str,
    nonces_col: &str,
    password: &str,
) -> PolarsResult<DataFrame> {
    let offsets = split_offsets(df.height(), rayon::current_num_threads());

    let key = generate_key_from_password(password);

    let dfs = offsets
        .par_iter()
        .map(|(offset, len)| {
            let sub_df = df.slice(*offset as i64, *len);

            let nonces_series = sub_df.column(nonces_col)?;
            let encrypted_series = sub_df.column(encrypted_col)?;

            let out = decrypt_with_nonces(nonces_series, encrypted_series, &key)?;

            Ok(out)
        })
        .collect::<PolarsResult<Vec<_>>>()?;

    accumulate_dataframes_vertical(dfs)
}

#[pyfunction]
fn parallel_encrypt(pydf: PyDataFrame, col: &str, key: &str) -> PyResult<PyDataFrame> {
    let df: DataFrame = pydf.into();
    let df = parallel_encrypt_data_frame(df, col, key).map_err(PyPolarsErr::from)?;
    Ok(PyDataFrame(df))
}

#[pyfunction]
fn lazy_parallel_encrypt(pydf: PyLazyFrame, col: &str, key: &str) -> PyResult<PyLazyFrame> {
    let df: LazyFrame = pydf.into();
    let df =
        parallel_encrypt_data_frame(df.collect().unwrap(), col, key).map_err(PyPolarsErr::from)?;
    Ok(PyLazyFrame(df.lazy()))
}

#[pyfunction]
fn parallel_decrypt(
    pydf: PyDataFrame,
    encrypted_col: &str,
    nonces_col: &str,
    key: &str,
) -> PyResult<PyDataFrame> {
    let df: DataFrame = pydf.into();
    let df = parallel_decrypt_data_frame(df, encrypted_col, nonces_col, key)
        .map_err(PyPolarsErr::from)?;
    Ok(PyDataFrame(df))
}

#[pyfunction]
fn lazy_parallel_decrypt(
    pydf: PyLazyFrame,
    encrypted_col: &str,
    nonces_col: &str,
    key: &str,
) -> PyResult<PyLazyFrame> {
    let df: LazyFrame = pydf.into();
    let df = parallel_decrypt_data_frame(df.collect().unwrap(), encrypted_col, nonces_col, key)
        .map_err(PyPolarsErr::from)?;
    Ok(PyLazyFrame(df.lazy()))
}

#[pymodule]
fn extend_polars(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parallel_encrypt, m)?)?;
    m.add_function(wrap_pyfunction!(lazy_parallel_encrypt, m)?)?;

    m.add_function(wrap_pyfunction!(parallel_decrypt, m)?)?;
    m.add_function(wrap_pyfunction!(lazy_parallel_decrypt, m)?)?;

    Ok(())
}
