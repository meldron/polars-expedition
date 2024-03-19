import polars as pl

from extend_polars import lazy_parallel_decrypt, lazy_parallel_encrypt, parallel_decrypt, parallel_encrypt

PASSWORD = "test"


def main():
    df = pl.read_parquet("2019-Nov-purchases-filtered.parquet")

    df_encrypted_product_id = parallel_encrypt(df, "product_id", PASSWORD)

    print(df_encrypted_product_id)

    decrypted_product_id = parallel_decrypt(df_encrypted_product_id, "product_id_encrypted", "product_id_nonce", PASSWORD)

    print(decrypted_product_id)


if __name__ == "__main__":
    main()