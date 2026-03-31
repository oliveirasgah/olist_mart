import duckdb
import argparse


def path_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", nargs="?", default=None)
    args = parser.parse_args()

    return args.csv_path if args.csv_path else "raw_data"


path = path_parse()

con = duckdb.connect("data/dev.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS raw")

files = {
    "orders": f"{path}/olist_orders_dataset.csv",
    "order_items": f"{path}/olist_order_items_dataset.csv",
    "products": f"{path}/olist_products_dataset.csv",
    "customers": f"{path}/olist_customers_dataset.csv",
    "payments": f"{path}/olist_order_payments_dataset.csv",
    "sellers": f"{path}/olist_sellers_dataset.csv",
    "category_translation": f"{path}/product_category_name_translation.csv",
}

for table, file in files.items():
    con.execute(
        f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM read_csv_auto('{file}')"
    )

    count = con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
    print(f"raw.{table}: {count} rows")

con.close()
