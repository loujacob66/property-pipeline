import sqlite3
import pandas as pd
import numpy as np

def colorize_rent_yield(y):
    if y is None or pd.isna(y):
        return "\033[90m{:<6}\033[0m".format("n/a")
    elif y >= 0.06:
        return "\033[92m{:<6.4f}\033[0m".format(y)  # green
    elif y >= 0.05:
        return "\033[92m{:<6.4f}\033[0m".format(y)  # green
    elif y >= 0.04:
        return "\033[93m{:<6.4f}\033[0m".format(y)  # yellow
    elif y >= 0.03:
        return "\033[91m{:<6.4f}\033[0m".format(y)  # red/orange
    else:
        return "\033[91m{:<6.4f}\033[0m".format(y)  # red

def generate_rent_yield_report(db_path="data/listings.db", output_csv="rent_yield_report.csv"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM listings;", conn)
    conn.close()

    df["estimated_rent"] = df["estimated_rent"].round(0).astype("Int64")
    df = df.sort_values(by="rent_yield", ascending=False)

    print("📊 Rent Yield Report:")
    for _, row in df.iterrows():
        address = row['address']
        price = f"${row['price']:<7}"
        rent = f"${row['estimated_rent']:<5}"
        city = f"{row['city']:<15}"
        zip_code = row['zip']
        yield_colored = colorize_rent_yield(row['rent_yield'])
        print(f"{address:<52} {city:<15} {zip_code:<6} {price} Rent: {rent} Yield: {yield_colored}")

    df[["address", "city", "zip", "price", "estimated_rent", "rent_yield"]].to_csv(output_csv, index=False)
    print(f"\n✅ Report also saved to: {output_csv}")

if __name__ == "__main__":
    generate_rent_yield_report()
