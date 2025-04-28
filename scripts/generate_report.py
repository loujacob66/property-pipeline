import sqlite3

def generate_report():
    db_filename = 'data/listings.db'
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()

    c.execute('''
        SELECT address, city, state, zip, price, beds, baths, sqft, estimated_rent, rent_yield, mls_number, mls_type, tax_info
        FROM listings
        ORDER BY imported_at DESC
    ''')

    rows = c.fetchall()

    if not rows:
        print("No listings found.")
        return

    # Define headers
    headers = [
        "Address", "City", "State", "ZIP", "Price", "Beds", "Baths", "Sqft",
        "Est. Rent", "Yield", "MLS#", "MLS Type", "Tax Info"
    ]

    # Calculate column widths
    col_widths = [len(header) for header in headers]
    for row in rows:
        for idx, value in enumerate(row):
            if value is None:
                value = ""
            elif isinstance(value, float):
                value = f"{value:,.2f}" if idx not in [9] else f"{value:.2%}"  # Special formatting for rent_yield
            elif isinstance(value, int):
                value = f"{value:,}"
            else:
                value = str(value)
            col_widths[idx] = max(col_widths[idx], len(value))

    # Print header
    header_row = " | ".join(header.ljust(col_widths[i]) for i, header in enumerate(headers))
    print(header_row)
    print("-" * len(header_row))

    # Print data rows
    for row in rows:
        formatted_row = []
        for idx, value in enumerate(row):
            if value is None:
                value = ""
            elif isinstance(value, float):
                value = f"{value:,.2f}" if idx not in [9] else f"{value:.2%}"  # Special formatting for rent_yield
            elif isinstance(value, int):
                value = f"{value:,}"
            else:
                value = str(value)
            formatted_row.append(value.ljust(col_widths[idx]))
        print(" | ".join(formatted_row))

    conn.close()

if __name__ == "__main__":
    generate_report()
