import sqlite3

def generate_report():
    db_filename = 'data/listings.db'
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()

    c.execute('''
        SELECT address, city, price, beds, baths, sqft, price_per_sqft, 
               estimated_rent, rent_yield, mls_number, mls_type, tax_information,
               days_on_compass, last_updated, favorite
        FROM listings
        ORDER BY imported_at DESC
    ''')

    rows = c.fetchall()

    if not rows:
        print("No listings found.")
        return

    # Define headers
    headers = [
        "Address", "City", "Price", "Beds", "Baths", "Sqft",
        "Price/Sqft", "Est. Rent", "Yield", "MLS#", "MLS Type", "Tax Info",
        "Days on Compass", "Last Updated", "Favorite"
    ]

    # Calculate column widths
    col_widths = [len(header) for header in headers]
    for row in rows:
        for idx, value in enumerate(row):
            if value is None:
                value = ""
            elif isinstance(value, float):
                value = f"{value:,.2f}" if idx not in [8] else f"{value:.2%}"  # Special formatting for rent_yield
            elif isinstance(value, int):
                if idx == 14:  # Favorite field
                    value = "★" if value else ""
                else:
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
                value = f"{value:,.2f}" if idx not in [8] else f"{value:.2%}"  # Special formatting for rent_yield
            elif isinstance(value, int):
                if idx == 14:  # Favorite field
                    value = "★" if value else ""
                else:
                    value = f"{value:,}"
            else:
                value = str(value)
            formatted_row.append(value.ljust(col_widths[idx]))
        print(" | ".join(formatted_row))

    conn.close()

if __name__ == "__main__":
    generate_report()
