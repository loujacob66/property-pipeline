import csv

def load_zori_data(filepath):
    zip_to_rent = {}
    with open(filepath, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        latest_month = reader.fieldnames[-1]
        for row in reader:
            zip_code = row['RegionName'].zfill(5)
            try:
                rent = float(row[latest_month]) if row[latest_month] else None
            except ValueError:
                rent = None
            zip_to_rent[zip_code] = rent
    return zip_to_rent
