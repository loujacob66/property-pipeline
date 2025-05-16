import sqlite3
import csv
import gzip
import logging
from datetime import datetime

# --- Configuration ---
DB_FILE = 'data/neighborhood_analysis.db' # Updated path
SCHEMA_FILE = 'data/neighborhood_analysis.sql' # Updated path
NEIGHBORHOOD_DATA_FILE = 'data/redfin_neighborhood_market_tracker.tsv000.gz'
REGIONAL_DATA_FILE = 'data/redfin_MoM_Full Data_data.csv'

# --- Target Geographic Areas for Filtering ---
TARGET_CITIES_UPPER = {
    "DENVER", "WHEAT RIDGE", "EDGEWATER", "ARVADA", 
    "LAKEWOOD", "GOLDEN", "WESTMINSTER"
}
# Keywords to identify specific neighborhoods/areas of interest from the REGION column
# This handles variations like "Sloan Lake", "Sloanes Lake", "Sloan's Lake"
TARGET_NEIGHBORHOOD_KEYWORDS_UPPER = {
    "SLOAN LAKE", "SLOANES LAKE", "SLOAN'S LAKE"
}

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_database(db_file, schema_file):
    """Creates the database and tables from the schema file."""
    logging.info(f"Creating database {db_file} using schema {schema_file}...")
    try:
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.executescript(schema_sql)
        conn.commit()
        conn.close()
        logging.info("Database and tables created successfully.")
    except sqlite3.Error as e:
        logging.error(f"Database error during creation: {e}")
        raise
    except FileNotFoundError:
        logging.error(f"Schema file {schema_file} not found.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during database creation: {e}")
        raise

def parse_datetime_value(datetime_str, input_format_type=None, output_date_only=True):
    """
    Parses a date/datetime string and returns it in the specified format.
    - datetime_str: The string to parse.
    - input_format_type: A hint for specific non-standard input formats (e.g., 'm/d/Y' for regional, 'iso_with_space_Z' for last_updated).
    - output_date_only: If True, returns 'YYYY-MM-DD'. If False, returns 'YYYY-MM-DD HH:MM:SS'.
    Returns None if parsing fails or input is NA/empty.
    """
    if not datetime_str or str(datetime_str).strip().lower() == 'na':
        return None

    dt_obj = None
    original_datetime_str = datetime_str # for logging

    try:
        if input_format_type == 'm/d/Y': # For regional data CSV: "2/1/2025"
            dt_obj = datetime.strptime(datetime_str, '%m/%d/%Y')
        elif input_format_type == 'iso_with_space_Z': # For last_updated: "2025-04-18 14:40:47.550 Z"
            # datetime.fromisoformat doesn't handle space before Z, strptime needs care with Z
            # Simplest for this specific format:
            if ' Z' in datetime_str:
                 dt_obj = datetime.strptime(datetime_str.split(' Z')[0], '%Y-%m-%d %H:%M:%S.%f')
            else: # Fallback if Z is missing but format is otherwise similar
                 dt_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
        elif 'T' in datetime_str and 'Z' in datetime_str: # Standard ISO 8601 like '2025-04-18T14:40:47.550Z'
            dt_obj = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        else: # Default attempt for 'YYYY-MM-DD'
            dt_obj = datetime.strptime(datetime_str, '%Y-%m-%d')

    except ValueError as e:
        logging.warning(f"Could not parse datetime string: '{original_datetime_str}' with hint '{input_format_type}'. Error: {e}")
        return None

    if dt_obj:
        if output_date_only:
            return dt_obj.strftime('%Y-%m-%d')
        else:
            return dt_obj.strftime('%Y-%m-%d %H:%M:%S') # SQLite friendly full datetime
    return None

def to_float(value_str):
    """Converts a string to a float. Returns None if 'NA' or conversion fails."""
    if isinstance(value_str, (int, float)): # Already a number
        return float(value_str)
    if not value_str or value_str.strip().lower() == 'na':
        return None
    try:
        return float(value_str)
    except ValueError:
        logging.warning(f"Could not convert '{value_str}' to float.")
        return None

def to_int(value_str):
    """Converts a string to an int. Returns None if 'NA' or conversion fails."""
    if isinstance(value_str, (int, float)): # Already a number (float will be truncated)
        return int(value_str)
    if not value_str or value_str.strip().lower() == 'na':
        return None
    try:
        return int(float(value_str)) # float() handles "123.0" then int()
    except ValueError:
        logging.warning(f"Could not convert '{value_str}' to int.")
        return None

def to_bool(value_str):
    """Converts a string to a boolean. Returns None if 'NA', False otherwise or if conversion fails."""
    if value_str is None or str(value_str).strip().lower() == 'na':
        return None
    return str(value_str).strip().lower() == 'true'


def process_neighborhood_data(db_file, tsv_gz_file):
    """Processes the gzipped TSV neighborhood data file and inserts into the database."""
    logging.info(f"Starting processing of neighborhood data from {tsv_gz_file}...")
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        with gzip.open(tsv_gz_file, 'rt', encoding='utf-8') as gz_file:
            reader = csv.reader(gz_file, delimiter='\t') # Tab-separated
            
            header = [h.strip().lower().replace('"', '') for h in next(reader)] # Clean header
            logging.info(f"TSV Header: {header}")

            insert_sql = f"""
            INSERT OR IGNORE INTO neighborhood_data (
                period_begin, period_end, period_duration, region_type, region_type_id,
                table_id, is_seasonally_adjusted, neighborhood_name, city, state_code,
                property_type, property_type_id, median_sale_price, median_sale_price_mom,
                median_sale_price_yoy, median_list_price, median_list_price_mom,
                median_list_price_yoy, median_ppsf, median_ppsf_mom, median_ppsf_yoy,
                median_list_ppsf, median_list_ppsf_mom, median_list_ppsf_yoy,
                homes_sold, homes_sold_mom, homes_sold_yoy, pending_sales,
                pending_sales_mom, pending_sales_yoy, new_listings, new_listings_mom,
                new_listings_yoy, inventory, inventory_mom, inventory_yoy,
                months_of_supply, months_of_supply_mom, months_of_supply_yoy,
                median_dom, median_dom_mom, median_dom_yoy, avg_sale_to_list,
                avg_sale_to_list_mom, avg_sale_to_list_yoy, sold_above_list,
                sold_above_list_mom, sold_above_list_yoy, price_drops, price_drops_mom,
                price_drops_yoy, off_market_in_two_weeks, off_market_in_two_weeks_mom,
                off_market_in_two_weeks_yoy, parent_metro_region,
                parent_metro_region_metro_code, last_updated
            ) VALUES ({', '.join(['?'] * 57)})
            """
            processed_rows = 0
            inserted_rows = 0
            skipped_rows_header_mismatch = 0
            skipped_rows_filter = 0 # Counter for rows skipped by geo filter

            col_map = {name: idx for idx, name in enumerate(header)}
            
            for i, row_raw in enumerate(reader):
                if not row_raw or len(row_raw) < 50 : # Basic check for malformed/short rows (flexible check)
                    logging.warning(f"Skipping malformed row {i+2}: {row_raw}")
                    skipped_rows_header_mismatch +=1
                    continue

                row = [val.strip().replace('"', '') for val in row_raw]
                
                try:
                    data_tuple = (
                        parse_datetime_value(row[col_map['period_begin']]),
                        parse_datetime_value(row[col_map['period_end']]),
                        to_int(row[col_map['period_duration']]),
                        row[col_map['region_type']],
                        to_int(row[col_map['region_type_id']]),
                        to_int(row[col_map['table_id']]),
                        to_bool(row[col_map.get('is_seasonally_adjusted')]),
                        row[col_map['region']], 
                        row[col_map['city']],
                        row[col_map['state_code']],
                        row[col_map['property_type']],
                        to_int(row[col_map['property_type_id']]),
                        to_float(row[col_map['median_sale_price']]),
                        to_float(row[col_map.get('median_sale_price_mom')]),
                        to_float(row[col_map.get('median_sale_price_yoy')]),
                        to_float(row[col_map['median_list_price']]),
                        to_float(row[col_map.get('median_list_price_mom')]),
                        to_float(row[col_map.get('median_list_price_yoy')]),
                        to_float(row[col_map['median_ppsf']]),
                        to_float(row[col_map.get('median_ppsf_mom')]),
                        to_float(row[col_map.get('median_ppsf_yoy')]),
                        to_float(row[col_map['median_list_ppsf']]),
                        to_float(row[col_map.get('median_list_ppsf_mom')]),
                        to_float(row[col_map.get('median_list_ppsf_yoy')]),
                        to_int(row[col_map['homes_sold']]),
                        to_float(row[col_map.get('homes_sold_mom')]),
                        to_float(row[col_map.get('homes_sold_yoy')]),
                        to_int(row[col_map.get('pending_sales')]),
                        to_float(row[col_map.get('pending_sales_mom')]),
                        to_float(row[col_map.get('pending_sales_yoy')]),
                        to_int(row[col_map['new_listings']]),
                        to_float(row[col_map.get('new_listings_mom')]),
                        to_float(row[col_map.get('new_listings_yoy')]),
                        to_int(row[col_map['inventory']]),
                        to_float(row[col_map.get('inventory_mom')]),
                        to_float(row[col_map.get('inventory_yoy')]),
                        to_float(row[col_map.get('months_of_supply')]),
                        to_float(row[col_map.get('months_of_supply_mom')]),
                        to_float(row[col_map.get('months_of_supply_yoy')]),
                        to_int(row[col_map['median_dom']]),
                        to_float(row[col_map.get('median_dom_mom')]),
                        to_float(row[col_map.get('median_dom_yoy')]),
                        to_float(row[col_map['avg_sale_to_list']]),
                        to_float(row[col_map.get('avg_sale_to_list_mom')]),
                        to_float(row[col_map.get('avg_sale_to_list_yoy')]),
                        to_float(row[col_map['sold_above_list']]),
                        to_float(row[col_map.get('sold_above_list_mom')]),
                        to_float(row[col_map.get('sold_above_list_yoy')]),
                        to_float(row[col_map.get('price_drops')]),
                        to_float(row[col_map.get('price_drops_mom')]),
                        to_float(row[col_map.get('price_drops_yoy')]),
                        to_float(row[col_map['off_market_in_two_weeks']]),
                        to_float(row[col_map.get('off_market_in_two_weeks_mom')]),
                        to_float(row[col_map.get('off_market_in_two_weeks_yoy')]),
                        row[col_map['parent_metro_region']],
                        row[col_map['parent_metro_region_metro_code']],
                        parse_datetime_value(row[col_map['last_updated']], input_format_type='iso_with_space_Z', output_date_only=False)
                    )
                    
                    # --- Geographic Filtering Logic ---
                    current_city_val = data_tuple[8] # city column value from tuple
                    current_region_val = data_tuple[7] # neighborhood_name / REGION column value from tuple
                    current_state_code_val = data_tuple[9] # state_code column value from tuple

                    city_match = False
                    if current_city_val and isinstance(current_city_val, str) and \
                       current_state_code_val and isinstance(current_state_code_val, str):
                        # Ensure city is in our target list AND state is Colorado
                        if current_city_val.upper() in TARGET_CITIES_UPPER and current_state_code_val.upper() == "CO":
                            city_match = True
                    
                    neighborhood_match = False
                    if current_region_val and isinstance(current_region_val, str):
                        # For neighborhood keywords, it's often specific enough, but we can be extra sure
                        # if the city is also Denver or another CO target city when a keyword matches.
                        # This logic assumes neighborhood keywords are primarily for Denver.
                        is_co_context = city_match or (current_city_val and isinstance(current_city_val, str) and current_city_val.upper() == "DENVER" and current_state_code_val and current_state_code_val.upper() == "CO")
                        if is_co_context: # Only check neighborhood keywords if we are in a CO context
                            for keyword in TARGET_NEIGHBORHOOD_KEYWORDS_UPPER:
                                if keyword in current_region_val.upper():
                                    neighborhood_match = True
                                    break # Found a keyword match

                    if not (city_match or neighborhood_match):
                        skipped_rows_filter += 1
                        if processed_rows < 5 or skipped_rows_filter % 10000 == 0: # Log first few skips and then periodically
                             logging.debug(f"Skipping row due to geo filter: City '{current_city_val}', Region '{current_region_val}'")
                        continue # Skip this row
                    # --- End Geographic Filtering Logic ---
                    
                    cursor.execute(insert_sql, data_tuple)
                    inserted_rows += cursor.rowcount 
                    processed_rows += 1

                    if processed_rows % 10000 == 0:
                        conn.commit()
                        logging.info(f"Processed {processed_rows} rows, inserted {inserted_rows}...")

                except IndexError as e:
                    logging.warning(f"Skipping row {i+2} due to IndexError (likely missing columns): {row}. Error: {e}")
                    skipped_rows_header_mismatch +=1
                except KeyError as e:
                    logging.error(f"Missing expected column in header map: {e}. Row {i+2}: {row_raw}")
                    skipped_rows_header_mismatch += 1
                except Exception as e:
                    logging.error(f"Error processing row {i+2}: {row}. Error: {e}")

            conn.commit()
            logging.info(f"Finished processing neighborhood data. Total processed: {processed_rows}, Total inserted: {inserted_rows}, Skipped (header/format): {skipped_rows_header_mismatch}, Skipped (geo filter): {skipped_rows_filter}")

        conn.close()
    except FileNotFoundError:
        logging.error(f"Neighborhood data file {tsv_gz_file} not found.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during neighborhood data processing: {e}")

def process_regional_data(db_file, csv_file):
    """Processes the CSV regional data file and inserts into the database."""
    logging.info(f"Starting processing of regional data from {csv_file}...")
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Try common encodings for CSV files
        encodings_to_try = ['utf-8-sig', 'utf-16', 'latin-1', 'utf-8'] # Try utf-16 after utf-8-sig
        file_opened_successfully = False
        used_encoding = None
        reader = None # Initialize reader to None

        for encoding in encodings_to_try:
            try:
                with open(csv_file, 'r', encoding=encoding) as f:
                    # Read the header to confirm successful decoding with this encoding
                    # We need to then re-open or wrap it in a way csv.reader can use it
                    # Simplest is to read lines and pass to csv.reader
                    header_line = f.readline()
                    if not header_line: # Empty file
                        logging.info(f"Regional data file {csv_file} is empty.")
                        conn.close()
                        return
                    
                    # Check for non-printable characters in header as a basic validation
                    if not all(c.isprintable() or c.isspace() for c in header_line):
                        logging.warning(f"Encoding {encoding} resulted in non-printable characters in header for {csv_file}. Trying next encoding.")
                        continue # Try next encoding

                    # If header is fine, reset and read all lines for csv.reader
                    f.seek(0)
                    lines = f.readlines()
                    reader = csv.reader(lines, delimiter='\t')
                    used_encoding = encoding
                    logging.info(f"Successfully opened regional data file {csv_file} with encoding: {used_encoding}")
                    file_opened_successfully = True # Mark as successfully opened
                    break # Successfully opened and read header
            except (UnicodeDecodeError, AttributeError) as e: # AttributeError for f.readline() if open fails weirdly
                logging.warning(f"Failed to open {csv_file} with encoding {encoding}: {e}")
                continue
        
        if not file_opened_successfully or not reader:
            logging.error(f"Could not open or decode regional data file {csv_file} with any attempted encodings.")
            conn.close()
            return

        header = [h.strip() for h in next(reader)]
        logging.info(f"Regional CSV Header: {header}")

        try:
            date_idx = header.index("Date")
            region_name_idx = header.index("Region Name")
            case_shiller_idx = header.index("Case Shiller Index MoM")
            index_sa_idx = header.index("INDEX_SA_ROLL3MTH_MOM")
        except ValueError as e:
            logging.error(f"Missing expected column in regional data CSV header: {e}. Header was: {header}")
            conn.close()
            return

        insert_sql = """
        INSERT OR IGNORE INTO regional_market_trends (
            date, region_name, case_shiller_index_mom, index_sa_roll3mth_mom
        ) VALUES (?, ?, ?, ?)
        """

        processed_rows = 0
        inserted_rows = 0

        for i, row in enumerate(reader):
            if not row or len(row) < max(date_idx, region_name_idx, case_shiller_idx, index_sa_idx) + 1:
                logging.warning(f"Skipping malformed regional data row {i+2}: {row}")
                continue
            
            try:
                date_val = parse_datetime_value(row[date_idx], input_format_type='m/d/Y')
                region_name_val = row[region_name_idx]
                case_shiller_val = to_float(row[case_shiller_idx])
                index_sa_val = to_float(row[index_sa_idx])

                if date_val is None:
                    logging.warning(f"Skipping regional data row {i+2} due to unparseable date: {row[date_idx]}")
                    continue

                data_tuple = (date_val, region_name_val, case_shiller_val, index_sa_val)
                
                cursor.execute(insert_sql, data_tuple)
                inserted_rows += cursor.rowcount
                processed_rows += 1

                if processed_rows % 100 == 0:
                    conn.commit()
                    logging.info(f"Processed {processed_rows} regional data rows, inserted {inserted_rows}...")
            
            except Exception as e:
                logging.error(f"Error processing regional data row {i+2}: {row}. Error: {e}")

        conn.commit()
        logging.info(f"Finished processing regional data. Total processed: {processed_rows}, Total inserted: {inserted_rows}")
    
        conn.close()
    except FileNotFoundError:
        logging.error(f"Regional data file {csv_file} not found.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during regional data processing: {e}")

if __name__ == "__main__":
    logging.info("Starting Redfin data ingestion process...")
    
    create_database(DB_FILE, SCHEMA_FILE)
    process_neighborhood_data(DB_FILE, NEIGHBORHOOD_DATA_FILE)
    process_regional_data(DB_FILE, REGIONAL_DATA_FILE)
    
    logging.info("Redfin data ingestion process completed.") 