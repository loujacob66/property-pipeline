import sqlite3
import pandas as pd
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import numpy as np # For NaN and power calculations if needed

# --- Configuration ---
DB_FILE = 'data/neighborhood_analysis.db'
PROPERTY_TYPE_FILTER = 'Single Family Residential'
MIN_HOMES_SOLD_THRESHOLD = 5 # Minimum homes sold for a data point to be considered reliable for appreciation

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions ---
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row # Access columns by name
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database {DB_FILE}: {e}")
        raise

def fetch_sf_residential_data(conn):
    """Fetches Single Family Residential data relevant for appreciation calculations."""
    query = f"""
    SELECT 
        id, -- This is neighborhood_data.id
        neighborhood_name, 
        city,
        period_end, 
        median_sale_price, 
        median_ppsf,
        homes_sold
    FROM neighborhood_data
    WHERE property_type = ? 
      AND period_end IS NOT NULL
      AND (median_sale_price IS NOT NULL OR median_ppsf IS NOT NULL) -- Ensure at least one price metric exists
    ORDER BY neighborhood_name, period_end ASC;
    """
    logging.info(f"Fetching data for property type: {PROPERTY_TYPE_FILTER}")
    try:
        df = pd.read_sql_query(query, conn, params=(PROPERTY_TYPE_FILTER,))
        if df.empty:
            logging.warning(f"No data found for property type '{PROPERTY_TYPE_FILTER}'. Exiting.")
            return pd.DataFrame() 
        
        df['period_end'] = pd.to_datetime(df['period_end'])
        logging.info(f"Fetched {len(df)} rows into DataFrame.")
        return df
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def store_appreciation_metric(cursor, neighborhood_data_id, metric_type, value, calculation_date):
    """Stores a single calculated appreciation metric in the database."""
    if pd.isna(value) or np.isinf(value): # Check for NaN or infinity
        return # Do not store invalid values
    try:
        cursor.execute("""
        INSERT INTO neighborhood_appreciation (
            neighborhood_data_id, metric_type, value, calculation_date
        ) VALUES (?, ?, ?, ?);
        """, (neighborhood_data_id, metric_type, float(value), calculation_date))
    except sqlite3.Error as e:
        logging.error(f"Error inserting appreciation metric ({metric_type}, {value}) for ID {neighborhood_data_id}: {e}")


# --- Calculation Functions (to be implemented) ---
def calculate_and_store_metrics(conn, df):
    """Calculates all appreciation metrics and stores them."""
    if df.empty:
        logging.info("DataFrame is empty, skipping calculations.")
        return

    cursor = conn.cursor()
    calculation_run_date = date.today().strftime('%Y-%m-%d')
    
    # Apply MIN_HOMES_SOLD_THRESHOLD
    df_filtered = df[df['homes_sold'] >= MIN_HOMES_SOLD_THRESHOLD].copy()
    if len(df_filtered) < len(df):
        logging.info(f"Filtered out {len(df) - len(df_filtered)} rows due to homes_sold < {MIN_HOMES_SOLD_THRESHOLD}.")
    if df_filtered.empty:
        logging.info(f"DataFrame is empty after filtering by homes_sold >= {MIN_HOMES_SOLD_THRESHOLD}. No metrics to calculate.")
        return

    # Ensure period_end is a datetime index for resampling and asof lookups
    df_filtered.set_index('period_end', inplace=True)

    # Group by neighborhood to process each one individually
    grouped = df_filtered.groupby('neighborhood_name')
    total_metrics_stored = 0

    logging.info(f"Calculating metrics for {grouped.ngroups} neighborhoods...")
    neighborhood_counter = 0 # Initialize counter

    for name, group_df in grouped:
        logging.debug(f"Processing neighborhood: {name} with {len(group_df)} data points (after homes_sold filter).")
        group_df = group_df.sort_index() # Ensure data is sorted by period_end (index)

        # Iterate through each data point in the neighborhood's history
        for current_period_end, row in group_df.iterrows():
            current_price = row['median_sale_price']
            current_ppsf = row['median_ppsf']
            neighborhood_data_id = row['id'] # This is the ID of the current row in neighborhood_data

            # --- Point-to-Point Change (MoM or equivalent for data frequency) ---
            # Find the immediately preceding data point
            # .shift(1) gets the previous row in the sorted group
            prev_row_series = group_df.shift(1).loc[current_period_end] # Get previous row data for current_period_end
            
            if not prev_row_series.empty and pd.notna(prev_row_series['median_sale_price']):
                prev_price = prev_row_series['median_sale_price']
                if prev_price > 0 and pd.notna(current_price):
                    ptp_price_appreciation = ((current_price / prev_price) - 1) * 100
                    store_appreciation_metric(cursor, neighborhood_data_id, 'median_sale_price_ptp_appreciation', ptp_price_appreciation, calculation_run_date)
                    total_metrics_stored +=1
            
            if not prev_row_series.empty and pd.notna(prev_row_series['median_ppsf']):
                prev_ppsf = prev_row_series['median_ppsf']
                if prev_ppsf > 0 and pd.notna(current_ppsf):
                    ptp_ppsf_appreciation = ((current_ppsf / prev_ppsf) - 1) * 100
                    store_appreciation_metric(cursor, neighborhood_data_id, 'median_ppsf_ptp_appreciation', ptp_ppsf_appreciation, calculation_run_date)
                    total_metrics_stored +=1
            
            # --- Time-based Lookbacks (Quarterly, Annual, Multi-Year) ---
            lookback_periods = {
                'quarterly': (relativedelta(months=3), 'Q'),
                'annual': (relativedelta(years=1), 'A'),
                '3_year_cagr': (relativedelta(years=3), '3Y'),
                '5_year_cagr': (relativedelta(years=5), '5Y'),
                '10_year_cagr': (relativedelta(years=10), '10Y'),
            }

            for period_name, (delta, suffix) in lookback_periods.items():
                past_date = current_period_end - delta
                
                # Use asof to find the closest available data point on or before past_date
                # Need to handle cases where group_df might be empty or past_date is too early
                past_data_row = group_df.asof(past_date)

                if pd.notna(past_data_row['median_sale_price']):
                    past_price = past_data_row['median_sale_price']
                    if past_price > 0 and pd.notna(current_price):
                        if 'cagr' in period_name:
                            years = delta.years
                            appreciation = (pow((current_price / past_price), (1/years)) - 1) * 100 if years > 0 else np.nan
                        else:
                            appreciation = ((current_price / past_price) - 1) * 100
                        store_appreciation_metric(cursor, neighborhood_data_id, f'median_sale_price_{period_name}_appreciation', appreciation, calculation_run_date)
                        total_metrics_stored +=1
                
                if pd.notna(past_data_row['median_ppsf']):
                    past_ppsf = past_data_row['median_ppsf']
                    if past_ppsf > 0 and pd.notna(current_ppsf):
                        if 'cagr' in period_name:
                            years = delta.years
                            appreciation = (pow((current_ppsf / past_ppsf), (1/years)) - 1) * 100 if years > 0 else np.nan
                        else:
                            appreciation = ((current_ppsf / past_ppsf) - 1) * 100
                        store_appreciation_metric(cursor, neighborhood_data_id, f'median_ppsf_{period_name}_appreciation', appreciation, calculation_run_date)
                        total_metrics_stored +=1
        
        neighborhood_counter += 1 # Increment counter
        if neighborhood_counter % 100 == 0: # Log progress every 100 neighborhoods
            logging.info(f"Committing after processing {neighborhood_counter} neighborhoods. Total metrics stored so far: {total_metrics_stored}")
            conn.commit()

    conn.commit() # Final commit
    logging.info(f"Finished calculating and storing metrics. Total metrics stored: {total_metrics_stored}")


# --- Main Execution ---
def main():
    logging.info("Starting appreciation calculation process...")
    conn = get_db_connection()
    if not conn:
        return

    try:
        df_sfr = fetch_sf_residential_data(conn)
        if not df_sfr.empty:
            # Clear the appreciation table for a fresh run (optional, but good for reruns)
            logging.info("Clearing existing data from neighborhood_appreciation table...")
            cursor = conn.cursor()
            cursor.execute("DELETE FROM neighborhood_appreciation;")
            conn.commit()
            logging.info("neighborhood_appreciation table cleared.")

            calculate_and_store_metrics(conn, df_sfr)
        else:
            logging.info("No Single Family Residential data to process.")

    except Exception as e:
        logging.error(f"An error occurred in the main execution: {e}")
        conn.rollback() # Rollback any partial transaction on error
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main() 