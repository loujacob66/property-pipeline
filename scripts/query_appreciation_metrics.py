import sqlite3
import pandas as pd
import logging
import argparse
from datetime import datetime

# --- Configuration ---
DB_FILE = 'data/neighborhood_analysis.db'
PROPERTY_TYPE_FOR_REPORT = 'Single Family Residential' # To find the relevant base data ID
MIN_HOMES_SOLD_THRESHOLD_REPORT = 5 # Consistent with calculation script

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions ---
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.row_factory = sqlite3.Row # Access columns by name
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database {DB_FILE}: {e}")
        raise

def get_latest_relevant_data_id(conn, neighborhood_name):
    """Finds the most recent neighborhood_data.id for a given neighborhood 
       that meets criteria (SFR, min homes sold)."""
    query = f"""
    SELECT id
    FROM neighborhood_data
    WHERE neighborhood_name = ? 
      AND property_type = ?
      AND homes_sold >= ?
    ORDER BY period_end DESC
    LIMIT 1;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (neighborhood_name, PROPERTY_TYPE_FOR_REPORT, MIN_HOMES_SOLD_THRESHOLD_REPORT))
        result = cursor.fetchone()
        return result['id'] if result else None
    except sqlite3.Error as e:
        logging.error(f"Error fetching latest data ID for {neighborhood_name}: {e}")
        return None

def get_neighborhood_appreciation_report(conn, neighborhood_name):
    """Retrieves and prints a summary of appreciation metrics for a specific neighborhood."""
    latest_data_id = get_latest_relevant_data_id(conn, neighborhood_name)
    if not latest_data_id:
        logging.warning(f"Could not find a recent, relevant data point for neighborhood: '{neighborhood_name}' with property type '{PROPERTY_TYPE_FOR_REPORT}' and at least {MIN_HOMES_SOLD_THRESHOLD_REPORT} homes sold.")
        print(f"No report generated for: {neighborhood_name}")
        return

    logging.info(f"Generating report for '{neighborhood_name}' (based on neighborhood_data.id: {latest_data_id})")

    query = f"""
    SELECT 
        na.metric_type, 
        na.value,
        nd.period_end, -- To show which period this metric corresponds to
        nd.median_sale_price AS current_median_sale_price, -- For context
        nd.median_ppsf AS current_median_ppsf, -- For context
        nd.homes_sold AS current_homes_sold -- For context
    FROM neighborhood_appreciation na
    JOIN neighborhood_data nd ON na.neighborhood_data_id = nd.id
    WHERE na.neighborhood_data_id = ? 
    ORDER BY na.metric_type;
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=(latest_data_id,))
        if df.empty:
            print(f"No appreciation metrics found for '{neighborhood_name}' (data_id: {latest_data_id}). This might mean calculations haven't run or no valid prior data existed for this point.")
            return

        print(f"\n--- Appreciation Report for: {neighborhood_name} ---")
        # Displaying the period_end and current values from the first row (they should be the same for this data_id)
        print(f"Metrics based on data ending: {pd.to_datetime(df['period_end'].iloc[0]).strftime('%Y-%m-%d')}")
        print(f"Current Median Sale Price (SFR): ${df['current_median_sale_price'].iloc[0]:,.0f}")
        print(f"Current Median PPSF (SFR): ${df['current_median_ppsf'].iloc[0]:,.2f}")
        print(f"Current Homes Sold (SFR): {df['current_homes_sold'].iloc[0]}")
        print("-----------------------------------------------------")

        for _, row in df.iterrows():
            print(f"{row['metric_type']:<50} | {row['value']:>8.2f}%")
        print("-----------------------------------------------------\n")

    except Exception as e:
        logging.error(f"Error generating report for {neighborhood_name}: {e}")

# --- Placeholder for Top N function ---
def get_top_n_report(conn, top_n_count, metric_to_sort_by):
    logging.info(f"Generating Top {top_n_count} report based on '{metric_to_sort_by}'...")

    # Step 1: Find the latest neighborhood_data.id for each neighborhood that meets base criteria
    # This subquery ensures we are only looking at metrics from the latest valid period for each neighborhood.
    # We also need to ensure that the specific metric_to_sort_by actually exists for that latest_data_id.
    query = f"""
    WITH LatestRelevantData AS (
        -- Select the latest period_end for each neighborhood meeting criteria
        SELECT 
            nd.neighborhood_name,
            MAX(nd.period_end) AS max_period_end
        FROM neighborhood_data nd
        WHERE nd.property_type = ? 
          AND nd.homes_sold >= ?
        GROUP BY nd.neighborhood_name
    ),
    LatestDataWithMetric AS (
        -- Join back to get the specific ID and check if the desired metric exists for this latest point
        SELECT
            nd.id AS neighborhood_data_id,
            nd.neighborhood_name,
            nd.period_end,
            na.value AS metric_value
        FROM neighborhood_data nd
        JOIN LatestRelevantData lrd ON nd.neighborhood_name = lrd.neighborhood_name AND nd.period_end = lrd.max_period_end
        JOIN neighborhood_appreciation na ON nd.id = na.neighborhood_data_id
        WHERE nd.property_type = ? 
          AND nd.homes_sold >= ?
          AND na.metric_type = ? 
    )
    SELECT 
        ldm.neighborhood_name,
        ldm.period_end,
        ldm.metric_value
    FROM LatestDataWithMetric ldm
    ORDER BY ldm.metric_value DESC
    LIMIT ?;
    """

    try:
        params = (
            PROPERTY_TYPE_FOR_REPORT, 
            MIN_HOMES_SOLD_THRESHOLD_REPORT,
            PROPERTY_TYPE_FOR_REPORT, # For the join condition in LatestDataWithMetric
            MIN_HOMES_SOLD_THRESHOLD_REPORT, # For the join condition in LatestDataWithMetric
            metric_to_sort_by,
            top_n_count
        )
        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            print(f"No data found for Top N report with metric '{metric_to_sort_by}'. Ensure the metric name is correct and data exists.")
            return

        print(f"\n--- Top {top_n_count} Neighborhoods by: {metric_to_sort_by} ---")
        print(f"Property Type: {PROPERTY_TYPE_FOR_REPORT}, Min Homes Sold: {MIN_HOMES_SOLD_THRESHOLD_REPORT}")
        print("------------------------------------------------------------------------------------")
        header = ["Neighborhood Name", "Period End", f"{metric_to_sort_by} (%)"]
        # Dynamic padding for neighborhood name based on longest name in results for better alignment
        max_name_len = df['neighborhood_name'].str.len().max() if not df.empty else 30
        max_name_len = max(max_name_len, len(header[0])) # Ensure header itself fits
        
        print(f"{header[0]:<{max_name_len}} | {header[1]:<12} | {header[2]:>15}")
        print("-" * (max_name_len + 3 + 12 + 3 + 15 + 2))

        for _, row in df.iterrows():
            period_end_str = pd.to_datetime(row['period_end']).strftime('%Y-%m-%d')
            print(f"{row['neighborhood_name']:<{max_name_len}} | {period_end_str:<12} | {row['metric_value']:>15.2f}%")
        print("------------------------------------------------------------------------------------\n")

    except Exception as e:
        logging.error(f"Error generating Top N report for metric '{metric_to_sort_by}': {e}")
        print(f"Could not generate Top N report. Check logs.")

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Query and display appreciation metrics from the Redfin database.") # Updated description
    parser.add_argument("--neighborhood", type=str, help="Display a detailed report for a specific neighborhood name (e.g., 'Denver, CO - Sloan Lake').")
    parser.add_argument("--top_n", type=int, help="Display the top N neighborhoods by a specific metric.")
    parser.add_argument("--metric", type=str, default="median_sale_price_annual_appreciation", help="The metric to use for sorting Top N (e.g., 'median_sale_price_annual_appreciation', 'median_ppsf_5_year_cagr_appreciation'). Default is median_sale_price_annual_appreciation.")
    
    args = parser.parse_args()

    conn = get_db_connection()
    if not conn:
        return

    try:
        if args.neighborhood:
            get_neighborhood_appreciation_report(conn, args.neighborhood)
        elif args.top_n:
            get_top_n_report(conn, args.top_n, args.metric)
        else:
            print("No report type specified. Use --neighborhood NAME or --top_n COUNT.") # Updated help guidance
            parser.print_help()
            
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main() 