import sqlite3
import logging

# --- Configuration ---
DB_FILE = 'data/neighborhood_analysis.db'

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_query(conn, query, fetch_all=True):
    """Helper function to run a query and fetch results."""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        if fetch_all:
            return cursor.fetchall()
        else:
            return cursor.fetchone()
    except sqlite3.Error as e:
        logging.error(f"SQL error: {e} for query: \n{query}")
        return None

def main():
    logging.info(f"Connecting to database: {DB_FILE}")
    conn = None # Ensure conn is defined in outer scope
    try:
        conn = sqlite3.connect(DB_FILE)

        # 1. Distinct property_type values
        logging.info("\n--- Distinct Property Types ---")
        query1 = "SELECT DISTINCT property_type FROM neighborhood_data ORDER BY property_type;"
        results1 = run_query(conn, query1)
        if results1:
            for row in results1:
                print(row[0])

        # 2. Range of period_begin and period_end dates
        logging.info("\n--- Date Ranges ---")
        query2 = """
        SELECT MIN(period_begin) AS min_period_begin, MAX(period_begin) AS max_period_begin,
               MIN(period_end) AS min_period_end, MAX(period_end) AS max_period_end
        FROM neighborhood_data;
        """
        results2 = run_query(conn, query2, fetch_all=False)
        if results2:
            print(f"Min Period Begin: {results2[0]}")
            print(f"Max Period Begin: {results2[1]}")
            print(f"Min Period End:   {results2[2]}")
            print(f"Max Period End:   {results2[3]}")

        # 3. Sample rows from "Denver" for "SLOAN LAKE"
        logging.info("\n--- Sample Rows (Denver - SLOAN LAKE) ---")
        # Note: neighborhood_name in DB is the full 'REGION' string like "Denver, CO - Sloan Lake"
        query3 = """
        SELECT period_end, neighborhood_name, property_type, median_sale_price, homes_sold, inventory, median_dom 
        FROM neighborhood_data
        WHERE city = 'Denver' AND neighborhood_name LIKE '%SLOAN LAKE%'
        ORDER BY period_end DESC
        LIMIT 5;
        """
        results3 = run_query(conn, query3)
        if results3:
            # Print header for sample rows
            header3 = ["Period End", "Neighborhood Name", "Property Type", "Median Sale Price", "Homes Sold", "Inventory", "Median DOM"]
            print(" | ".join(header3))
            for row in results3:
                print(" | ".join(map(str, row)))
        else:
            print("No data found for Sloan Lake in Denver with the LIKE operator. Check TARGET_NEIGHBORHOOD_KEYWORDS_UPPER in ingest script if this is unexpected.")

        # 4. Counts of records per city
        logging.info("\n--- Record Counts per City ---")
        query4 = """
        SELECT city, COUNT(*) AS record_count
        FROM neighborhood_data
        GROUP BY city
        ORDER BY record_count DESC;
        """
        results4 = run_query(conn, query4)
        if results4:
            print("City | Record Count")
            for row in results4:
                print(f"{row[0]} | {row[1]}")

    except sqlite3.Error as e:
        logging.error(f"Database connection error or other SQL issue: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main() 