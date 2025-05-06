import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

# Define project root and database path using pathlib
try:
    # Assumes db_utils.py is in a 'lib' directory one level below project root
    LIB_DIR = Path(__file__).resolve().parent
    ROOT = LIB_DIR.parent
    DB_PATH = ROOT / "data" / "listings.db"
except NameError:
    # Fallback if __file__ is not available
    ROOT = Path(".").resolve()
    DB_PATH = ROOT / "data" / "listings.db"
    print(f"Warning: Could not determine script directory. Assuming CWD is project root: {ROOT}")
    print(f"Database path set to: {DB_PATH}")

def ensure_tables_exist(conn):
    """Ensure required tables (listings, listing_changes, address_blacklist) exist."""
    cursor = conn.cursor()
    # Ensure listings table exists (schema might vary, use IF NOT EXISTS)
    # Existing schema definition is complex, avoid redefining here. 
    # Rely on init_db.py or migrations for the full listings schema.
    # Just ensure the table exists minimally.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS listings (id INTEGER PRIMARY KEY)
    """)
    # Ensure listing_changes table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS listing_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER,
            field_name TEXT,
            old_value TEXT,
            new_value TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            FOREIGN KEY (listing_id) REFERENCES listings(id) ON DELETE CASCADE
        )
    """)
    # Ensure address_blacklist table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS address_blacklist (
            address TEXT PRIMARY KEY NOT NULL,
            reason TEXT,
            blacklisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    print("Ensured necessary tables exist.")

def track_changes(conn: sqlite3.Connection, listing_id: int, changes: Dict[str, Any], source: str) -> None:
    """
    Track changes to a listing in the listing_changes table.
    
    Args:
        conn: Database connection
        listing_id: ID of the listing that changed
        changes: Dictionary of field names and their new values
        source: Source of the change (e.g., 'gmail', 'compass')
    """
    cursor = conn.cursor()
    
    # Get current values for changed fields
    cursor.execute("SELECT * FROM listings WHERE id = ?", (listing_id,))
    current_values = dict(zip([col[0] for col in cursor.description], cursor.fetchone()))
    
    # Track each change
    for field, new_value in changes.items():
        if field in current_values:
            old_value = current_values[field]
            if old_value != new_value:  # Only track actual changes
                cursor.execute("""
                    INSERT INTO listing_changes 
                    (listing_id, field_name, old_value, new_value, source)
                    VALUES (?, ?, ?, ?, ?)
                """, (listing_id, field, str(old_value), str(new_value), source))

def update_listing(conn: sqlite3.Connection, listing_id: int, updates: Dict[str, Any], source: str) -> bool:
    """
    Update a listing and track changes.
    
    Args:
        conn: Database connection
        listing_id: ID of the listing to update
        updates: Dictionary of field names and their new values
        source: Source of the update (e.g., 'gmail', 'compass')
    
    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        cursor = conn.cursor()
        
        # Track changes before updating
        track_changes(conn, listing_id, updates, source)
        
        # Build update query
        set_clause = ", ".join(f"{key} = ?" for key in updates.keys())
        values = list(updates.values()) + [listing_id]
        
        # Update the listing
        cursor.execute(f"""
            UPDATE listings 
            SET {set_clause}, last_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        """, values)
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error updating listing {listing_id}: {e}")
        conn.rollback()
        return False

def insert_listings(listings, source="compass"):
    """Insert new listings or update existing ones, skipping blacklisted addresses."""
    if not DB_PATH.parent.exists():
        print(f"Error: Data directory {DB_PATH.parent} does not exist.")
        return
    if not DB_PATH.exists():
        print(f"Error: Database file {DB_PATH} does not exist. Run init_db.py first.")
        return
        
    conn = sqlite3.connect(DB_PATH)
    # Ensure all necessary tables exist before proceeding
    ensure_tables_exist(conn)
    cursor = conn.cursor()
    
    processed_count = 0
    inserted_count = 0
    updated_count = 0
    blacklisted_count = 0
    error_count = 0
    
    for listing in listings:
        processed_count += 1
        address = listing.get("address")
        if not address:
            print("⚠️ Skipping listing with no address.")
            error_count += 1
            continue

        try:
            address_lower = address.lower()
            
            # --- Blacklist Check --- 
            cursor.execute("SELECT 1 FROM address_blacklist WHERE LOWER(address) = ?", (address_lower,))
            if cursor.fetchone():
                print(f"🚫 Address '{address}' is blacklisted. Skipping.")
                blacklisted_count += 1
                continue # Skip this listing
            # --- End Blacklist Check ---

            # Proceed with insertion/update logic
            print(f"🔍 Processing listing: {address}")
            # Print key details for debugging
            # for k in ("city", "state", "zip", "price", "beds", "baths", "sqft", "url"):
            #     print(f"   {k}: {listing.get(k)}")

            # Check if listing exists by address (case-insensitive)
            cursor.execute("SELECT id FROM listings WHERE LOWER(address) = ?", (address_lower,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing listing
                listing_id = existing[0]
                print(f"  Found existing listing ID: {listing_id}")
                # Define fields allowed for update
                allowed_update_fields = {
                    "city", "state", "zip", "price", "beds", "baths", "sqft", 
                    "estimated_rent", "rent_yield", "url", "from_collection", 
                    "mls_number", "mls_type", "tax_information", "days_on_compass", 
                    "favorite", "status", "walk_score", "transit_score", 
                    "bike_score", "walkscore_shorturl", "compass_shorturl"
                }
                updates = {k: v for k, v in listing.items() if k in allowed_update_fields and v is not None}
                
                if not updates:
                     print("  No valid fields to update.")
                     continue # Nothing to update

                # Fetch current values to compare before updating
                cursor.execute(f"SELECT {', '.join(updates.keys())} FROM listings WHERE id = ?", (listing_id,))
                current_values = dict(zip(updates.keys(), cursor.fetchone()))

                # Filter out updates where the value hasn't actually changed
                actual_updates = {}
                for key, new_value in updates.items():
                    # Handle type differences, e.g., comparing int from DB with float from scrape
                    current_value = current_values.get(key)
                    try:
                        # Attempt numeric comparison if possible
                        if isinstance(new_value, (int, float)) and isinstance(current_value, (int, float)):
                            if float(new_value) != float(current_value):
                                actual_updates[key] = new_value
                        elif str(new_value) != str(current_value):
                             actual_updates[key] = new_value
                    except (ValueError, TypeError):
                         # Fallback to string comparison if numeric fails
                         if str(new_value) != str(current_value):
                             actual_updates[key] = new_value
                
                if actual_updates:
                    print(f"  Fields to update: {', '.join(actual_updates.keys())}")
                    if update_listing(conn, listing_id, actual_updates, source):
                        print("✅ Successfully updated existing listing.")
                        updated_count += 1
                    else:
                         print("❌ Failed to update existing listing.")
                         error_count += 1
                else:
                    print("  No actual changes detected.")

            else:
                # Insert new listing
                print(f"  Inserting as new listing.")
                # Define columns for insertion (should match the table schema)
                # Assuming a comprehensive schema based on previous scripts
                columns = [
                    "address", "city", "state", "zip", "price", "beds", "baths", 
                    "sqft", "price_per_sqft", "url", "from_collection", "source", 
                    "estimated_rent", "rent_yield", "mls_number", "mls_type", 
                    "tax_information", "days_on_compass", "favorite", "status", 
                    "walk_score", "transit_score", "bike_score", 
                    "walkscore_shorturl", "compass_shorturl", 
                    "imported_at", "last_updated" # Timestamps handled by default/triggers potentially
                ]
                
                # Prepare values, using None for missing keys
                values_tuple = []
                missing_keys = []
                for col in columns:
                     if col == "imported_at" or col == "last_updated":
                         values_tuple.append(datetime.now())
                     elif col == "source":
                         values_tuple.append(source) # Use the passed source
                     else:
                        val = listing.get(col)
                        if val is None and col not in ["price_per_sqft"]: # Allow calculated price_per_sqft to be None
                            missing_keys.append(col)
                        values_tuple.append(val)

                if missing_keys:
                    print(f"  Warning: Missing data for columns: {', '.join(missing_keys)}")

                placeholders = ", ".join(["?"] * len(columns))
                sql = f"INSERT INTO listings ({', '.join(columns)}) VALUES ({placeholders})"
                
                try:
                    cursor.execute(sql, tuple(values_tuple))
                    print("✅ Successfully inserted new listing.")
                    inserted_count += 1
                except sqlite3.IntegrityError as ie:
                    if "UNIQUE constraint failed: listings.url" in str(ie):
                         print(f"⚠️ Integrity Error: URL '{listing.get('url')}' likely already exists for a different address. Skipping insert.")
                         error_count += 1
                    elif "UNIQUE constraint failed: listings.address" in str(ie):
                         print(f"⚠️ Integrity Error: Address '{address}' already exists (race condition?). Skipping insert.")
                         error_count += 1
                    else:
                        print(f"❌ Database Integrity Error during insert: {ie}")
                        error_count += 1
                except Exception as inner_e:
                    print(f"❌ Error during insert execution: {inner_e}")
                    error_count += 1

        except Exception as e:
            print(f"❌ Error processing listing '{address}': {e}")
            import traceback
            traceback.print_exc() # Print detailed traceback for errors
            error_count += 1
            
    conn.commit()
    conn.close()
    print("--- Insertion/Update Summary ---")
    print(f"  Processed:   {processed_count}")
    print(f"  Inserted:    {inserted_count}")
    print(f"  Updated:     {updated_count}")
    print(f"  Blacklisted: {blacklisted_count}")
    print(f"  Errors:      {error_count}")
    print("------------------------------")

def get_listing_changes(listing_id: int, limit: Optional[int] = None) -> list:
    """
    Get change history for a listing.
    
    Args:
        listing_id: ID of the listing
        limit: Optional limit on number of changes to return
    
    Returns:
        list: List of changes, most recent first
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = """
        SELECT field_name, old_value, new_value, changed_at, source
        FROM listing_changes
        WHERE listing_id = ?
        ORDER BY changed_at DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
        
    cursor.execute(query, (listing_id,))
    changes = cursor.fetchall()
    
    conn.close()
    return changes
