import streamlit as st
import pandas as pd
import sqlite3
import os

# Set page configuration
st.set_page_config(
    page_title="Property Pipeline Explorer",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Function to connect to the database
@st.cache_resource
def get_connection(db_path):
    """Create a connection to the SQLite database"""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn

# Function to load data
@st.cache_data
def load_data(_conn, table_name):
    """Load data from the specified table"""
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(query, _conn)

# App title and description
st.title("Property Pipeline Explorer")
st.markdown("Navigate, filter, and analyze property listings extracted from Gmail")

# Use hardcoded database path
db_path = "../data/listings.db"

# Check if database exists
if not os.path.exists(db_path):
    st.error(f"Database not found at {db_path}. Please check the path in the code.")
else:
    try:
        # Connect to the database
        conn = get_connection(db_path)
        
        # Make sure the new columns exist in the database
        try:
            conn.execute("SELECT tax_information, mls_type FROM listings LIMIT 1")
            st.success("New columns (tax_information, mls_type) exist in the database")
        except sqlite3.OperationalError as e:
            st.warning(f"Database schema issue: {e}. You may need to run the SQL script to add the new columns.")
        
        # Hardcode to listings table
        selected_table = "listings"
        
        # Load data from the selected table
        try:
            df = load_data(_conn=conn, table_name=selected_table)
            
            # Display table info
            st.subheader(f"Table: {selected_table}")
            st.write(f"Total Records: {len(df)}")
            
            # Debug: Show all available columns
            st.write("Available columns:", df.columns.tolist())
            
            # Show column selector
            all_columns = df.columns.tolist()
            
            # Set default columns - removed 'id' from the list and added new fields
            default_columns = ['address', 'city', 'state', 'zip', 'rent_yield', 'mls_type', 'tax_information', 'url']
            
            # Filter default columns to only include those that exist in the database
            default_columns = [col for col in default_columns if col in all_columns]
            
            # Debug: Show which default columns exist in the database
            st.write("Default columns that exist in the database:", default_columns)
            
            if not default_columns and all_columns:
                default_columns = all_columns[:5]  # First 5 columns if our defaults don't exist
                
            selected_columns = st.multiselect("Select Columns to Display", all_columns, default=default_columns)
            
            # Filter section
            st.sidebar.subheader("Filters")
            
            # Dynamic filters based on column types, excluding ID
            filters = {}
            for col in all_columns:
                # Skip ID filter
                if col.lower() == 'id':
                    continue
                    
                if df[col].dtype == "object":  # String columns
                    unique_vals = df[col].dropna().unique()
                    if len(unique_vals) < 20 and len(unique_vals) > 1:  # Only show selector if not too many unique values and more than 1
                        filters[col] = st.sidebar.multiselect(f"Filter by {col}", unique_vals)
                elif pd.api.types.is_numeric_dtype(df[col]):  # Numeric columns
                    min_val, max_val = float(df[col].min()), float(df[col].max())
                    # Add a small buffer if min and max are the same to avoid slider error
                    if min_val == max_val:
                        st.sidebar.text(f"{col}: {min_val}")
                    else:
                        filters[col] = st.sidebar.slider(f"Filter by {col}", min_val, max_val, (min_val, max_val))
            
            # Apply filters
            filtered_df = df.copy()
            for col, filter_val in filters.items():
                if filter_val:  # If filter is not empty
                    if isinstance(filter_val, list):  # For multiselect filters
                        if filter_val:
                            filtered_df = filtered_df[filtered_df[col].isin(filter_val)]
                    elif isinstance(filter_val, tuple) and len(filter_val) == 2:  # For range sliders
                        filtered_df = filtered_df[(filtered_df[col] >= filter_val[0]) & (filtered_df[col] <= filter_val[1])]
            
            # Display data with sorting
            if selected_columns:
                # Make a copy for display
                display_df = filtered_df[selected_columns].copy()
                
                # Create column config dictionary
                column_config = {
                    # Define column configurations for special columns
                    "url": st.column_config.LinkColumn(
                        "url",
                        display_text="listing",
                        width="small"
                    )
                }
                
                # Add new column configs if they exist
                if "tax_information" in selected_columns:
                    column_config["tax_information"] = st.column_config.TextColumn(
                        "Tax Information",
                        help="Property tax details",
                        width="medium"
                    )
                
                if "mls_type" in selected_columns:
                    column_config["mls_type"] = st.column_config.TextColumn(
                        "MLS Type",
                        help="Type of MLS listing",
                        width="small"
                    )
                
                # Display using st.dataframe with default sorting enabled
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config
                )
                
                # Show filtered record count
                st.caption(f"Displaying {len(display_df)} of {len(df)} records")
            else:
                st.warning("Please select at least one column to display.")
                
            # Display basic stats if numeric columns are present and user wants to see them
            numeric_cols = [col for col in display_df.columns if pd.api.types.is_numeric_dtype(display_df[col])]
            if numeric_cols and st.checkbox("Show Statistics"):
                st.subheader("Basic Statistics")
                st.write(display_df[numeric_cols].describe())
                
        except Exception as e:
            st.error(f"Error loading listings table: {e}")
            st.info("Make sure the 'listings' table exists in your database.")
            
    except Exception as e:
        st.error(f"Error accessing database: {e}")
