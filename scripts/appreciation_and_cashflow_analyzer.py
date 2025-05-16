#!/usr/bin/env python3
"""
Enhanced Cashflow and Appreciation Analyzer for Real Estate Properties

This script calculates estimated monthly cashflow, long-term appreciation,
and total return on investment for a property.
It uses detailed CapEx, maintenance, and market-based appreciation factors.

Usage:
    python scripts/appreciation_and_cashflow_analyzer.py --address "<FULL_ADDRESS>" \
                                        --down-payment <DOLLARS> \
                                        --rate <ANNUAL_RATE> \
                                        --insurance <ANNUAL_INSURANCE> \
                                        --misc-monthly <MONTHLY_MISC> \
                                        [--other_options...]
"""

import argparse
import sqlite3
import re
import json
import datetime
import sys
from pathlib import Path
import requests # For fetching real appreciation data
import csv      # For parsing CSV appreciation data
from io import StringIO # For handling CSV data in memory

# --- Constants ---
ROOT = Path(__file__).parent.parent
DEFAULT_DB_PATH = ROOT / "data" / "listings.db"
DEFAULT_CONFIG_PATH = ROOT / "config" / "cashflow_config.json" # Assumes a shared config

# --- New constant for historical data query ---
MIN_HOMES_SOLD_THRESHOLD_HISTORICAL = 5

# CapEx Components (from modified_cashflow_analyzer.py)
CAPEX_COMPONENTS = {
    "roof": {"lifespan": 25, "cost_per_sqft": 5.5},
    "hvac": {"lifespan": 18, "cost_base": 4500, "cost_per_sqft": 1.5},
    "water_heater": {"lifespan": 10, "cost_base": 900},
    "electrical": {"lifespan": 35, "cost_base": 1800},
    "plumbing": {"lifespan": 45, "cost_per_sqft": 2.0},
    "flooring": {"lifespan": 10, "cost_per_sqft": 3.5},
    "appliances": {"lifespan": 12, "cost_base": 3000},
    "bathroom_fixtures": {"lifespan": 18, "cost_base": 1000},
    "interior_paint": {"lifespan": 6, "cost_per_sqft": 1.0},
    "cabinets": {"lifespan": 18, "cost_per_sqft": 1.25},
    "exterior_paint": {"lifespan": 8, "cost_per_sqft": 1.5},
    "windows": {"lifespan": 20, "cost_per_sqft": 1.75},
    "driveway": {"lifespan": 25, "cost_base": 3000}
}

# Property condition multipliers (from modified_cashflow_analyzer.py)
CONDITION_MULTIPLIERS = {
    "excellent": 0.7, "good": 1.0, "fair": 1.3, "poor": 1.7
}

# --- Helper Functions (Core Logic from modified_cashflow_analyzer.py) ---

def get_age_multiplier(age):
    if age <= 5: return 0.6
    elif age <= 15: return 0.9
    elif age <= 30: return 1.1
    elif age <= 50: return 1.3
    else: return 1.5

def load_config(config_path):
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_path}'. Please check its format.", file=sys.stderr)
        return {}

def parse_arguments(config):
    parser = argparse.ArgumentParser(description="Real Estate Cashflow and Appreciation Analyzer")
    
    # Define script defaults here so they can be used as fallbacks for config.get()
    SCRIPT_DEFAULTS = {
        "loan_term": 30,
        "db_path": str(DEFAULT_DB_PATH),
        "config_path": str(DEFAULT_CONFIG_PATH),
        "vacancy_rate": 5.0,
        "property_mgmt_fee": 0.0,
        "maintenance_percent": 1.0,
        "capex_percent": 1.0,
        "utilities_monthly": 0.0,
        "property_age": 20,
        "property_condition": "good",
        "square_feet": 1400.0,
        "use_dynamic_capex": False,
        "verbose": False,
        "appreciation_rate": None, # Explicitly None, to be set by CLI, historical, or JSON logic
        "neighborhood": None,      # Explicitly None, to be auto-detected or set by CLI
        "investment_horizon": 5,
        "fetch_real_appreciation": True, # <<< CHANGE THIS TO TRUE FOR TESTING
        # New arguments for historical
        "neighborhood_analysis_db_path": ROOT / "data" / "neighborhood_analysis.db", # Default path
        "use_historical_metric": "median_sale_price_5_year_cagr_appreciation", # Default metric to use, matching DB
        "target_city_for_historical": None # e.g., "Denver"
    }

    # Helper to get default value: config > script_default
    def get_default_val(key):
        return config.get(key, SCRIPT_DEFAULTS.get(key))
    
    # --- Argument Definitions ---
    # For required args like address, no default is set here.
    parser.add_argument("--address", type=str, required=True, help="Full property address.")

    # Financial args - default to None if not in config, then validated later
    parser.add_argument("--down-payment", type=float, default=config.get("down_payment"), help="Down payment amount (dollars).")
    parser.add_argument("--rate", type=float, default=config.get("rate"), help="Annual interest rate (e.g., 5.5).")
    parser.add_argument("--insurance", type=float, default=config.get("insurance"), help="Annual insurance cost.")
    parser.add_argument("--misc-monthly", type=float, default=config.get("misc_monthly"), help="Miscellaneous monthly costs.")

    # Other args using the helper for Config > ScriptDefault precedence for their *defaults*
    parser.add_argument("--loan-term", type=int, default=get_default_val("loan_term"), help="Loan term in years.")
    parser.add_argument("--db-path", type=str, default=SCRIPT_DEFAULTS["db_path"], help="Path to SQLite DB (generally not overridden by config).")
    parser.add_argument("--config-path", type=str, default=SCRIPT_DEFAULTS["config_path"], help="Path to JSON config file.")
    parser.add_argument("--vacancy-rate", type=float, default=get_default_val("vacancy_rate"), help="Vacancy rate (%).")
    parser.add_argument("--property-mgmt-fee", type=float, default=get_default_val("property_mgmt_fee"), help="Property management fee (%).")
    parser.add_argument("--maintenance-percent", type=float, default=get_default_val("maintenance_percent"), help="Annual maintenance (%% of property value).")
    parser.add_argument("--capex-percent", type=float, default=get_default_val("capex_percent"), help="Annual CapEx reserve (%% of property value).")
    parser.add_argument("--utilities-monthly", type=float, default=get_default_val("utilities_monthly"), help="Monthly utilities paid by landlord.")
    parser.add_argument("--property-age", type=int, default=get_default_val("property_age"), help="Property age (years). Used if not in DB or config.")
    parser.add_argument("--property-condition", type=str, default=get_default_val("property_condition"), choices=["excellent", "good", "fair", "poor"], help="Property condition.")
    parser.add_argument("--square-feet", type=float, default=get_default_val("square_feet"), help="Square footage. Used if not in DB or config.")
    
    parser.add_argument("--use-dynamic-capex", action=argparse.BooleanOptionalAction, default=get_default_val("use_dynamic_capex"), help="Use detailed CapEx calculations.")
    parser.add_argument("--capex-guide", action="store_true", help="Print CapEx reference guide and exit.") # No default from config for this action
    parser.add_argument("-v", "--verbose", action="store_true", default=get_default_val("verbose"), help="Enable verbose debug output.")

    # Appreciation-specific arguments
    parser.add_argument("--appreciation-rate", type=float, default=get_default_val("appreciation_rate"), help="Manual annual appreciation rate (%%).")
    parser.add_argument("--neighborhood", type=str, default=SCRIPT_DEFAULTS.get("neighborhood"), help="Manual neighborhood override. Auto-detected by ZIP if not set here or by CLI.")
    parser.add_argument("--investment-horizon", type=int, default=get_default_val("investment_horizon"), help="Investment holding period (years).")
    parser.add_argument("--fetch-real-appreciation", action=argparse.BooleanOptionalAction, default=get_default_val("fetch_real_appreciation"), help="Fetch real appreciation data.")
    
    # New arguments for historical data
    parser.add_argument("--neighborhood-analysis-db-path", type=str, default=str(SCRIPT_DEFAULTS["neighborhood_analysis_db_path"]), help="Path to neighborhood_analysis.db for historical metrics.")
    parser.add_argument("--use-historical-metric", type=str, default=SCRIPT_DEFAULTS["use_historical_metric"], help="Metric name from neighborhood_appreciation table to use (e.g., median_sale_price_5_year_cagr).")
    parser.add_argument("--target-city-for-historical", type=str, default=SCRIPT_DEFAULTS["target_city_for_historical"], help="Specify city for disambiguating neighborhood in historical DB.")

    # Now, parse_args(). If CLI provides a value, it overrides the default set above.
    args = parser.parse_args()
    
    # Validation for essential financial args (must come from CLI or config)
    required_financial_args = ["down_payment", "rate", "insurance", "misc_monthly"]
    missing_financial_args = [arg_name for arg_name in required_financial_args if getattr(args, arg_name) is None]

    if missing_financial_args:
        parser.error(
            f"Missing required financial arguments: {', '.join(missing_financial_args)}. "
            f"Provide via CLI or ensure they are in config ('{args.config_path}')."
        )
        
    return args

def fetch_property_data(db_path, address, verbose=False):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT price, tax_information, estimated_rent, id, sqft, year_built, zip, city FROM listings WHERE address = ?",
            (address,)
        )
        row = cursor.fetchone()
        if row:
            db_price, db_tax_info, db_rent_raw, db_id, db_sqft_raw, db_year_built_raw, db_zip, db_city = row
            processed_sqft = None
            if db_sqft_raw is not None:
                try:
                    val = float(db_sqft_raw)
                    if val > 0: processed_sqft = val
                    elif verbose: print(f"Warning: DB sqft '{db_sqft_raw}' for '{address}' not positive.", file=sys.stderr)
                except (ValueError, TypeError):
                    if verbose: print(f"Warning: DB sqft '{db_sqft_raw}' for '{address}' not valid number.", file=sys.stderr)
            
            calculated_age = None
            if db_year_built_raw:
                match = re.search(r'(\d{4})', str(db_year_built_raw))
                if match:
                    year_built = int(match.group(1))
                    current_year = datetime.datetime.now().year
                    if 1800 <= year_built <= current_year:
                        calculated_age = current_year - year_built
                    elif verbose: print(f"Warning: Parsed year '{year_built}' from DB '{db_year_built_raw}' for '{address}' out of range.", file=sys.stderr)
                elif verbose: print(f"Warning: Could not parse year from DB '{db_year_built_raw}' for '{address}'.", file=sys.stderr)
            
            return {
                "price": db_price, "tax_information_raw": db_tax_info,
                "estimated_rent_raw": db_rent_raw, "id": db_id, "sqft": processed_sqft,
                "year_built_raw": db_year_built_raw, "calculated_property_age": calculated_age,
                "zip": db_zip, "city": db_city
            }
        else:
            print(f"Error: Property with address '{address}' not found.", file=sys.stderr)
            return None
    except sqlite3.Error as e:
        print(f"Database error for '{address}': {e}", file=sys.stderr)
        return None
    finally:
        conn.close()

def parse_tax_amount(tax_info_str, verbose=False):
    if not tax_info_str: return None
    match = re.search(r'\$?([\d,]+(?:\.\d+)?)', tax_info_str)
    if match:
        try: return float(match.group(1).replace(',', ''))
        except ValueError: 
            if verbose: print(f"Warning: Could not parse tax amount from '{tax_info_str}'.", file=sys.stderr)
            return None
    if verbose: print(f"Warning: No tax amount pattern found in '{tax_info_str}'.", file=sys.stderr)
    return None

def calculate_mortgage_payment(principal, annual_rate_percent, term_years):
    if principal <= 0: return 0
    monthly_rate = (annual_rate_percent / 100) / 12
    num_payments = term_years * 12
    if monthly_rate == 0: return principal / num_payments if num_payments > 0 else 0
    return principal * (monthly_rate * (1 + monthly_rate) ** num_payments) / \
           ((1 + monthly_rate) ** num_payments - 1)

def calculate_capex_reserves(purchase_price, sqft, age, condition, verbose=False):
    age_mult = get_age_multiplier(age)
    cond_mult = CONDITION_MULTIPLIERS.get(condition.lower(), 1.0)
    reserves = {"components": {}, "total_annual": 0, "total_monthly": 0}
    
    if sqft is None or sqft <=0: # Need sqft for many components
        if verbose: print("Warning: Valid square footage not available for detailed CapEx. Using 0 for component costs dependent on sqft.", file=sys.stderr)
        # Allow calculation to proceed, but sqft-based costs will be 0 or base only
    
    for comp, details in CAPEX_COMPONENTS.items():
        adj_lifespan = details["lifespan"] * (1 / cond_mult)
        repl_cost = 0
        if "cost_per_sqft" in details:
            if sqft and sqft > 0:
                 repl_cost = details["cost_per_sqft"] * sqft
            if "cost_base" in details: # Add base cost if sqft cost was calculated or not
                repl_cost += details["cost_base"]
        elif "cost_base" in details:
             repl_cost = details["cost_base"]
        
        adj_cost = repl_cost * cond_mult * age_mult
        annual_res = adj_cost / adj_lifespan if adj_lifespan > 0 else 0
        
        reserves["components"][comp] = {
            "replacement_cost": adj_cost, "lifespan_years": adj_lifespan,
            "annual_reserve": annual_res, "monthly_reserve": annual_res / 12
        }
        reserves["total_annual"] += annual_res
    
    reserves["total_monthly"] = reserves["total_annual"] / 12
    reserves["percent_of_value"] = (reserves["total_annual"] / purchase_price) * 100 if purchase_price > 0 else 0
    return reserves

def calculate_financial_components(
    purchase_price, tax_info_raw, est_monthly_rent, down_payment_dollars,
    annual_rate_percent, loan_term_years, annual_insurance, misc_monthly,
    vacancy_rate_pct, property_mgmt_fee_pct, maintenance_pct, capex_pct,
    utilities_monthly, use_dynamic_capex, prop_age, prop_cond, sq_ft, verbose=False
):
    if purchase_price is None or purchase_price <= 0:
        print("Error: Purchase price missing or invalid.", file=sys.stderr)
        return None
    
    eff_rent = est_monthly_rent if est_monthly_rent is not None else 0
    if est_monthly_rent is None and verbose:
        print("Warning: Estimated monthly rent not found. Using $0.", file=sys.stderr)

    dp_amount = down_payment_dollars
    loan_amt = 0
    if dp_amount > purchase_price:
        if verbose: print(f"Info: Down payment (${dp_amount:,.2f}) exceeds price. Clamping loan to $0.", file=sys.stderr)
        dp_amount = purchase_price
    elif dp_amount < 0:
        if verbose: print("Info: Negative down payment. Setting to $0.", file=sys.stderr)
        dp_amount = 0
    loan_amt = purchase_price - dp_amount
    dp_pct = (dp_amount / purchase_price) * 100 if purchase_price > 0 else 0

    p_and_i = calculate_mortgage_payment(loan_amt, annual_rate_percent, loan_term_years)
    annual_tax = parse_tax_amount(tax_info_raw, verbose)
    monthly_tax = (annual_tax / 12) if annual_tax is not None else 0
    monthly_ins = (annual_insurance / 12) if annual_insurance is not None else 0

    eff_rent_after_vacancy = eff_rent
    monthly_prop_mgmt = 0
    adj_maint_pct = maintenance_pct
    monthly_maint = 0
    adj_capex_pct = capex_pct
    monthly_capex_val = 0
    capex_details = None

    if use_dynamic_capex:
        eff_rent_after_vacancy = eff_rent * (1 - (vacancy_rate_pct / 100))
        monthly_prop_mgmt = eff_rent_after_vacancy * (property_mgmt_fee_pct / 100)
        
        age_mult = get_age_multiplier(prop_age)
        cond_mult = CONDITION_MULTIPLIERS.get(prop_cond.lower(), 1.0)
        adj_maint_pct = maintenance_pct * age_mult * cond_mult
        monthly_maint = (purchase_price * (adj_maint_pct / 100)) / 12
        
        capex_details = calculate_capex_reserves(purchase_price, sq_ft, prop_age, prop_cond, verbose)
        monthly_capex_val = capex_details["total_monthly"]
        adj_capex_pct = capex_details["percent_of_value"]
    
    total_monthly_exp = p_and_i + monthly_tax + monthly_ins + misc_monthly
    if use_dynamic_capex:
        total_monthly_exp += monthly_prop_mgmt + monthly_maint + monthly_capex_val + utilities_monthly
    
    net_monthly_cashflow = eff_rent_after_vacancy - total_monthly_exp
    annual_cashflow = net_monthly_cashflow * 12
    coc_roi = (annual_cashflow / dp_amount) * 100 if dp_amount > 0 else 0
    
    annual_noi, cap_rate = None, None
    if use_dynamic_capex:
        # NOI = Effective Gross Income - Operating Expenses (excluding P&I, but including prop mgmt, maint, capex)
        op_expenses_annual = (monthly_tax + monthly_ins + monthly_prop_mgmt + monthly_maint + monthly_capex_val + utilities_monthly + misc_monthly) * 12
        annual_noi = (eff_rent_after_vacancy * 12) - op_expenses_annual
        cap_rate = (annual_noi / purchase_price) * 100 if purchase_price > 0 else 0

    return {
        "purchase_price": purchase_price, "down_payment_amount": dp_amount, "down_payment_percentage": dp_pct,
        "loan_amount": loan_amt, "annual_interest_rate_percent": annual_rate_percent, "loan_term_years": loan_term_years,
        "annual_insurance_cost": annual_insurance, "misc_monthly_cost": misc_monthly, "tax_info_raw": tax_info_raw,
        "estimated_monthly_rent": eff_rent, "monthly_p_and_i": p_and_i, "annual_taxes": annual_tax,
        "monthly_taxes": monthly_tax, "monthly_insurance": monthly_ins,
        "vacancy_rate_percent": vacancy_rate_pct if use_dynamic_capex else None,
        "effective_rent_after_vacancy": eff_rent_after_vacancy, # Store this regardless of use_dynamic_capex for appreciation calc
        "property_mgmt_fee_percent": property_mgmt_fee_pct if use_dynamic_capex else None,
        "monthly_property_mgmt": monthly_prop_mgmt if use_dynamic_capex else 0, # Store 0 if not dynamic
        "maintenance_percent": maintenance_pct, "adjusted_maintenance_percent": adj_maint_pct if use_dynamic_capex else None,
        "monthly_maintenance": monthly_maint if use_dynamic_capex else 0, # Store 0 if not dynamic
        "capex_percent": capex_pct, "adjusted_capex_percent": adj_capex_pct if use_dynamic_capex else None,
        "monthly_capex": monthly_capex_val if use_dynamic_capex else 0, # Store 0 if not dynamic
        "capex_reserve_details": capex_details, "utilities_monthly": utilities_monthly if use_dynamic_capex else 0, # Store 0 if not dynamic
        "total_monthly_expenses": total_monthly_exp, "net_monthly_cashflow": net_monthly_cashflow,
        "annual_cashflow": annual_cashflow, "cash_on_cash_roi": coc_roi, "annual_noi": annual_noi, "cap_rate": cap_rate,
        "property_age": prop_age, "property_condition": prop_cond, "square_feet": sq_ft, "use_dynamic_capex": use_dynamic_capex
    }

# --- Appreciation Specific Functions ---

def fetch_denver_appreciation_data(neighborhood=None, verbose=False):
    # This is a mock function. In a real scenario, fetch from a live API or updated CSV.
    # For now, it returns pre-defined mock data.
    mock_csv_data = """neighborhood,2020,2021,2022,2023,2024,5yr_avg
Five Points,5.2,6.1,8.4,7.5,6.8,6.8
Highland,4.9,5.8,7.9,6.8,5.6,6.2
Cherry Creek,4.5,5.5,7.1,6.3,6.1,5.9
Wash Park,5.1,6.2,8.0,7.1,6.1,6.5
Stapleton,4.6,5.4,7.2,6.2,5.1,5.7
LoDo,4.8,5.7,7.5,6.4,5.6,6.0
Downtown,4.7,5.6,7.3,6.2,5.3,5.8
Capitol Hill,4.5,5.3,7.0,6.0,5.2,5.6
Baker,4.4,5.2,6.9,5.9,5.1,5.5
City Park,4.9,5.8,7.6,6.5,5.7,6.1
"""
    try:
        csv_file = StringIO(mock_csv_data)
        reader = csv.DictReader(csv_file)
        app_data = {}
        for row in reader:
            name = row['neighborhood'].lower().replace(' ', '_')
            app_data[name] = {
                'annual_rates': [float(row[year]) for year in ['2020', '2021', '2022', '2023', '2024']],
                '5yr_avg': float(row['5yr_avg'])
            }
        
        if neighborhood:
            neighborhood_key = neighborhood.lower().replace(' ', '_')
            # Fallback to a common neighborhood if the specific one isn't in mock data
            return app_data.get(neighborhood_key, app_data.get('five_points')) 
        return app_data # Or return all data if no specific neighborhood
    except Exception as e:
        if verbose: print(f"Warning: Could not process mock appreciation data: {e}", file=sys.stderr)
        return None

def fetch_historical_appreciation_metric(
    neighborhood_name, 
    city_name, 
    metric_to_fetch, 
    db_path, 
    verbose=False
):
    """
    Fetches a specific historical appreciation metric from the neighborhood_analysis.db.
    """
    if not metric_to_fetch or not neighborhood_name:
        if verbose: print(f"DEBUG (Historical): Metric name or neighborhood name not provided. Cannot fetch.", flush=True)
        return None

    conn_hist = None
    try:
        conn_hist = sqlite3.connect(db_path, timeout=10)
        cursor_hist = conn_hist.cursor()

        # Validate metric_to_fetch to prevent SQL injection if it comes from less trusted source
        # For now, assuming it's controlled. If it could be arbitrary user input, more validation needed.
        # Example: Check against a list of known valid column names.
        valid_metrics = [
            "median_sale_price_ptp_appreciation", "median_ppsf_ptp_appreciation",
            "median_sale_price_quarterly_appreciation", "median_ppsf_quarterly_appreciation",
            "median_sale_price_annual_appreciation", "median_ppsf_annual_appreciation",
            "median_sale_price_3_year_cagr_appreciation", "median_ppsf_3_year_cagr_appreciation",
            "median_sale_price_5_year_cagr_appreciation", "median_ppsf_5_year_cagr_appreciation",
            "median_sale_price_10_year_cagr_appreciation", "median_ppsf_10_year_cagr_appreciation"
        ]
        if metric_to_fetch not in valid_metrics:
            if verbose: print(f"DEBUG (Historical): Invalid metric_to_fetch: {metric_to_fetch}. Not in allowed list.", flush=True)
            return None

        # Base query: Select the metric from neighborhood_appreciation
        # Join with neighborhood_data to filter by neighborhood_name, city, property_type, and homes_sold
        # Order by period_end descending to get the latest metric
        query = f"""
            SELECT na.value
            FROM neighborhood_appreciation na
            JOIN neighborhood_data nd ON na.neighborhood_data_id = nd.id
            WHERE 
                na.metric_type = ? 
                AND nd.property_type = 'Single Family Residential'
                AND nd.homes_sold >= ?
        """
        params = [metric_to_fetch, MIN_HOMES_SOLD_THRESHOLD_HISTORICAL]

        # Flexible neighborhood matching:
        # Try to match "Denver, CO - Sloan Lake" style first, then just "Sloan Lake"
        # The neighborhood_name from cashflow analyzer might be "sloan_lake" or "Sloan Lake"
        # The region in DB might be "Denver, CO - Sloan Lake" or "Sloan Lake"
        # The neighborhood_name in DB is usually the pure name like "Sloan Lake"
        
        # Normalize neighborhood_name from input (e.g. "sloan_lake" -> "sloan lake")
        normalized_neighborhood_input = neighborhood_name.lower().replace('_', ' ')

        # Query attempts:
        # 1. Exact match on nd.neighborhood_name (normalized) and city if provided
        # 2. LIKE match on nd.neighborhood_name (normalized) and city if provided
        # 3. Exact match on nd.region (normalized input) if city NOT provided (region might contain city)
        # 4. LIKE match on nd.region (normalized input) if city NOT provided
        # 5. LIKE match on nd.neighborhood_name (normalized) if city NOT provided (broader)

        if city_name:
            query += " AND lower(nd.city) = ? AND lower(nd.neighborhood_name) = ?"
            params.extend([city_name.lower(), normalized_neighborhood_input])
        else:
            # If no city, try matching the input against neighborhood_name or region
            # This allows for inputs like "Sloan Lake" or "Denver, CO - Sloan Lake" directly
            # Corrected: If no city, only match against neighborhood_name as region column is not in nd table as queried.
            query += " AND lower(nd.neighborhood_name) = ?"
            params.append(normalized_neighborhood_input)
        
        query += f" ORDER BY nd.period_end DESC LIMIT 1"


        if verbose: print(f"DEBUG (Historical): Querying historical DB: {query} with params {params}", flush=True)
        cursor_hist.execute(query, tuple(params))
        result = cursor_hist.fetchone()

        if result and result[0] is not None:
            if verbose: print(f"DEBUG (Historical): Found historical metric '{metric_to_fetch}' for '{neighborhood_name}' (City: {city_name}): {result[0]}", flush=True)
            return float(result[0]) # Return the raw decimal value from DB
        else:
            # Try a broader LIKE match if the specific one failed
            query_like = f"""
                SELECT na.value
                FROM neighborhood_appreciation na
                JOIN neighborhood_data nd ON na.neighborhood_data_id = nd.id
                WHERE 
                    na.metric_type = ? 
                    AND nd.property_type = 'Single Family Residential'
                    AND nd.homes_sold >= ?
            """
            params_like = [metric_to_fetch, MIN_HOMES_SOLD_THRESHOLD_HISTORICAL]

            if city_name:
                query_like += " AND lower(nd.city) = ? AND lower(nd.neighborhood_name) LIKE ?"
                params_like.extend([city_name.lower(), f"%{normalized_neighborhood_input}%"])
            else:
                # Corrected: If no city, only match against neighborhood_name with LIKE
                query_like += " AND lower(nd.neighborhood_name) LIKE ?"
                params_like.append(f"%{normalized_neighborhood_input}%")
            
            query_like += f" ORDER BY nd.period_end DESC LIMIT 1"
            
            if verbose: print(f"DEBUG (Historical): Retrying with LIKE query: {query_like} with params {params_like}", flush=True)
            cursor_hist.execute(query_like, tuple(params_like))
            result_like = cursor_hist.fetchone()

            if result_like and result_like[0] is not None:
                 if verbose: print(f"DEBUG (Historical): Found historical metric (LIKE match) '{metric_to_fetch}' for '{neighborhood_name}' (City: {city_name}): {result_like[0]}", flush=True)
                 return float(result_like[0]) # Return the raw decimal value from DB
            else:
                if verbose: print(f"DEBUG (Historical): No historical metric found for '{neighborhood_name}' (City: {city_name}, Metric: {metric_to_fetch}) after all attempts.", flush=True)
                return None

    except sqlite3.Error as e:
        if verbose: print(f"SQLite error when fetching historical appreciation for '{neighborhood_name}': {e}", file=sys.stderr)
        return None
    except Exception as e:
        if verbose: print(f"General error when fetching historical appreciation for '{neighborhood_name}': {e}", file=sys.stderr)
        return None
    finally:
        if conn_hist:
            conn_hist.close()

def calculate_appreciation_returns(
    financials, # Expects the dictionary from calculate_financial_components
    investment_horizon,
    manual_appreciation_rate=None, 
    neighborhood_name=None, 
    fetch_real_data_flag=False, # This is the boolean value of the flag
    neighborhood_appreciation_config=None, # This is the data from config.json
    # New args for historical data
    use_historical_metric_name=None,
    historical_db_path=None,
    target_city_for_historical=None,
    verbose=False
):
    purchase_price = financials["purchase_price"]
    down_payment_amount = financials["down_payment_amount"]
    loan_amount = financials["loan_amount"]
    annual_interest_rate_percent = financials["annual_interest_rate_percent"]
    loan_term_years = financials["loan_term_years"]
    annual_cashflow = financials["annual_cashflow"]

    eff_app_rate = None  # Effective annual appreciation rate in percent (e.g., 5.0 for 5%)
    market_outlook = "N/A"
    source_of_data_message = "N/A"

    # Step 1: Try Historical DB if fetch_real_data_flag is True
    historical_metric_value_raw = None # This will be the direct value from DB, e.g., 0.06069
    if fetch_real_data_flag and use_historical_metric_name and historical_db_path and target_city_for_historical:
        if verbose: print(f"DEBUG: Attempting to fetch historical metric '{use_historical_metric_name}' for neighborhood '{neighborhood_name}' (City: {target_city_for_historical}) from DB: {historical_db_path}", flush=True)
        historical_metric_value_raw = fetch_historical_appreciation_metric(
            neighborhood_name=neighborhood_name,
            city_name=target_city_for_historical,
            metric_to_fetch=use_historical_metric_name,
            db_path=historical_db_path,
            verbose=verbose
        )
        if historical_metric_value_raw is not None:
            eff_app_rate = historical_metric_value_raw # The value from DB is already a percentage (e.g., 6.069)
            market_outlook = "historical_db" 
            source_of_data_message = f"Historical DB ({use_historical_metric_name})"
            if verbose: print(f"DEBUG: Using HISTORICAL DB rate: {eff_app_rate:.2f}%. Outlook: {market_outlook}. Source: {source_of_data_message}", flush=True)
        elif verbose:
            print(f"DEBUG: Historical metric '{use_historical_metric_name}' not found for '{neighborhood_name}' (City: {target_city_for_historical}). Will check JSON/default.", flush=True)

    # Step 2: If Historical not used OR not found, try JSON config data
    # This logic applies if fetch_real_data_flag was False, OR if it was True but no historical_metric_value_raw was found.
    if eff_app_rate is None:
        if verbose: print(f"DEBUG: Historical rate not applied. Checking JSON config for neighborhood '{neighborhood_name}'. fetch_real_data_flag was {fetch_real_data_flag}.", flush=True)
        if neighborhood_appreciation_config and neighborhood_name:
            # Try exact match first
            hood_data = neighborhood_appreciation_config.get(neighborhood_name)
            if not hood_data and '_' in neighborhood_name: # try replacing underscore with space or vice-versa if common pattern
                hood_data = neighborhood_appreciation_config.get(neighborhood_name.replace('_', ' '))
            if not hood_data and ' ' in neighborhood_name:
                 hood_data = neighborhood_appreciation_config.get(neighborhood_name.replace(' ', '_'))

            if hood_data:
                json_appr_rate = hood_data.get("historical_appreciation")
                if json_appr_rate is not None:
                    try:
                        eff_app_rate = float(json_appr_rate)
                        market_outlook = hood_data.get("long_term_outlook", "N/A (from JSON)")
                        source_of_data_message = f"JSON Config ('{neighborhood_name}')"
                        if verbose: print(f"DEBUG: Using JSON config for '{neighborhood_name}': Appr: {eff_app_rate:.2f}%, Outlook: {market_outlook}. Source: {source_of_data_message}", flush=True)
                    except ValueError:
                        if verbose: print(f"Warning: Could not parse 'historical_appreciation' from JSON for '{neighborhood_name}': {json_appr_rate}", flush=True)
                elif verbose:
                    print(f"DEBUG: Neighborhood '{neighborhood_name}' found in JSON, but no 'historical_appreciation' field.", flush=True)
            elif verbose:
                print(f"DEBUG: Neighborhood '{neighborhood_name}' not found in JSON config. Will check for a general default in JSON.", flush=True)

        # If specific neighborhood not in JSON or no rate, try the 'default' from JSON
        if eff_app_rate is None and neighborhood_appreciation_config:
            default_hood_data = neighborhood_appreciation_config.get("default")
            if default_hood_data:
                json_default_appr_rate = default_hood_data.get("historical_appreciation")
                if json_default_appr_rate is not None:
                    try:
                        eff_app_rate = float(json_default_appr_rate)
                        market_outlook = default_hood_data.get("long_term_outlook", "N/A (from JSON default)")
                        source_of_data_message = "JSON Config (default)"
                        if verbose: print(f"DEBUG: Using JSON config 'default': Appr: {eff_app_rate:.2f}%, Outlook: {market_outlook}. Source: {source_of_data_message}", flush=True)
                    except ValueError:
                        if verbose: print(f"Warning: Could not parse 'historical_appreciation' from JSON for 'default': {json_default_appr_rate}", flush=True)
                elif verbose:
                    print(f"DEBUG: JSON 'default' entry found, but no 'historical_appreciation' field.", flush=True)
            elif verbose:
                print(f"DEBUG: No 'default' entry found in JSON config's neighborhood_appreciation_data.", flush=True)
        elif eff_app_rate is None and verbose: # If still None and no neighborhood_appreciation_config
             print(f"DEBUG: No neighborhood_appreciation_config provided or processed. eff_app_rate remains None.", flush=True)


    # Step 3: Manual Override (Highest Precedence)
    # This applies *after* the above attempts. If manual_appreciation_rate is set, it wins.
    if manual_appreciation_rate is not None:
        eff_app_rate = manual_appreciation_rate # This is already a percentage
        market_outlook = "manual_override"
        source_of_data_message = "CLI Manual Rate Override"
        if verbose: print(f"DEBUG: Manually overriding appreciation rate to: {eff_app_rate:.2f}%. Outlook: {market_outlook}. Source: {source_of_data_message}", flush=True)
    
    # Step 4. Final Fallback if nothing else set eff_app_rate
    if eff_app_rate is None:
        # SCRIPT_DEFAULTS['appreciation_rate'] is None by default, so this won't trigger from there unless changed.
        # We might want a hardcoded ultimate fallback if SCRIPT_DEFAULTS['appreciation_rate'] could also be None.
        ultimate_fallback_rate = SCRIPT_DEFAULTS.get("appreciation_rate") # Check if script has a default
        if ultimate_fallback_rate is not None:
             eff_app_rate = ultimate_fallback_rate
             market_outlook = "script_default_fallback"
             source_of_data_message = "Script Default Fallback"
             if verbose: print(f"DEBUG: No appreciation rate found from historical, JSON, or CLI. Using SCRIPT_DEFAULTS['appreciation_rate']: {eff_app_rate:.2f}%. Source: {source_of_data_message}", flush=True)
        else:
            if verbose: print(f"DEBUG: No appreciation rate found from historical, JSON, CLI or SCRIPT_DEFAULTS. Using a final hardcoded default of 0.0%.", flush=True)
            eff_app_rate = 0.0 # Final hardcoded fallback
            market_outlook = "hardcoded_fallback"
            source_of_data_message = "Script Hardcoded Fallback (0.0%)"

    if verbose: print(f"INFO: Final effective appreciation rate: {eff_app_rate:.2f}%, Outlook: {market_outlook}, Source: {source_of_data_message}")

    future_val = purchase_price * ((1 + (eff_app_rate / 100)) ** investment_horizon)
    total_appr = future_val - purchase_price
    
    # Remaining loan balance
    monthly_payment = calculate_mortgage_payment(loan_amount, annual_interest_rate_percent, loan_term_years)
    payments_made = investment_horizon * 12
    num_total_payments = loan_term_years * 12
    
    remaining_balance = loan_amount # Start with full loan amount
    if loan_amount > 0 and monthly_payment > 0: # Ensure there is a loan to pay
        if payments_made >= num_total_payments:
            remaining_balance = 0 # Loan paid off
        else:
            # Correct remaining balance calculation using formula
            # R = P * ( (1+r)^n - (1+r)^p ) / ( (1+r)^n - 1 )
            # Where P=principal, r=monthly_rate, n=total_payments, p=payments_made
            monthly_rate = (annual_interest_rate_percent / 100) / 12
            if monthly_rate > 0 :
                remaining_balance = loan_amount * \
                                (( (1 + monthly_rate)**num_total_payments ) - ( (1 + monthly_rate)**payments_made) ) / \
                                ( ( (1 + monthly_rate)**num_total_payments ) - 1 ) if (( (1 + monthly_rate)**num_total_payments ) - 1) != 0 else 0
            else: # 0% interest rate
                 remaining_balance = loan_amount - (monthly_payment * payments_made)
                 remaining_balance = max(0, remaining_balance) # Cannot be negative

    equity_from_mortgage_paydown = loan_amount - remaining_balance
    total_equity_at_horizon = down_payment_amount + equity_from_mortgage_paydown + total_appr
    total_cashflow_over_horizon = annual_cashflow * investment_horizon
    total_profit = total_equity_at_horizon - down_payment_amount + total_cashflow_over_horizon
    
    total_roi_pct = (total_profit / down_payment_amount) * 100 if down_payment_amount > 0 else 0
    annualized_roi = 0
    if down_payment_amount > 0 and investment_horizon > 0:
        # ( (End Value / Start Value) ^ (1/Years) ) - 1
        # End Value = Initial Equity + Total Profit
        # Start Value = Initial Equity
        annualized_roi = (((down_payment_amount + total_profit) / down_payment_amount) ** (1 / investment_horizon) - 1) * 100 if down_payment_amount + total_profit > 0 else 0


    return {
        "purchase_price": purchase_price, "future_value": future_val, "total_appreciation": total_appr,
        "appreciation_percent_total": (total_appr / purchase_price) * 100 if purchase_price > 0 else 0,
        "annual_appreciation_rate_used": eff_app_rate,
        "equity_from_mortgage_paydown": equity_from_mortgage_paydown,
        "remaining_loan_balance": remaining_balance,
        "total_cashflow_over_horizon": total_cashflow_over_horizon,
        "total_profit": total_profit, "total_roi_percent_on_equity": total_roi_pct,
        "annualized_roi_on_equity": annualized_roi,
        "initial_equity": down_payment_amount, "total_equity_at_horizon": total_equity_at_horizon,
        "market_outlook_assessment": market_outlook, # USE THE RESOLVED market_outlook
        "investment_horizon_years": investment_horizon,
        "source_of_appreciation_data": source_of_data_message, # For transparency
        "use_historical_metric_name": use_historical_metric_name,
        "historical_db_path": historical_db_path,
        "target_city_for_historical": target_city_for_historical
    }

# --- Output Formatting Helpers (from modified_cashflow_analyzer.py) ---
def hr(char='=', length=80): return char * length
def section_title(title, char='='):
    padding = (80 - len(title) - 4) // 2
    padding = max(0, padding) # Ensure padding isn't negative
    return f"\n{char * padding} {title.upper()} {char * padding}"

def format_currency(amount): return f"${amount:,.2f}" if amount is not None else "$N/A"
def format_percent(amount): return f"{amount:.2f}%" if amount is not None else "N/A"
def format_label_value(label, value, width=35): return f"{label:<{width}} {value}"

def print_capex_guide(args): # Now expects args for verbose
    if args.verbose: print("DEBUG: Entering print_capex_guide function...", flush=True)
    print(section_title("CAPEX COMPONENTS REFERENCE GUIDE", "-"))
    print("This guide shows typical CapEx components, default lifespans, and costs.")
    print("Values are adjusted by property age/condition in dynamic analysis.")
    print(hr("-"))
    # ... (rest of capex guide, simplified for brevity here, ensure it's complete in actual file)
    print(f"{'Component':<20} {'Typical Lifespan':<20} {'Cost Basis':<30}")
    print("-" * 80)
    for comp, details in CAPEX_COMPONENTS.items():
        name = comp.replace('_', ' ').title()
        lifespan = f"{details['lifespan']} years"
        cost_basis = f"${details.get('cost_per_sqft',0):.2f}/sqft + ${details.get('cost_base',0):.2f}" if "cost_per_sqft" in details else f"${details.get('cost_base',0):.2f} base"
        print(f"{name:<20} {lifespan:<20} {cost_basis:<30}")
    print(hr("-"))
    if args.verbose: print("DEBUG: Exiting print_capex_guide function...", flush=True)

# --- Main Calculation and Printing Logic ---
def run_analysis_and_print(args_dict, property_data, neighborhood_data_from_config, effective_neighborhood_name_for_analysis):
    # args_dict is now a dictionary
    if args_dict.get('verbose'): print(f"DEBUG: Running analysis for property: {property_data}", flush=True)
    if args_dict.get('verbose'): print(f"DEBUG: Neighborhood appreciation data being used (full config map): {neighborhood_data_from_config}", flush=True)
    if args_dict.get('verbose'): print(f"DEBUG: Effective neighborhood name for this analysis: {effective_neighborhood_name_for_analysis}", flush=True)

    # Determine actual sq_ft and prop_age (DB > CLI/Config > Default)
    actual_sq_ft = args_dict.get('square_feet')
    if property_data.get("sqft") is not None: actual_sq_ft = property_data["sqft"]
    elif args_dict.get('verbose'): print(f"DEBUG: Using arg/config for sqft: {actual_sq_ft}", flush=True)
    
    actual_prop_age = args_dict.get('property_age')
    if property_data.get("calculated_property_age") is not None: actual_prop_age = property_data["calculated_property_age"]
    elif args_dict.get('verbose'): print(f"DEBUG: Using arg/config for age: {actual_prop_age} (DB year: {property_data.get('year_built_raw')})", flush=True)

    financials = calculate_financial_components(
        purchase_price=property_data["price"],
        tax_info_raw=property_data["tax_information_raw"],
        est_monthly_rent=property_data["estimated_rent_raw"],
        down_payment_dollars=args_dict.get('down_payment'),
        annual_rate_percent=args_dict.get('rate'),
        loan_term_years=args_dict.get('loan_term'),
        annual_insurance=args_dict.get('insurance'),
        misc_monthly=args_dict.get('misc_monthly'),
        vacancy_rate_pct=args_dict.get('vacancy_rate'),
        property_mgmt_fee_pct=args_dict.get('property_mgmt_fee'),
        maintenance_pct=args_dict.get('maintenance_percent'),
        capex_pct=args_dict.get('capex_percent'),
        utilities_monthly=args_dict.get('utilities_monthly'),
        use_dynamic_capex=args_dict.get('use_dynamic_capex'),
        prop_age=actual_prop_age,
        prop_cond=args_dict.get('property_condition'),
        sq_ft=actual_sq_ft,
        verbose=args_dict.get('verbose')
    )

    if not financials:
        print("Critical Error: Could not calculate core financial components. Exiting.", file=sys.stderr)
        return

    appreciation_returns = calculate_appreciation_returns(
        financials=financials,
        investment_horizon=args_dict.get('investment_horizon'),
        manual_appreciation_rate=args_dict.get('appreciation_rate'), 
        neighborhood_name=effective_neighborhood_name_for_analysis,
        fetch_real_data_flag=args_dict.get('fetch_real_appreciation'),
        neighborhood_appreciation_config=neighborhood_data_from_config,
        use_historical_metric_name=args_dict.get('use_historical_metric'),
        historical_db_path=args_dict.get('neighborhood_analysis_db_path'),
        target_city_for_historical=args_dict.get('target_city_for_historical'), 
        verbose=args_dict.get('verbose')
    )
    
    # --- Printing The Report ---
    use_color = sys.stdout.isatty()
    pos_color, neg_color, bold, end_color = ('\033[92m', '\033[91m', '\033[1m', '\033[0m') if use_color else ('','','','')

    def colorize(text, color): return f"{color}{text}{end_color}" if use_color else text
    def f_curr_color(amount):
        val = format_currency(amount)
        if amount is None: return val
        return colorize(val, pos_color if amount > 0 else (neg_color if amount < 0 else ''))

    print(hr("="))
    print(colorize(f"REAL ESTATE INVESTMENT ANALYSIS: {args_dict.get('address')}", bold))
    print(f"Analysis Date: {datetime.datetime.now().strftime('%B %d, %Y')}")
    print(hr("="))

    # Property & Loan Details (using .get for safety with dict)
    print(section_title("Property & Loan Details", "-"))
    print(format_label_value("Purchase Price:", format_currency(financials["purchase_price"])))
    print(format_label_value("Square Footage:", f"{financials['square_feet']:.0f} sq ft" if financials['square_feet'] else "N/A"))
    print(format_label_value("Property Age:", f"{financials['property_age']} years" if financials['property_age'] is not None else "N/A"))
    print(format_label_value("Property Condition:", financials['property_condition'].upper()))
    print(format_label_value("Down Payment:", f"{format_currency(financials['down_payment_amount'])} ({format_percent(financials['down_payment_percentage'])})"))
    print(format_label_value("Loan Amount:", format_currency(financials['loan_amount'])))
    print(format_label_value("Interest Rate:", format_percent(financials['annual_interest_rate_percent'])))
    print(format_label_value("Loan Term:", f"{financials['loan_term_years']} years"))

    # Monthly Cashflow Analysis
    print(section_title("Monthly Cashflow Analysis", "-"))
    print(format_label_value("Gross Monthly Rent:", format_currency(financials["estimated_monthly_rent"])))
    if args_dict.get('use_dynamic_capex'):
        print(format_label_value("Vacancy Loss:", f"{format_currency(financials['estimated_monthly_rent'] - financials['effective_rent_after_vacancy'])} ({format_percent(financials['vacancy_rate_percent'])})"))
        print(format_label_value("Effective Monthly Income:", format_currency(financials['effective_rent_after_vacancy'])))
    
    print(format_label_value("Mortgage (P&I):", format_currency(financials["monthly_p_and_i"])))
    tax_warn = "" if financials['annual_taxes'] is not None else " (Could not parse)"
    print(format_label_value("Property Taxes:", f"{format_currency(financials['monthly_taxes'])}{tax_warn}"))
    print(format_label_value("Insurance:", format_currency(financials['monthly_insurance'])))
    
    if args_dict.get('use_dynamic_capex'):
        print(format_label_value("Property Management:", f"{format_currency(financials['monthly_property_mgmt'])} ({format_percent(financials['property_mgmt_fee_percent'])})"))
        print(format_label_value("Maintenance Reserve:", f"{format_currency(financials['monthly_maintenance'])} ({format_percent(financials['adjusted_maintenance_percent'])} annual)"))
        print(format_label_value("CapEx Reserve:", f"{format_currency(financials['monthly_capex'])} ({format_percent(financials['adjusted_capex_percent'])} of value)"))
        print(format_label_value("Utilities (Landlord):", format_currency(financials['utilities_monthly'])))
    
    print(format_label_value("Misc. Monthly Costs:", format_currency(financials['misc_monthly_cost'])))
    print(hr("-", 40))
    print(format_label_value("Total Monthly Expenses:", format_currency(financials['total_monthly_expenses'])))
    print(hr("-", 40))
    print(format_label_value(f"{bold}Net Monthly Cashflow:{end_color}", f_curr_color(financials['net_monthly_cashflow'])))
    print(format_label_value(f"{bold}Annual Cashflow:{end_color}", f_curr_color(financials['annual_cashflow'])))
    print(format_label_value(f"{bold}Cash-on-Cash ROI:{end_color}", format_percent(financials['cash_on_cash_roi'])))
    if args_dict.get('use_dynamic_capex') and financials.get('cap_rate') is not None:
        print(format_label_value("Cap Rate (NOI Based):", format_percent(financials['cap_rate'])))

    # Long-Term Investment & Appreciation Analysis
    print(section_title(f"Long-Term Projection ({args_dict.get('investment_horizon')} Years)", "-"))
    print(format_label_value("Investment Horizon:", f"{appreciation_returns['investment_horizon_years']} years"))
    print(format_label_value("Annual Appreciation Rate:", f"{format_percent(appreciation_returns['annual_appreciation_rate_used'])} (Market: {appreciation_returns['market_outlook_assessment']}, Source: {appreciation_returns['source_of_appreciation_data']})"))
    print(format_label_value("Est. Future Property Value:", format_currency(appreciation_returns['future_value'])))
    print(format_label_value("Total Property Appreciation:", format_currency(appreciation_returns['total_appreciation'])))
    print(format_label_value("Equity from Paydown:", format_currency(appreciation_returns['equity_from_mortgage_paydown'])))
    print(format_label_value("Remaining Loan Balance:", format_currency(appreciation_returns['remaining_loan_balance'])))
    print(format_label_value("Total Equity at Horizon:", format_currency(appreciation_returns['total_equity_at_horizon'])))
    print(format_label_value("Total Cashflow during Horizon:", format_currency(appreciation_returns['total_cashflow_over_horizon'])))
    print(hr("-", 40))
    print(format_label_value(f"{bold}Total Estimated Profit:{end_color}", f_curr_color(appreciation_returns['total_profit'])))
    print(format_label_value(f"{bold}Total ROI (on initial equity):{end_color}", format_percent(appreciation_returns['total_roi_percent_on_equity'])))
    print(format_label_value(f"{bold}Annualized ROI (on equity):{end_color}", format_percent(appreciation_returns['annualized_roi_on_equity'])))
    
    if args_dict.get('use_dynamic_capex') and financials.get("capex_reserve_details"):
        print(section_title("Detailed CapEx Breakdown (Dynamic Mode)", "-"))
        details = financials["capex_reserve_details"]["components"]
        col_comp, col_cost, col_life, col_month = 24, 18, 12, 18
        header = f"{'Component':<{col_comp}} {'Repl. Cost':>{col_cost}} {'Lifespan':>{col_life}} {'Monthly Res.':>{col_month}}"
        print(header)
        print(hr('-', 80))
        for comp, det in sorted(details.items()):
            name = comp.replace('_', ' ').title()
            cost_s = format_currency(det['replacement_cost'])
            life_s = f"{det['lifespan_years']:.1f} yrs"
            month_s = format_currency(det['monthly_reserve'])
            print(f"{name:<{col_comp}} {cost_s:>{col_cost}} {life_s:>{col_life}} {month_s:>{col_month}}")
        print(hr('-', 80))
        print(format_label_value("Total Monthly CapEx Reserve:", format_currency(financials['monthly_capex'])))

    print(section_title("Deal Analysis & Summary", "-"))

    def score_cashflow(cf_monthly):
        if cf_monthly > 300: return 2.5, "Excellent"
        if cf_monthly > 100: return 1.5, "Good"
        if cf_monthly > 0: return 0.5, "Fair"
        if cf_monthly == 0: return 0.0, "Neutral"
        if cf_monthly > -100: return -0.5, "Poor"
        if cf_monthly > -300: return -1.5, "Very Poor"
        return -2.5, "Extremely Poor"

    def score_coc_roi(coc):
        if coc > 12: return 2.5, "Excellent"
        if coc > 8: return 1.5, "Good"
        if coc > 5: return 0.5, "Fair"
        if coc > 2: return 0.0, "Neutral"
        if coc >= 0 : return -0.5, "Poor"
        return -1.5, "Very Poor"

    def score_cap_rate(cap, is_dynamic_capex):
        if not is_dynamic_capex or cap is None: return 0.0, "N/A (Dynamic CapEx off or N/A)"
        if cap > 7: return 2.0, "Excellent"
        if cap > 5.5: return 1.0, "Good"
        if cap > 4: return 0.0, "Fair"
        if cap > 2.5: return -1.0, "Poor"
        return -2.0, "Very Poor"

    def score_annualized_total_roi(annual_roi):
        if annual_roi > 15: return 2.0, "Excellent"
        if annual_roi > 10: return 1.0, "Good"
        if annual_roi > 7: return 0.5, "Fair"
        if annual_roi > 4: return 0.0, "Neutral"
        if annual_roi >= 0: return -0.5, "Poor"
        return -1.0, "Very Poor"

    overall_score = 0
    summary_lines = []

    cf_score, cf_rating = score_cashflow(financials['net_monthly_cashflow'])
    overall_score += cf_score
    print(format_label_value("Net Monthly Cashflow:", f"{f_curr_color(financials['net_monthly_cashflow'])} (Rating: {cf_rating}, Score: {cf_score})"))
    summary_lines.append(f"Net Monthly Cashflow rating: {cf_rating.lower()}")

    coc_score, coc_rating = score_coc_roi(financials['cash_on_cash_roi'])
    overall_score += coc_score
    print(format_label_value("Cash-on-Cash ROI:", f"{format_percent(financials['cash_on_cash_roi'])} (Rating: {coc_rating}, Score: {coc_score})"))
    summary_lines.append(f"Cash-on-Cash ROI rating: {coc_rating.lower()}")

    cap_score, cap_rating = score_cap_rate(financials.get('cap_rate'), args_dict.get('use_dynamic_capex'))
    overall_score += cap_score
    print(format_label_value("Cap Rate (NOI Based):", f"{format_percent(financials.get('cap_rate'))} (Rating: {cap_rating}, Score: {cap_score})"))
    summary_lines.append(f"Cap Rate rating: {cap_rating.lower()}")

    annual_roi_score, annual_roi_rating = score_annualized_total_roi(appreciation_returns['annualized_roi_on_equity'])
    overall_score += annual_roi_score
    print(format_label_value("Annualized Total ROI (Equity):", f"{format_percent(appreciation_returns['annualized_roi_on_equity'])} (Score: {annual_roi_score})")) # Rating not printed here for space
    summary_lines.append(f"long-term total returns rated: {annual_roi_rating.lower()}")

    # Normalize overall_score to a 0-10 scale (assuming max positive score ~8, min score ~-8)
    # This is a rough normalization, can be refined.
    # Max possible score: 2.5 (CF) + 2.5 (CoC) + 2.0 (Cap) + 2.0 (AnnualROI) = 9.0
    # Min possible score: -2.5 - 1.5 - 2.0 - 1.0 = -7.0
    # Range is 16. Let's map -7 to 0 and 9 to 10.
    normalized_score = ((overall_score - (-7)) / (9 - (-7))) * 10 if (9 - (-7)) != 0 else 0
    normalized_score = max(0, min(10, normalized_score)) # Clamp between 0 and 10

    overall_rating = "Poor Investment Prospect"
    if normalized_score >= 8.5: overall_rating = "Excellent Investment Prospect!"
    elif normalized_score >= 6.5: overall_rating = "Good Investment Prospect"
    elif normalized_score >= 4.0: overall_rating = "Fair Investment Prospect, Potential Upsides"
    elif normalized_score >= 2.0: overall_rating = "Marginal Investment, Consider Carefully"
    
    print(hr("-", 40))
    print(format_label_value(f"{bold}Overall Investment Score:{end_color}", f"{normalized_score:.1f}/10 ({overall_rating})"))
    print(hr("-", 40))

    # New summary block
    print() # Add a blank line for spacing
    print(f"{bold}Key Performance Indicators:{end_color}")
    for line_text in summary_lines:
        # Example line_text: "Net Monthly Cashflow rating: excellent"
        # Or: "Cap Rate rating: n/a (dynamic capex off or n/a)"
        # Or: "long-term total returns rated: good"
        cleaned_text = line_text.replace(" rating: ", ": ").replace(" rated: ", ": ")
        
        parts = cleaned_text.split(': ')
        if len(parts) == 2:
            indicator_display = parts[0].capitalize()
            value_display = parts[1]
            # Special handling for "N/A" to ensure it's uppercase, then capitalize rest or keep as is
            if value_display.lower().startswith("n/a"):
                value_display = "N/A" + value_display[3:] # Preserve details after "n/a"
            else:
                value_display = value_display.capitalize() # Capitalize ratings like "excellent"
            print(f"  - {indicator_display}: {value_display}")
        else:
            # Fallback if splitting failed (should not happen with current summary_lines structure)
            print(f"  - {cleaned_text.capitalize()}")
    
    print(hr("="))
    if args_dict.get('verbose'): print("DEBUG: Analysis printing complete.", flush=True)


# --- Main Function Definition ---
def main():
    # Initial load of config to pass to argparse for its internal defaults for --config-path
    temp_parser = argparse.ArgumentParser(add_help=False)
    temp_parser.add_argument("--config-path", type=Path, default=DEFAULT_CONFIG_PATH, help="Path to the JSON config file.")
    temp_args, _ = temp_parser.parse_known_args()

    config = load_config(temp_args.config_path)
    args = parse_arguments(config) 

    if args.verbose:
        print("--- Initial Arguments & Config ---")
        print(f"Using config file: {temp_args.config_path}")
        # Conditional print of full config for debugging, can be large
        # if os.environ.get("DEBUG_CONFIG") == "true":
        #    print(f"Loaded config: {json.dumps(config, indent=2)}")
        # else:
        print(f"Loaded config: {{keys: {list(config.keys())}}}") # Print only keys for brevity
        print(f"Arguments after parsing (Config > CLI > ScriptDefault): {vars(args)}")

    if args.capex_guide:
        print_capex_guide(args)
        return

    property_data = fetch_property_data(args.db_path, args.address, args.verbose)
    if not property_data:
        print(f"Error: Property with address '{args.address}' not found in {args.db_path}", file=sys.stderr)
        return

    city_for_historical_lookup = property_data.get("city")
    if not city_for_historical_lookup and args.target_city_for_historical:
        city_for_historical_lookup = args.target_city_for_historical
    
    if args.verbose and city_for_historical_lookup:
        source_city_msg = "from listings.db" if property_data.get("city") else "from CLI argument"
        print(f"Info: Using target city '{city_for_historical_lookup}' {source_city_msg} for historical lookup.")
    elif args.verbose and not city_for_historical_lookup and args.use_historical_metric:
        print(f"Warning: Historical metric lookup is enabled but no target city determined. Lookup may fail.")

    neighborhood_appreciation_data_from_config = config.get("neighborhood_appreciation_data", {})
    zip_to_neighborhood_mapping = config.get("zip_to_neighborhood_mapping", {})
    effective_neighborhood_name_for_analysis = args.neighborhood

    if not effective_neighborhood_name_for_analysis:
        db_zip = property_data.get("zip")
        if db_zip:
            inferred_neighborhood_key = zip_to_neighborhood_mapping.get(str(db_zip))
            if inferred_neighborhood_key:
                effective_neighborhood_name_for_analysis = inferred_neighborhood_key
                if args.verbose: print(f"Info: Inferred neighborhood '{effective_neighborhood_name_for_analysis}' from ZIP '{db_zip}'.")
            elif args.verbose: print(f"Warning: ZIP '{db_zip}' not in zip_to_neighborhood_mapping.")
    
    if not effective_neighborhood_name_for_analysis:
        effective_neighborhood_name_for_analysis = config.get("neighborhood")
        if args.verbose and effective_neighborhood_name_for_analysis: print(f"Info: Using general neighborhood '{effective_neighborhood_name_for_analysis}' from config.")

    if not effective_neighborhood_name_for_analysis:
        effective_neighborhood_name_for_analysis = SCRIPT_DEFAULTS.get("neighborhood", "default")
        if args.verbose: print(f"Info: Using script default neighborhood: '{effective_neighborhood_name_for_analysis}'.")
    
    true_manual_cli_appreciation_rate = None 
    try:
        idx = sys.argv.index('--appreciation-rate')
        if idx + 1 < len(sys.argv) and not sys.argv[idx + 1].startswith('--'):
            true_manual_cli_appreciation_rate = args.appreciation_rate 
            if args.verbose: print(f"DEBUG: CLI override --appreciation-rate IS SET with value: {true_manual_cli_appreciation_rate}")
        elif args.verbose:
             print(f"DEBUG: CLI flag --appreciation-rate found but no value followed. Not an override.")
    except ValueError:
        if args.verbose: print(f"DEBUG: CLI override --appreciation-rate IS NOT SET in sys.argv.")

    analysis_args_dict = vars(args).copy()
    analysis_args_dict['target_city_for_historical'] = city_for_historical_lookup
    analysis_args_dict['appreciation_rate'] = true_manual_cli_appreciation_rate

    run_analysis_and_print(
        args_dict=analysis_args_dict, 
        property_data=property_data,
        neighborhood_data_from_config=neighborhood_appreciation_data_from_config,
        effective_neighborhood_name_for_analysis=effective_neighborhood_name_for_analysis
    )

# --- Script Entry Point ---
if __name__ == "__main__":
    main()
