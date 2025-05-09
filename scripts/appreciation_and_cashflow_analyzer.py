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
        "appreciation_rate": None, # Explicitly None, to be set by CLI, neighborhood, or other logic
        "neighborhood": None,      # Explicitly None, to be auto-detected or set by CLI
        "investment_horizon": 5,
        "fetch_real_appreciation": False
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
    parser.add_argument("--neighborhood", type=str, default=get_default_val("neighborhood"), help="Manual neighborhood override. Auto-detected by ZIP if not set here or by CLI.")
    parser.add_argument("--investment-horizon", type=int, default=get_default_val("investment_horizon"), help="Investment holding period (years).")
    parser.add_argument("--fetch-real-appreciation", action=argparse.BooleanOptionalAction, default=get_default_val("fetch_real_appreciation"), help="Fetch real appreciation data.")
    
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
            "SELECT price, tax_information, estimated_rent, id, sqft, year_built, zip FROM listings WHERE address = ?",
            (address,)
        )
        row = cursor.fetchone()
        if row:
            db_price, db_tax_info, db_rent_raw, db_id, db_sqft_raw, db_year_built_raw, db_zip = row
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
                "zip": db_zip
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


def calculate_appreciation_returns(
    financials, # Expects the dictionary from calculate_financial_components
    investment_horizon,
    manual_appreciation_rate=None, # Renamed for clarity
    neighborhood_name=None, # Renamed for clarity
    fetch_real_data_flag=False, # Renamed for clarity
    neighborhood_appreciation_config=None, # New parameter for config data
    verbose=False
):
    purchase_price = financials["purchase_price"]
    loan_amount = financials["loan_amount"]
    annual_interest_rate_percent = financials["annual_interest_rate_percent"]
    loan_term_years = financials["loan_term_years"]
    annual_cashflow = financials["annual_cashflow"] 
    down_payment_amount = financials["down_payment_amount"]

    eff_app_rate = 5.0  # Default fallback if no other rate is determined
    market_outlook = "moderate" # Default fallback

    if fetch_real_data_flag:
        if verbose: print(f"DEBUG: --fetch-real-appreciation is TRUE. Using neighborhood data from config ('{neighborhood_appreciation_config is not None}') as the source.", flush=True)
        if neighborhood_name and neighborhood_appreciation_config:
            lookup_key = neighborhood_name.lower().replace(' ', '_')
            default_entry = neighborhood_appreciation_config.get("default", 
                                                               {"historical_appreciation": 5.0, "short_term_outlook": "moderate"})
            neighborhood_info = neighborhood_appreciation_config.get(lookup_key, default_entry)
            
            eff_app_rate = neighborhood_info.get("historical_appreciation", default_entry["historical_appreciation"])
            market_outlook = neighborhood_info.get("short_term_outlook", default_entry["short_term_outlook"])
            
            if verbose and lookup_key not in neighborhood_appreciation_config and lookup_key != "default":
                 print(f"Warning (fetch_real=True): Neighborhood '{neighborhood_name}' (key: '{lookup_key}') not in config data. Using default from config.", file=sys.stderr)
            if verbose: print(f"DEBUG (fetch_real=True): Using data from config for '{lookup_key}': Appr: {eff_app_rate:.2f}%, Outlook: {market_outlook}", flush=True)
        elif verbose:
            print(f"Warning (fetch_real=True): Neighborhood name or appreciation config data missing. Using fallback appreciation {eff_app_rate:.2f}%.", file=sys.stderr)

    elif neighborhood_name and neighborhood_appreciation_config: # This block is for when fetch_real_data_flag is FALSE
        if verbose: print(f"DEBUG: --fetch-real-appreciation is FALSE. Using neighborhood data from config.", flush=True)
        lookup_key = neighborhood_name.lower().replace(' ', '_')
        default_entry = neighborhood_appreciation_config.get("default", 
                                                           {"historical_appreciation": 5.0, "short_term_outlook": "moderate"})
        neighborhood_info = neighborhood_appreciation_config.get(lookup_key, default_entry)
        
        eff_app_rate = neighborhood_info.get("historical_appreciation", default_entry["historical_appreciation"])
        market_outlook = neighborhood_info.get("short_term_outlook", default_entry["short_term_outlook"])

        if verbose and lookup_key not in neighborhood_appreciation_config and lookup_key != "default":
             print(f"Warning (fetch_real=False): Neighborhood '{neighborhood_name}' (key: '{lookup_key}') not in config data. Using default from config.", file=sys.stderr)
        if verbose: print(f"DEBUG (fetch_real=False): Using data from config for '{lookup_key}': Appr: {eff_app_rate:.2f}%, Outlook: {market_outlook}", flush=True)
    elif verbose: # fetch_real_data_flag is False, and also no neighborhood_name or no neighborhood_appreciation_config
        print(f"Warning: No neighborhood data source. Using fallback appreciation {eff_app_rate:.2f}%.", file=sys.stderr)

    # Manual Override (Highest Precedence)
    if manual_appreciation_rate is not None:
        eff_app_rate = manual_appreciation_rate
        market_outlook = "manual_override" # Indicate that the rate was manually set
        if verbose: print(f"DEBUG: Manually overriding appreciation rate to: {eff_app_rate:.2f}% (Outlook: {market_outlook})", flush=True)

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
        "market_outlook_assessment": market_outlook, # Based on fetched or local data
        "investment_horizon_years": investment_horizon
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
def run_analysis_and_print(args, property_data, neighborhood_data_from_config, effective_neighborhood_name_for_analysis):
    if args.verbose: print(f"DEBUG: Running analysis for property: {property_data}", flush=True)
    if args.verbose: print(f"DEBUG: Neighborhood appreciation data being used (full config map): {neighborhood_data_from_config}", flush=True)
    if args.verbose: print(f"DEBUG: Effective neighborhood name for this analysis: {effective_neighborhood_name_for_analysis}", flush=True)

    # Determine actual sq_ft and prop_age (DB > CLI/Config > Default)
    actual_sq_ft = args.square_feet
    if property_data.get("sqft") is not None: actual_sq_ft = property_data["sqft"]
    elif args.verbose: print(f"DEBUG: Using arg/config for sqft: {actual_sq_ft}", flush=True)
    
    actual_prop_age = args.property_age
    if property_data.get("calculated_property_age") is not None: actual_prop_age = property_data["calculated_property_age"]
    elif args.verbose: print(f"DEBUG: Using arg/config for age: {actual_prop_age} (DB year: {property_data.get('year_built_raw')})", flush=True)

    financials = calculate_financial_components(
        purchase_price=property_data["price"],
        tax_info_raw=property_data["tax_information_raw"],
        est_monthly_rent=property_data["estimated_rent_raw"],
        down_payment_dollars=args.down_payment,
        annual_rate_percent=args.rate,
        loan_term_years=args.loan_term,
        annual_insurance=args.insurance,
        misc_monthly=args.misc_monthly,
        vacancy_rate_pct=args.vacancy_rate,
        property_mgmt_fee_pct=args.property_mgmt_fee,
        maintenance_pct=args.maintenance_percent,
        capex_pct=args.capex_percent,
        utilities_monthly=args.utilities_monthly,
        use_dynamic_capex=args.use_dynamic_capex,
        prop_age=actual_prop_age,
        prop_cond=args.property_condition,
        sq_ft=actual_sq_ft,
        verbose=args.verbose
    )

    if not financials:
        print("Critical Error: Could not calculate core financial components. Exiting.", file=sys.stderr)
        return

    appreciation_returns = calculate_appreciation_returns(
        financials=financials,
        investment_horizon=args.investment_horizon,
        manual_appreciation_rate=args.appreciation_rate,
        neighborhood_name=effective_neighborhood_name_for_analysis,
        fetch_real_data_flag=args.fetch_real_appreciation,
        neighborhood_appreciation_config=neighborhood_data_from_config,
        verbose=args.verbose
    )
    
    # --- Printing The Report ---
    # (Colorization helpers can be added here if desired, like in modified_cashflow_analyzer.py)
    use_color = sys.stdout.isatty()
    pos_color, neg_color, bold, end_color = ('\033[92m', '\033[91m', '\033[1m', '\033[0m') if use_color else ('','','','')

    def colorize(text, color): return f"{color}{text}{end_color}" if use_color else text
    def f_curr_color(amount):
        val = format_currency(amount)
        if amount is None: return val
        return colorize(val, pos_color if amount > 0 else (neg_color if amount < 0 else ''))


    print(hr("="))
    print(colorize(f"REAL ESTATE INVESTMENT ANALYSIS: {args.address}", bold))
    print(f"Analysis Date: {datetime.datetime.now().strftime('%B %d, %Y')}")
    print(hr("="))

    # Property & Loan Details
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
    if args.use_dynamic_capex:
        print(format_label_value("Vacancy Loss:", f"{format_currency(financials['estimated_monthly_rent'] - financials['effective_rent_after_vacancy'])} ({format_percent(financials['vacancy_rate_percent'])})"))
        print(format_label_value("Effective Monthly Income:", format_currency(financials['effective_rent_after_vacancy'])))
    
    print(format_label_value("Mortgage (P&I):", format_currency(financials["monthly_p_and_i"])))
    tax_warn = "" if financials['annual_taxes'] is not None else " (Could not parse)"
    print(format_label_value("Property Taxes:", f"{format_currency(financials['monthly_taxes'])}{tax_warn}"))
    print(format_label_value("Insurance:", format_currency(financials['monthly_insurance'])))
    
    if args.use_dynamic_capex:
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
    if args.use_dynamic_capex and financials.get('cap_rate') is not None:
        print(format_label_value("Cap Rate (NOI Based):", format_percent(financials['cap_rate'])))

    # Long-Term Investment & Appreciation Analysis
    print(section_title(f"Long-Term Projection ({args.investment_horizon} Years)", "-"))
    print(format_label_value("Investment Horizon:", f"{appreciation_returns['investment_horizon_years']} years"))
    print(format_label_value("Annual Appreciation Rate:", f"{format_percent(appreciation_returns['annual_appreciation_rate_used'])} (Market: {appreciation_returns['market_outlook_assessment']})"))
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
    
    # --- Detailed CapEx Breakdown (Printed BEFORE Deal Summary if applicable) ---
    if args.use_dynamic_capex and financials.get("capex_reserve_details"):
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

    # --- Deal Analysis & Summary (Now printed after CapEx details) ---
    print(section_title("Deal Analysis & Summary", "-"))

    # --- Scoring Logic ---
    def score_cashflow(cf_monthly):
        if cf_monthly > 300: return 2.5
        if cf_monthly > 100: return 1.5
        if cf_monthly > 0: return 0.5
        if cf_monthly == 0: return 0.0
        if cf_monthly > -100: return -0.5
        if cf_monthly > -300: return -1.5
        return -2.5

    def score_coc_roi(coc):
        if coc is None: return -1.0 # Penalize if not calculable (e.g., zero down payment)
        if coc > 12: return 2.5
        if coc > 8: return 1.5
        if coc > 5: return 0.5
        if coc >= 0: return 0.0 # Slightly positive or zero is neutral here
        if coc > -5: return -1.0
        return -2.5

    def score_cap_rate(cap, is_dynamic_capex):
        if not is_dynamic_capex or cap is None: return 0.0 # Neutral if not dynamic or not calculable
        if cap > 8: return 2.5
        if cap > 6: return 1.5
        if cap > 4: return 0.5
        if cap > 0: return -0.5 # Positive but low cap rate
        return -1.0 # Negative cap rate

    def score_annualized_total_roi(annual_roi):
        if annual_roi is None: return 0.0 # Should ideally always be calculable
        if annual_roi > 15: return 2.5
        if annual_roi > 10: return 1.5
        if annual_roi > 5: return 0.5
        if annual_roi >= 0: return 0.0
        if annual_roi > -5: return -1.0
        return -2.5

    cf_monthly_val = financials['net_monthly_cashflow']
    coc_roi_val = financials['cash_on_cash_roi']
    cap_rate_val = financials.get('cap_rate') # Might be None
    annualized_total_roi_val = appreciation_returns.get('annualized_roi_on_equity')

    score_cf = score_cashflow(cf_monthly_val)
    score_coc = score_coc_roi(coc_roi_val)
    score_cap = score_cap_rate(cap_rate_val, args.use_dynamic_capex)
    score_ann_roi = score_annualized_total_roi(annualized_total_roi_val)

    raw_total_score = score_cf + score_coc + score_cap + score_ann_roi
    final_score = max(-10.0, min(10.0, raw_total_score))

    # Qualitative Score Assessment
    score_interpretation = ""
    if final_score >= 8: score_interpretation = "Excellent Investment Prospect"
    elif final_score >= 5: score_interpretation = "Good Investment Prospect"
    elif final_score >= 1: score_interpretation = "Fair Investment Prospect, Potential Upsides"
    elif final_score > -2: score_interpretation = "Marginal Investment, Exercise Caution"
    elif final_score > -5: score_interpretation = "Poor Investment Prospect"
    else: score_interpretation = "Very Poor Investment Prospect, Likely Avoid"

    print(colorize(format_label_value("Overall Investment Score:", f"{final_score:.1f}/10 ({score_interpretation})"), bold))
    print(hr("-", 40))
    
    # Print individual metric ratings (using previous logic for now, can be integrated with scores)
    coc_rating = ("Excellent" if coc_roi_val is not None and coc_roi_val > 12 else
                  "Good" if coc_roi_val is not None and coc_roi_val > 8 else
                  "Fair" if coc_roi_val is not None and coc_roi_val > 5 else
                  "Poor" if coc_roi_val is not None else "N/A")

    cap_rating = "N/A"
    if args.use_dynamic_capex and cap_rate_val is not None:
        cap_rating = ("Excellent" if cap_rate_val > 8 else
                      "Good" if cap_rate_val > 6 else
                      "Fair" if cap_rate_val > 4 else
                      "Poor")

    cashflow_rating = ("Excellent" if cf_monthly_val > 300 else
                       "Good" if cf_monthly_val > 100 else
                       "Fair" if cf_monthly_val > 0 else
                       "Poor" if cf_monthly_val <=0 else "N/A") # Adjusted poor condition

    print(format_label_value("Net Monthly Cashflow:", f"{format_currency(cf_monthly_val)} (Rating: {cashflow_rating}, Score: {score_cf:.1f})"))
    print(format_label_value("Cash-on-Cash ROI:", f"{format_percent(coc_roi_val)} (Rating: {coc_rating}, Score: {score_coc:.1f})"))
    
    if args.use_dynamic_capex:
        print(format_label_value("Cap Rate (NOI Based):", f"{format_percent(cap_rate_val)} (Rating: {cap_rating}, Score: {score_cap:.1f})"))
    else:
        print(format_label_value("Cap Rate (NOI Based):", "N/A (Requires Dynamic CapEx mode)"))
        
    print(format_label_value("Annualized Total ROI (Equity):", f"{format_percent(annualized_total_roi_val)} (Score: {score_ann_roi:.1f})"))
    
    # Construct a more detailed summary statement
    summary_points = []
    if score_cf > 1.0: summary_points.append("strong positive cashflow")
    elif score_cf < -1.0: summary_points.append("significant negative cashflow")
    elif score_cf <=0: summary_points.append("marginal or negative cashflow")
    else: summary_points.append("modest positive cashflow")

    if score_coc > 1.0: summary_points.append("excellent CoC ROI")
    elif score_coc < -1.0: summary_points.append("poor CoC ROI")
    else: summary_points.append("moderate CoC ROI")

    if args.use_dynamic_capex:
        if score_cap > 1.0: summary_points.append("strong Cap Rate")
        elif score_cap < 0: summary_points.append("weak Cap Rate") # cap score is 0 or negative if not strong
        else: summary_points.append("fair Cap Rate")
    else:
        summary_points.append("Cap Rate not assessed (Dynamic CapEx off)")

    if score_ann_roi > 1.0: summary_points.append("promising long-term total returns")
    elif score_ann_roi < -1.0: summary_points.append("poor long-term total return outlook")
    else: summary_points.append("moderate long-term total returns expected")

    # Final summary statement based on the score and points
    final_summary_text = f"SUMMARY ({final_score:.1f}/10 - {score_interpretation}): This property shows "
    if len(summary_points) > 0:
        if len(summary_points) == 1:
            final_summary_text += summary_points[0] + "."
        elif len(summary_points) == 2:
            final_summary_text += summary_points[0] + " and " + summary_points[1] + "."
        else: # more than 2
            final_summary_text += ", ".join(summary_points[:-1]) + ", and " + summary_points[-1] + "."
    else:
        final_summary_text += "a mixed profile requiring careful review." # Fallback

    # Determine color based on final score
    summary_color = neg_color if final_score < 0 else (pos_color if final_score > 1 else '') # Neutral for scores near 0-1

    print(hr("-", 40))
    print(colorize(final_summary_text, summary_color if final_score != 0 else '')) # Avoid coloring if score is exactly 0
    # --- End Deal Analysis & Summary ---

    print(hr("="))
    if args.verbose: print("DEBUG: Analysis printing complete.", flush=True)


# --- Main Execution ---
def main():
    # Initial load of config to pass to argparse for its internal defaults for --config-path
    # This means --config-path itself defaults to DEFAULT_CONFIG_PATH
    # If user provides --config-path on CLI, it will be used by argparse.
    
    # Step 1: Parse arguments once to get the config_path (among others)
    # We create a temporary parser just to get the config_path if specified.
    temp_parser = argparse.ArgumentParser(add_help=False)
    temp_parser.add_argument("--config-path", default=str(DEFAULT_CONFIG_PATH), help="Path to JSON config file.")
    known_args, _ = temp_parser.parse_known_args()
    config_path_to_load = known_args.config_path

    config = load_config(config_path_to_load)

    # Step 2: Parse all arguments, now providing the loaded config.
    # The parse_arguments function will handle Config > CLI > Script Default precedence.
    args = parse_arguments(config)


    if args.capex_guide:
        print_capex_guide(args) # Pass args for verbose
        return

    if args.verbose:
        print("--- Initial Arguments & Config ---")
        print(f"Using config file: {args.config_path}") # This will be the resolved one
        print(f"Loaded config: {json.dumps(config, indent=2)}")
        print(f"Arguments after parsing (Config > CLI > ScriptDefault): {vars(args)}")

    # Step 3: Fetch property data from DB (DB is highest precedence for specific fields)
    property_data = fetch_property_data(args.db_path, args.address, args.verbose)

    if not property_data:
        print(f"Could not retrieve data for address: {args.address}. Exiting.", file=sys.stderr)
        return

    if args.verbose:
        print("--- Property Data from DB ---")
        print(json.dumps(property_data, indent=2))

    # Step 4: Determine effective parameters using DB > Config > CLI > Script Default
    # For most args, this is already handled by parse_arguments taking 'config'.
    # Now we explicitly override with DB data where applicable.

    eff_config = config # Use the already loaded config

    # Fields to be taken from DB if available, otherwise from args (which already factored in config/CLI)
    db_price = property_data.get("price")
    db_sqft = property_data.get("sqft")
    db_prop_age = property_data.get("calculated_property_age")
    db_zip = property_data.get("zip")
    # These are not direct args but used in calculations
    db_tax_info_raw = property_data.get("tax_information_raw")
    db_est_rent_raw = property_data.get("estimated_rent_raw")


    # Create a dictionary for effective parameters that will be passed to analysis functions
    # Start with args (which are Config > CLI > Script Default)
    effective_params = vars(args).copy()

    # Override with DB values if they exist and are valid
    if db_price is not None and db_price > 0:
        effective_params["purchase_price"] = db_price # This will be used by financial_components
    elif 'purchase_price' not in effective_params or effective_params.get('purchase_price') is None:
        # If price isn't in DB and not otherwise set (e.g. if we ever add --price CLI/config)
        # This path should ideally not be hit if price is always in DB for a found property.
        print(f"Error: Purchase price not found in DB for {args.address} and not otherwise specified.", file=sys.stderr)
        return


    if db_sqft is not None and db_sqft > 0:
        effective_params["square_feet"] = db_sqft
    if db_prop_age is not None: # Can be 0 for new construction
        effective_params["property_age"] = db_prop_age
    
    # For rent and tax, they are not direct CLI args but are passed to financial_components
    # So, ensure property_data values are used by financial_components call
    effective_params["tax_information_raw"] = db_tax_info_raw
    effective_params["estimated_rent_raw"] = db_est_rent_raw


    # Resolve neighborhood: CLI > ZIP Mapping > Config Default ("neighborhood" key) > Script Default ("default")
    neighborhood_appreciation_config_data = eff_config.get("neighborhood_appreciation_data", {})
    zip_to_neighborhood_mapping = eff_config.get("zip_to_neighborhood_mapping", {})
    
    effective_neighborhood_name = args.neighborhood # 1. CLI
    if not effective_neighborhood_name and db_zip:    # 2. ZIP Mapping
        effective_neighborhood_name = zip_to_neighborhood_mapping.get(str(db_zip))
        if effective_neighborhood_name and args.verbose:
            print(f"Info: Inferred neighborhood '{effective_neighborhood_name}' from ZIP '{db_zip}'.")
    if not effective_neighborhood_name:               # 3. Config "neighborhood" field
        effective_neighborhood_name = eff_config.get("neighborhood")
    if not effective_neighborhood_name:               # 4. Fallback to literal "default"
        effective_neighborhood_name = "default"
        if args.verbose:
            print(f"Info: Using default neighborhood '{effective_neighborhood_name}'.")


    if args.verbose:
        print("--- Effective Parameters for Analysis ---")
        print(f"Purchase Price (for calc): {effective_params.get('purchase_price')}")
        print(f"Square Feet (for calc): {effective_params.get('square_feet')}")
        print(f"Property Age (for calc): {effective_params.get('property_age')}")
        print(f"Neighborhood (for calc): {effective_neighborhood_name}")
        print(f"Raw Tax Info (for calc): {effective_params.get('tax_information_raw')}")
        print(f"Raw Est. Rent (for calc): {effective_params.get('estimated_rent_raw')}")
        # Print other key effective_params if needed for debugging
        # for k, v in effective_params.items():
        #     if k not in ['purchase_price', 'square_feet', 'property_age', 'tax_information_raw', 'estimated_rent_raw']:
        #         print(f"{k}: {v}")


    # Call the main analysis function with the resolved effective parameters
    run_analysis_and_print(
        args=argparse.Namespace(**effective_params), # Pass the fully resolved params
        property_data=property_data, # Still pass original DB data for reference if needed by run_analysis_and_print
        neighborhood_data_from_config=neighborhood_appreciation_config_data,
        effective_neighborhood_name_for_analysis=effective_neighborhood_name
    )

if __name__ == "__main__":
    main()
