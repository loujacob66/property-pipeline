#!/usr/bin/env python3
"""
Enhanced Cashflow Analyzer for Real Estate Properties

This script calculates the estimated monthly cashflow for a property based on
purchase price, loan details, taxes, rent, and other expenses.
It includes detailed CapEx and maintenance calculations based on property characteristics.

Usage:
    python scripts/enhanced_cashflow_analyzer.py --address "<FULL_ADDRESS>" \
                                        --down-payment <DOLLARS> \
                                        --rate <ANNUAL_RATE> \
                                        --insurance <ANNUAL_INSURANCE> \
                                        --misc-monthly <MONTHLY_MISC> \
                                        [--property-age <AGE_IN_YEARS>] \
                                        [--property-condition <EXCELLENT|GOOD|FAIR|POOR>] \
                                        [--square-feet <SQUARE_FEET>] \
                                        [--loan-term <YEARS>] \
                                        [--db-path <PATH_TO_DB>]
"""

# Add debug output at the beginning
print("DEBUG: Script starting...", flush=True)
print("DEBUG: Importing modules...", flush=True)

import argparse
import sqlite3
import re
import json
import datetime
import sys
from pathlib import Path

print(f"DEBUG: Python version: {sys.version}", flush=True)
print(f"DEBUG: Arguments received: {sys.argv}", flush=True)

# Constants
ROOT = Path(__file__).parent.parent
DEFAULT_DB_PATH = ROOT / "data" / "listings.db"
DEFAULT_CONFIG_PATH = ROOT / "config" / "cashflow_config.json"

print(f"DEBUG: Script path: {__file__}", flush=True)
print(f"DEBUG: ROOT path: {ROOT}", flush=True)
print(f"DEBUG: Default DB path: {DEFAULT_DB_PATH}", flush=True)
print(f"DEBUG: Default config path: {DEFAULT_CONFIG_PATH}", flush=True)
print("DEBUG: About to define CAPEX_COMPONENTS...", flush=True)

# CapEx Components with typical lifespans and costs
# This dictionary powers the detailed CapEx calculations
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

print("DEBUG: CAPEX_COMPONENTS defined.", flush=True)
print("DEBUG: About to define CONDITION_MULTIPLIERS...", flush=True)

# Property condition multipliers - affects maintenance and CapEx costs
CONDITION_MULTIPLIERS = {
    "excellent": 0.7,  # Lower costs for excellent condition
    "good": 1.0,       # Baseline
    "fair": 1.3,       # Higher costs for fair condition
    "poor": 1.7        # Much higher costs for poor condition
}

print("DEBUG: CONDITION_MULTIPLIERS defined.", flush=True)
print("DEBUG: About to define get_age_multiplier function...", flush=True)

# Age multipliers function - affects maintenance and CapEx costs
def get_age_multiplier(age):
    """Returns a cost multiplier based on property age."""
    if age <= 5:
        return 0.6    # New properties have lower costs
    elif age <= 15:
        return 0.9    # Newer properties have slightly lower costs
    elif age <= 30:
        return 1.1    # Middle-aged properties have slightly higher costs
    elif age <= 50:
        return 1.3    # Older properties have higher costs
    else:
        return 1.5    # Very old properties have much higher costs

print("DEBUG: get_age_multiplier function defined.", flush=True)
print("DEBUG: About to define load_config function...", flush=True)

def load_config(config_path):
    """Loads configuration from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        # print(f"Info: Configuration file '{config_path}' not found. Using command-line arguments or defaults.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_path}'. Please check its format.")
        return {} # Return empty dict to allow CLI to take precedence or error out if required args missing

def parse_arguments(config):
    """Parses command-line arguments, using config for defaults."""
    parser = argparse.ArgumentParser(description="Enhanced Real Estate Cashflow Analyzer")
    parser.add_argument(
        "--address",
        type=str,
        required=True,
        help="The full address of the property in the listings table."
    )
    parser.add_argument(
        "--down-payment",
        type=float,
        default=config.get("down_payment"),
        help=f"Down payment amount in actual dollars, not percent (e.g., 50000 for $50,000). Default from config: {config.get('down_payment')}"
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=config.get("rate"),
        help=f"Annual interest rate (e.g., 5.5 for 5.5%%). Default from config: {config.get('rate')}"
    )
    parser.add_argument(
        "--insurance",
        type=float,
        default=config.get("insurance"),
        help=f"Estimated annual insurance cost. Default from config: {config.get('insurance')}"
    )
    parser.add_argument(
        "--misc-monthly",
        type=float,
        default=config.get("misc_monthly"),
        help=f"Miscellaneous monthly costs. Default from config: {config.get('misc_monthly')}"
    )
    parser.add_argument(
        "--loan-term",
        type=int,
        default=config.get("loan_term", 30),
        help=f"Loan term in years. Default from config: {config.get('loan_term', 30)}"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to the SQLite database file (default: {DEFAULT_DB_PATH})."
    )
    parser.add_argument(
        "--config-path",
        type=str,
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to the JSON configuration file (default: {DEFAULT_CONFIG_PATH})."
    )
    # Parameters for enhanced cashflow analysis
    parser.add_argument(
        "--vacancy-rate",
        type=float,
        default=config.get("vacancy_rate", 5.0),
        help=f"Expected vacancy rate as percentage (e.g., 5.0 for 5%). Default: {config.get('vacancy_rate', 5.0)}%"
    )
    parser.add_argument(
        "--property-mgmt-fee",
        type=float,
        default=config.get("property_mgmt_fee", 0.0),
        help=f"Property management fee as percentage of rent (e.g., 8.0 for 8%). Default: {config.get('property_mgmt_fee', 0.0)}%"
    )
    parser.add_argument(
        "--maintenance-percent",
        type=float,
        default=config.get("maintenance_percent", 1.0),
        help=f"Annual maintenance cost as percentage of property value (e.g., 1.0 for 1%). Default: {config.get('maintenance_percent', 1.0)}%"
    )
    parser.add_argument(
        "--capex-percent",
        type=float,
        default=config.get("capex_percent", 1.0),
        help=f"Annual capital expenditure reserve as percentage of property value. Default: {config.get('capex_percent', 1.0)}%"
    )
    parser.add_argument(
        "--utilities-monthly",
        type=float,
        default=config.get("utilities_monthly", 0.0),
        help=f"Monthly utilities cost paid by landlord. Default: ${config.get('utilities_monthly', 0.0)}"
    )
    # New parameters for dynamic CapEx and maintenance calculation
    parser.add_argument(
        "--property-age",
        type=int,
        default=config.get("property_age", 20),
        help=f"Age of the property in years. Used if not calculable from DB (e.g., missing year_built) or if DB value is invalid. Default: {config.get('property_age', 20)} years"
    )
    parser.add_argument(
        "--property-condition",
        type=str,
        default=config.get("property_condition", "good").lower(),
        choices=["excellent", "good", "fair", "poor"],
        help=f"Condition of the property. Default: {config.get('property_condition', 'good')}"
    )
    parser.add_argument(
        "--square-feet",
        type=float,
        default=config.get("square_feet", 1400),
        help=f"Square footage of the property. Used if not found or invalid in DB. Default from config: {config.get('square_feet', 1400)} sq ft"
    )
    parser.add_argument(
        "--use-dynamic-capex",
        action=argparse.BooleanOptionalAction,
        default=config.get("use_dynamic_capex", False),
        help="Use detailed component-based CapEx calculations. Can be set in config. Overrides with --use-dynamic-capex or --no-use-dynamic-capex."
    )
    parser.add_argument(
        "--capex-guide",
        action="store_true", 
        help="Print the CapEx reference guide and exit"
    )
    
    args = parser.parse_args()

    # Validate that essential financial parameters are present either from CLI or config
    required_financial_args = ["down_payment", "rate", "insurance", "misc_monthly"]
    missing_args = []
    for arg_name in required_financial_args:
        if getattr(args, arg_name) is None:
            missing_args.append(arg_name)
    
    if missing_args:
        parser.error(f"Missing required financial arguments: {', '.join(missing_args)}. Provide them via command line or in the config file ('{args.config_path}').")

    return args

def fetch_property_data(db_path, address):
    """Fetches property data from the database by address."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        # Fetch price, tax_information, estimated_rent, id, sqft, and year_built
        cursor.execute(
            "SELECT price, tax_information, estimated_rent, id, sqft, year_built FROM listings WHERE address = ?",
            (address,)
        )
        property_data_row = cursor.fetchone()
        if property_data_row:
            db_price = property_data_row[0]
            db_tax_info = property_data_row[1]
            db_rent_raw = property_data_row[2]
            db_id = property_data_row[3]
            db_sqft_raw = property_data_row[4]
            db_year_built_raw = property_data_row[5]

            # Process sqft
            processed_sqft = None
            if db_sqft_raw is not None:
                try:
                    val = float(db_sqft_raw)
                    if val > 0:
                        processed_sqft = val
                    else:
                        print(f"Warning: DB sqft value '{db_sqft_raw}' for address '{address}' is not positive. Sqft not used from DB.")
                except (ValueError, TypeError):
                    print(f"Warning: DB sqft value '{db_sqft_raw}' for address '{address}' is not a valid number. Sqft not used from DB.")
            
            # Process year_built to calculate age
            calculated_property_age = None
            if db_year_built_raw:
                try:
                    # Extract potential year using regex to handle formats like 'Built in YYYY' or just 'YYYY'
                    match = re.search(r'(\d{4})', str(db_year_built_raw))
                    if match:
                        year_built_int = int(match.group(1))
                        current_year = datetime.datetime.now().year
                        # Basic sanity check for the year
                        if 1800 <= year_built_int <= current_year:
                            calculated_property_age = current_year - year_built_int
                        else:
                            print(f"Warning: Parsed year_built '{year_built_int}' from DB value '{db_year_built_raw}' for address '{address}' is out of reasonable range. Age not calculated from DB.")
                    else:
                        print(f"Warning: Could not parse a 4-digit year from DB year_built value '{db_year_built_raw}' for address '{address}'. Age not calculated from DB.")
                except ValueError: # Should be caught by regex, but as a safeguard
                    print(f"Warning: DB year_built value '{db_year_built_raw}' for address '{address}' could not be converted to an integer year. Age not calculated from DB.")
            
            return {
                "price": db_price,
                "tax_information_raw": db_tax_info,
                "estimated_rent_raw": db_rent_raw,
                "id": db_id,
                "sqft": processed_sqft, # Use processed sqft
                "year_built_raw": db_year_built_raw, # Store raw year_built for reference/logging
                "calculated_property_age": calculated_property_age # Age calculated from year_built
            }
        else:
            print(f"Error: Property with address '{address}' not found in the database.")
            return None
    except sqlite3.Error as e:
        print(f"Database error while fetching property data for '{address}': {e}")
        return None
    finally:
        conn.close()

def parse_tax_amount(tax_info_str):
    """
    Extracts a numerical annual tax amount from a string.
    Assumes the string contains an annual tax figure.
    Example: "$5,000 / Annually" -> 5000.0
    Example: "Taxes: $4,800" -> 4800.0
    Returns None if no amount can be parsed.
    """
    if not tax_info_str:
        return None
    
    # Look for amounts like $5,000 or 5000
    match = re.search(r'\$?([\d,]+(?:\.\d+)?)', tax_info_str)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            return None
    return None

def calculate_mortgage_payment(principal, annual_interest_rate_percent, loan_term_years):
    """Calculates the monthly mortgage payment (Principal & Interest)."""
    if principal <= 0:
        return 0
    
    monthly_interest_rate = (annual_interest_rate_percent / 100) / 12
    number_of_payments = loan_term_years * 12

    if monthly_interest_rate == 0: # Avoid division by zero for 0% interest
        return principal / number_of_payments if number_of_payments > 0 else 0

    m = principal * (monthly_interest_rate * (1 + monthly_interest_rate) ** number_of_payments) / \
        ((1 + monthly_interest_rate) ** number_of_payments - 1)
    return m

def calculate_capex_reserves(purchase_price, square_feet, property_age, property_condition):
    """
    Calculates detailed CapEx reserves based on property specifics.
    Returns a dictionary with individual component costs and total annual/monthly reserves.
    """
    age_multiplier = get_age_multiplier(property_age)
    condition_multiplier = CONDITION_MULTIPLIERS.get(property_condition.lower(), 1.0)
    
    # Calculate annual reserve for each component
    component_reserves = {}
    total_annual_reserve = 0
    
    for component, details in CAPEX_COMPONENTS.items():
        lifespan = details["lifespan"]
        
        # Adjust lifespan based on condition (better condition = longer life)
        adjusted_lifespan = lifespan * (1 / condition_multiplier)
        
        # Calculate replacement cost based on square footage or base cost
        if "cost_per_sqft" in details:
            replacement_cost = details["cost_per_sqft"] * square_feet
            if "cost_base" in details:
                replacement_cost += details["cost_base"]
        else:
            replacement_cost = details["cost_base"]
        
        # Apply condition and age multipliers to cost
        adjusted_cost = replacement_cost * condition_multiplier * age_multiplier
        
        # Calculate annual reserve (replacement cost / adjusted lifespan)
        annual_reserve = adjusted_cost / adjusted_lifespan
        
        # Store component details
        component_reserves[component] = {
            "replacement_cost": adjusted_cost,
            "lifespan_years": adjusted_lifespan,
            "annual_reserve": annual_reserve,
            "monthly_reserve": annual_reserve / 12
        }
        
        total_annual_reserve += annual_reserve
    
    # Calculate as percentage of property value for comparison
    capex_percent_of_value = (total_annual_reserve / purchase_price) * 100 if purchase_price > 0 else 0
    
    return {
        "components": component_reserves,
        "total_annual": total_annual_reserve,
        "total_monthly": total_annual_reserve / 12,
        "percent_of_value": capex_percent_of_value
    }

def calculate_financial_components(
    purchase_price, 
    tax_info_raw, 
    estimated_monthly_rent, 
    down_payment_input_dollars, 
    annual_interest_rate_percent, 
    loan_term_years, 
    annual_insurance_cost, 
    misc_monthly_cost,
    vacancy_rate_percent=5.0,
    property_mgmt_fee_percent=0.0,
    maintenance_percent=1.0,
    capex_percent=1.0,
    utilities_monthly=0.0,
    use_dynamic_capex=False,
    property_age=20,
    property_condition="good",
    square_feet=1400
):
    """
    Calculates all key financial components for cashflow analysis.

    Returns:
        A dictionary containing calculated financial components.
        Returns None if essential data like purchase_price is missing or invalid.
    """
    if purchase_price is None or purchase_price <= 0:
        print("Error: Purchase price is missing or invalid for financial calculation.")
        return None # Or raise an error

    # Ensure effective_rent is always defined
    if estimated_monthly_rent is None:
        print("Warning: Estimated monthly rent not found. Cashflow will be impacted. Using $0 for rent.")
        effective_rent = 0
    else:
        effective_rent = estimated_monthly_rent

    # 1. Down Payment & Loan Amount
    down_payment_amount = down_payment_input_dollars
    loan_amount = 0
    
    if down_payment_amount > purchase_price:
        print(f"Info: Down payment (${down_payment_amount:,.2f}) exceeds purchase price (${purchase_price:,.2f}). Clamping loan to $0.")
        down_payment_amount = purchase_price
        loan_amount = 0
    elif down_payment_amount < 0:
        print("Info: Down payment cannot be negative. Setting to $0.")
        down_payment_amount = 0
        loan_amount = purchase_price
    else:
        loan_amount = purchase_price - down_payment_amount
    
    down_payment_percentage = (down_payment_amount / purchase_price) * 100 if purchase_price > 0 else 0

    # 2. Monthly P&I
    monthly_p_and_i = calculate_mortgage_payment(loan_amount, annual_interest_rate_percent, loan_term_years)

    # 3. Monthly Taxes
    # ASSUMPTION: Parsed tax amount is ANNUAL.
    annual_taxes = parse_tax_amount(tax_info_raw)
    monthly_taxes = 0
    if annual_taxes is not None:
        monthly_taxes = annual_taxes / 12
    else:
        # This info message could be handled by the caller if more context is needed
        # print(f"Info: Could not parse tax information: '{tax_info_raw}'. Monthly taxes set to 0 for calculation.")
        pass # Let the caller decide how to log this based on verbosity needs
    
    # 4. Monthly Insurance
    monthly_insurance = annual_insurance_cost / 12 if annual_insurance_cost is not None else 0
    
    # 5. Vacancy loss calculation (if using enhanced features)
    if use_dynamic_capex:
        vacancy_factor = vacancy_rate_percent / 100
        effective_rent_after_vacancy = effective_rent * (1 - vacancy_factor)
    else:
        effective_rent_after_vacancy = effective_rent
    
    # 6. Property management fee (if using enhanced features)
    if use_dynamic_capex:
        monthly_property_mgmt = effective_rent_after_vacancy * (property_mgmt_fee_percent / 100)
    else:
        monthly_property_mgmt = 0
    
    # 7. Maintenance reserve (if using enhanced features)
    if use_dynamic_capex:
        age_multiplier = get_age_multiplier(property_age)
        condition_multiplier = CONDITION_MULTIPLIERS.get(property_condition.lower(), 1.0)
        adjusted_maintenance_percent = maintenance_percent * age_multiplier * condition_multiplier
        monthly_maintenance = (purchase_price * (adjusted_maintenance_percent / 100)) / 12
    else:
        adjusted_maintenance_percent = maintenance_percent
        monthly_maintenance = 0
    
    # 8. CapEx reserve (if using enhanced features)
    if use_dynamic_capex:
        # Calculate component-based CapEx
        capex_reserve = calculate_capex_reserves(
            purchase_price,
            square_feet,
            property_age,
            property_condition
        )
        monthly_capex = capex_reserve["total_monthly"]
        adjusted_capex_percent = capex_reserve["percent_of_value"]
    else:
        monthly_capex = 0
        capex_reserve = None
        adjusted_capex_percent = capex_percent

    # 9. Total Monthly Expenses (based on whether using enhanced features)
    if use_dynamic_capex:
        total_monthly_expenses = (
            monthly_p_and_i + 
            monthly_taxes + 
            monthly_insurance + 
            misc_monthly_cost +
            monthly_property_mgmt + 
            monthly_maintenance + 
            monthly_capex + 
            utilities_monthly
        )
    else:
        # Original calculation
        total_monthly_expenses = monthly_p_and_i + monthly_taxes + monthly_insurance + misc_monthly_cost

    # 10. Net Monthly Cashflow
    if use_dynamic_capex:
        net_monthly_cashflow = effective_rent_after_vacancy - total_monthly_expenses
    else:
        net_monthly_cashflow = effective_rent - total_monthly_expenses
    
    # 11. Calculate annualized returns (if using enhanced features)
    if use_dynamic_capex:
        annual_cashflow = net_monthly_cashflow * 12
        cash_on_cash_roi = (annual_cashflow / down_payment_amount) * 100 if down_payment_amount > 0 else 0
        
        # 12. Calculate cap rate (NOI / Purchase Price)
        annual_noi = (effective_rent_after_vacancy * 12) - (
            (monthly_insurance + monthly_taxes + monthly_property_mgmt + 
            monthly_maintenance + monthly_capex + utilities_monthly + misc_monthly_cost) * 12
        )
        cap_rate = (annual_noi / purchase_price) * 100 if purchase_price > 0 else 0
    else:
        annual_cashflow = net_monthly_cashflow * 12
        cash_on_cash_roi = (annual_cashflow / down_payment_amount) * 100 if down_payment_amount > 0 else 0
        annual_noi = None
        cap_rate = None
    
    return {
        "purchase_price": purchase_price,
        "down_payment_amount": down_payment_amount,
        "down_payment_percentage": down_payment_percentage,
        "loan_amount": loan_amount,
        "annual_interest_rate_percent": annual_interest_rate_percent,
        "loan_term_years": loan_term_years,
        "annual_insurance_cost": annual_insurance_cost,
        "misc_monthly_cost": misc_monthly_cost,
        "tax_info_raw": tax_info_raw,
        "estimated_monthly_rent": effective_rent,
        "monthly_p_and_i": monthly_p_and_i,
        "annual_taxes": annual_taxes,
        "monthly_taxes": monthly_taxes,
        "monthly_insurance": monthly_insurance,
        "vacancy_rate_percent": vacancy_rate_percent if use_dynamic_capex else None,
        "effective_rent_after_vacancy": effective_rent_after_vacancy if use_dynamic_capex else effective_rent,
        "property_mgmt_fee_percent": property_mgmt_fee_percent if use_dynamic_capex else None,
        "monthly_property_mgmt": monthly_property_mgmt if use_dynamic_capex else None,
        "maintenance_percent": maintenance_percent if use_dynamic_capex else None,
        "adjusted_maintenance_percent": adjusted_maintenance_percent if use_dynamic_capex else None,
        "monthly_maintenance": monthly_maintenance if use_dynamic_capex else None,
        "capex_percent": capex_percent if use_dynamic_capex else None,
        "adjusted_capex_percent": adjusted_capex_percent if use_dynamic_capex else None,
        "monthly_capex": monthly_capex if use_dynamic_capex else None,
        "capex_reserve": capex_reserve,
        "utilities_monthly": utilities_monthly if use_dynamic_capex else None,
        "total_monthly_expenses": total_monthly_expenses,
        "net_monthly_cashflow": net_monthly_cashflow,
        "annual_cashflow": annual_cashflow,
        "cash_on_cash_roi": cash_on_cash_roi,
        "annual_noi": annual_noi,
        "cap_rate": cap_rate,
        # Property specific data if using dynamic calculations
        "property_age": property_age if use_dynamic_capex else None,
        "property_condition": property_condition if use_dynamic_capex else None,
        "square_feet": square_feet if use_dynamic_capex else None,
        "use_dynamic_capex": use_dynamic_capex
    }

def print_capex_guide():
    """Prints detailed information about CapEx components for reference."""
    print("DEBUG: Entering print_capex_guide function...")
    print("\n" + "=" * 80)
    print(f"CAPEX COMPONENTS REFERENCE GUIDE")
    print("=" * 80)
    print("This guide shows typical CapEx components, their default lifespans and costs.")
    print("These values are adjusted based on property age and condition in the analysis.")
    print("=" * 80)
    
    print(f"{'Component':<20} {'Typical Lifespan':<20} {'Cost Basis':<30}")
    print("-" * 80)
    
    for component, details in CAPEX_COMPONENTS.items():
        component_name = component.replace('_', ' ').title()
        lifespan = f"{details['lifespan']} years"
        
        if "cost_per_sqft" in details:
            cost_basis = f"${details['cost_per_sqft']:.2f}/sq ft"
            if "cost_base" in details:
                cost_basis += f" + ${details['cost_base']:.2f} base"
        else:
            cost_basis = f"${details['cost_base']:.2f} flat fee"
            
        print(f"{component_name:<20} {lifespan:<20} {cost_basis:<30}")
    
    print("=" * 80)
    print("PROPERTY CONDITION MULTIPLIERS")
    print("-" * 80)
    for condition, multiplier in sorted(CONDITION_MULTIPLIERS.items()):
        print(f"{condition.title():<20} {multiplier:.2f}x")
    
    print("=" * 80)
    print("AGE MULTIPLIERS")
    print("-" * 80)
    print(f"{'New (≤ 5 years)':<20} {0.6:.2f}x")
    print(f"{'Newer (6-15 years)':<20} {0.9:.2f}x")
    print(f"{'Middle-age (16-30)':<20} {1.1:.2f}x")
    print(f"{'Older (31-50 years)':<20} {1.3:.2f}x")
    print(f"{'Very old (>50 years)':<20} {1.5:.2f}x")
    print("=" * 80)
    print("DEBUG: Exiting print_capex_guide function...")

def calculate_and_print_cashflow(args, property_data):
    """Calculates and prints the cashflow analysis using calculate_financial_components."""
    
    purchase_price = property_data.get("price")
    tax_info_raw = property_data.get("tax_information_raw")
    estimated_monthly_rent = property_data.get("estimated_rent_raw") # This can be None

    if purchase_price is None: # Basic check before calling the calculation
        print("Error: Purchase price not found for the property. Cannot calculate cashflow.")
        return
    if purchase_price <= 0:
        print("Error: Purchase price must be a positive value. Cannot calculate cashflow.")
        return
        
    # Determine actual square_feet and property_age to use
    # Prioritize DB values if they exist and are valid, otherwise use args (from CLI/config/defaults)
    
    # Square Feet
    actual_square_feet = args.square_feet # Default to arg
    db_sqft_value = property_data.get("sqft")
    if db_sqft_value is not None: # This implies it was valid when processed in fetch_property_data
        actual_square_feet = db_sqft_value
        print(f"DEBUG: Using square footage from DB: {actual_square_feet:.0f}", flush=True)
    else:
        # This implies sqft was NULL in DB, or had an invalid value (e.g. non-positive, non-numeric)
        # Warnings for invalid DB values are printed in fetch_property_data
        print(f"DEBUG: Square footage not found or invalid in DB for property. Using value from arguments/config: {args.square_feet:.0f}", flush=True)

    # Property Age
    actual_property_age = args.property_age # Default to arg
    db_calculated_age = property_data.get("calculated_property_age")
    if db_calculated_age is not None: # This implies year_built was present and resulted in a valid age
        actual_property_age = db_calculated_age
        print(f"DEBUG: Using property age calculated from DB (Year Built: {property_data.get('year_built_raw', 'N/A')}): {actual_property_age} years", flush=True)
    else:
        # This implies year_built was missing, unparseable, or resulted in an invalid age
        # Warnings for these cases are printed in fetch_property_data
        print(f"DEBUG: Property age not calculated from DB (Year Built from DB: '{property_data.get('year_built_raw', 'N/A')}'). Using value from arguments/config: {args.property_age} years", flush=True)

    # Handle case where estimated_rent_raw might be None
    # The calculation function also has a default, but good to be explicit
    effective_estimated_monthly_rent = estimated_monthly_rent if estimated_monthly_rent is not None else 0
    if estimated_monthly_rent is None:
         print("Warning: Estimated monthly rent not found for the property. Using $0 for calculation.")

    financials = calculate_financial_components(
        purchase_price=purchase_price,
        tax_info_raw=tax_info_raw,
        estimated_monthly_rent=effective_estimated_monthly_rent, # Pass the handled rent
        down_payment_input_dollars=args.down_payment,
        annual_interest_rate_percent=args.rate,
        loan_term_years=args.loan_term,
        annual_insurance_cost=args.insurance,
        misc_monthly_cost=args.misc_monthly,
        vacancy_rate_percent=args.vacancy_rate,
        property_mgmt_fee_percent=args.property_mgmt_fee,
        maintenance_percent=args.maintenance_percent,
        capex_percent=args.capex_percent,
        utilities_monthly=args.utilities_monthly,
        use_dynamic_capex=args.use_dynamic_capex,
        property_age=actual_property_age,
        property_condition=args.property_condition,
        square_feet=actual_square_feet
    )

    if financials is None:
        # Error message already printed by calculate_financial_components or preceding checks
        return

    # --------------------------------------------------------------
    # Basic output (similar to original script) when not using dynamic CapEx
    # --------------------------------------------------------------
    if not args.use_dynamic_capex:
        # Original-style output
        print("\n--- Cashflow Analysis ---")
        print(f"Address: {args.address}")
        print(f"Property ID in DB: {property_data.get('id')}") 
        print(f"Database Path: {args.db_path}")
        print("--- Inputs ---")
        print(f"Purchase Price: ${financials['purchase_price']:,.2f}")
        print(f"Down Payment: ${financials['down_payment_amount']:,.2f} ({financials['down_payment_percentage']:.2f}% of purchase price)")
        print(f"Loan Amount: ${financials['loan_amount']:,.2f}")
        print(f"Annual Interest Rate: {financials['annual_interest_rate_percent']:.3f}%")
        print(f"Loan Term: {financials['loan_term_years']} years")
        print(f"Estimated Annual Insurance: ${financials['annual_insurance_cost']:,.2f}")
        print(f"Miscellaneous Monthly Costs: ${financials['misc_monthly_cost']:,.2f}")
        print(f"Raw Tax Information from DB: '{financials['tax_info_raw']}'")
        print(f"Raw Estimated Rent from DB: {property_data.get('estimated_rent_raw')} (used as ${financials['estimated_monthly_rent']:,.2f} monthly in calculation)")

        print("--- Monthly Breakdown ---")
        print(f"Principal & Interest (P&I): ${financials['monthly_p_and_i']:,.2f}")
        
        if financials['annual_taxes'] is not None:
            print(f"Taxes: ${financials['monthly_taxes']:,.2f} (derived from '{financials['tax_info_raw']}')")
        else:
            print(f"Taxes: ${financials['monthly_taxes']:,.2f} (Warning: Could not parse tax data: '{financials['tax_info_raw']}')")

        print(f"Insurance: ${financials['monthly_insurance']:,.2f}")
        print(f"Misc Costs: ${financials['misc_monthly_cost']:,.2f}")
        print(f"Total Estimated Monthly Expenses: ${financials['total_monthly_expenses']:,.2f}")
        
        print("--- Cashflow ---")
        print(f"Estimated Monthly Rent: ${financials['estimated_monthly_rent']:,.2f}") # Use the value from financials dict
        print(f"Net Estimated Monthly Cashflow: ${financials['net_monthly_cashflow']:,.2f}")
        print("-------------------------\n")
        
    # --------------------------------------------------------------
    # Enhanced output when using dynamic CapEx
    # --------------------------------------------------------------
    else:
        # Define formatting helpers
        def hr(char='=', length=80):
            return char * length
        
        def section_title(title):
            padding = (80 - len(title) - 4) // 2
            return f"\n{hr('=', padding)} {title} {hr('=', padding)}"
        
        def format_currency(amount):
            return f"${amount:,.2f}"
        
        def format_percent(amount):
            return f"{amount:.2f}%"
        
        def format_label_value(label, value, width=35):
            return f"{label:<{width}} {value}"
        
        # Try to determine if terminal supports color
        use_color = True
        try:
            import os
            use_color = os.isatty(1)  # Check if stdout is a terminal
        except:
            use_color = False
        
        pos_color = '\033[92m'  # Green
        neg_color = '\033[91m'  # Red
        bold = '\033[1m'
        end_color = '\033[0m'
        
        def colorize(text, color_code):
            if use_color:
                return f"{color_code}{text}{end_color}"
            return text
        
        # Format values with color if they're positive or negative
        def format_currency_with_color(amount):
            formatted = format_currency(amount)
            if amount > 0:
                return colorize(formatted, pos_color)
            elif amount < 0:
                return colorize(formatted, neg_color)
            return formatted
        
        # Calculate profitability status
        is_profitable = financials['net_monthly_cashflow'] > 0
        profit_status = colorize("✓ PROFITABLE", pos_color) if is_profitable else colorize("✗ NEGATIVE CASHFLOW", neg_color)
        
        # Output the enhanced analysis
        print(hr())
        print(colorize(f"ENHANCED INVESTMENT PROPERTY CASHFLOW ANALYSIS", bold))
        print(hr())
        print(f"Property: {args.address}")
        print(f"Analysis Date: {datetime.datetime.now().strftime('%B %d, %Y')}")
        print(f"Status: {profit_status}")
        print(hr())
        
        print(section_title("PROPERTY DETAILS"))
        print(format_label_value("Purchase Price:", format_currency(financials['purchase_price'])))
        print(format_label_value("Square Footage:", f"{financials['square_feet']:.0f} sq ft"))
        print(format_label_value("Property Age:", f"{financials['property_age']} years"))
        print(format_label_value("Property Condition:", financials['property_condition'].upper()))
        print(format_label_value("Down Payment:", f"{format_currency(financials['down_payment_amount'])} ({format_percent(financials['down_payment_percentage'])})"))
        print(format_label_value("Loan Amount:", format_currency(financials['loan_amount'])))
        print(format_label_value("Interest Rate:", format_percent(financials['annual_interest_rate_percent'])))
        print(format_label_value("Loan Term:", f"{financials['loan_term_years']} years"))
        
        print(section_title("MONTHLY INCOME"))
        print(format_label_value("Gross Rental Income:", format_currency(financials['estimated_monthly_rent'])))
        print(format_label_value("Vacancy Loss:", f"{format_currency(financials['estimated_monthly_rent'] - financials['effective_rent_after_vacancy'])} ({format_percent(financials['vacancy_rate_percent'])})"))
        print(format_label_value("Effective Rental Income:", format_currency(financials['effective_rent_after_vacancy'])))
        
        print(section_title("MONTHLY EXPENSES"))
        print(format_label_value("Mortgage (P&I):", format_currency(financials['monthly_p_and_i'])))
        
        if financials['annual_taxes'] is not None:
            print(format_label_value("Property Taxes:", format_currency(financials['monthly_taxes'])))
        else:
            tax_warning = f"{format_currency(financials['monthly_taxes'])} (Warning: Could not parse tax data)"
            print(format_label_value("Property Taxes:", tax_warning))
        
        print(format_label_value("Insurance:", format_currency(financials['monthly_insurance'])))
        print(format_label_value("Property Management:", f"{format_currency(financials['monthly_property_mgmt'])} ({format_percent(financials['property_mgmt_fee_percent'])})"))
        
        # Maintenance reserve details
        print(format_label_value("Maintenance Reserve:", f"{format_currency(financials['monthly_maintenance'])} ({format_percent(financials['adjusted_maintenance_percent'])} annual)"))
        print(f"   - Base rate: {format_percent(financials['maintenance_percent'])}")
        print(f"   - Adjusted by age factor: {get_age_multiplier(financials['property_age']):.2f}x")
        print(f"   - Adjusted by condition factor: {CONDITION_MULTIPLIERS[financials['property_condition']]:.2f}x")
        
        # CapEx reserve details
        print(format_label_value("CapEx Reserve:", f"{format_currency(financials['monthly_capex'])}"))
        print(f"   - Calculated as: {format_percent(financials['adjusted_capex_percent'])} of property value")
        print(f"   - Based on detailed component analysis (see below)")
        
        print(format_label_value("Utilities:", format_currency(financials['utilities_monthly'])))
        print(format_label_value("Miscellaneous:", format_currency(financials['misc_monthly_cost'])))
        print(hr('-'))
        print(format_label_value("Total Monthly Expenses:", format_currency(financials['total_monthly_expenses'])))
        
        print(section_title("CASHFLOW SUMMARY"))
        print(format_label_value("Monthly Income:", format_currency(financials['effective_rent_after_vacancy'])))
        print(format_label_value("Monthly Expenses:", format_currency(financials['total_monthly_expenses'])))
        print(format_label_value("Net Monthly Cashflow:", format_currency_with_color(financials['net_monthly_cashflow'])))
        print(format_label_value("Annual Cashflow:", format_currency_with_color(financials['annual_cashflow'])))
        
        print(section_title("INVESTMENT METRICS"))
        print(format_label_value("Cash-on-Cash Return:", format_percent(financials['cash_on_cash_roi'])))
        print(format_label_value("Annual NOI:", format_currency(financials['annual_noi'])))
        print(format_label_value("Cap Rate:", format_percent(financials['cap_rate'])))
        
        # Display detailed CapEx breakdown if using dynamic calculation
        if financials['capex_reserve']:
            print(section_title("DETAILED CAPEX BREAKDOWN"))
            # Define column widths for the CapEx table
            col_component = 24
            col_repl_cost = 18
            col_lifespan = 12
            col_monthly_res = 18

            header = f"{'Component':<{col_component}} {'Replacement Cost':>{col_repl_cost}} {'Lifespan':>{col_lifespan}} {'Monthly Reserve':>{col_monthly_res}}"
            print(header)
            print(hr('-'))
            capex_components = financials['capex_reserve']['components']
            for component, details in sorted(capex_components.items()): # Sort for consistent order
                component_name = component.replace('_', ' ').title()
                repl_cost_str = format_currency(details['replacement_cost'])
                lifespan_str = f"{details['lifespan_years']:.1f} yrs"
                monthly_res_str = format_currency(details['monthly_reserve'])
                print(f"{component_name:<{col_component}} {repl_cost_str:>{col_repl_cost}} {lifespan_str:>{col_lifespan}} {monthly_res_str:>{col_monthly_res}}")
            print(hr('-'))
            total_monthly_capex_str = format_currency(financials['monthly_capex'])
            print(format_label_value("Total Monthly CapEx Reserve:", total_monthly_capex_str))
        
        # Deal Analysis Summary - Quick reference for decision making
        print(section_title("DEAL ANALYSIS"))
        coc_rating = "Excellent" if financials['cash_on_cash_roi'] > 12 else "Good" if financials['cash_on_cash_roi'] > 8 else "Fair" if financials['cash_on_cash_roi'] > 5 else "Poor"
        cap_rating = "Excellent" if financials['cap_rate'] > 8 else "Good" if financials['cap_rate'] > 6 else "Fair" if financials['cap_rate'] > 4 else "Poor"
        cashflow_per_unit = financials['net_monthly_cashflow']  # For multi-unit, you'd divide by # of units
        cashflow_rating = "Excellent" if cashflow_per_unit > 300 else "Good" if cashflow_per_unit > 200 else "Fair" if cashflow_per_unit > 100 else "Poor"
        
        print(format_label_value("Cash-on-Cash Rating:", f"{coc_rating} ({format_percent(financials['cash_on_cash_roi'])})"))
        print(format_label_value("Cap Rate Rating:", f"{cap_rating} ({format_percent(financials['cap_rate'])})"))
        print(format_label_value("Cashflow Rating:", f"{cashflow_rating} ({format_currency(cashflow_per_unit)}/month)"))
        
        # Final assessment
        print(hr())
        if financials['cash_on_cash_roi'] > 8 and financials['cap_rate'] > 6 and cashflow_per_unit > 200:
            print(colorize("SUMMARY: Strong investment opportunity with good returns.", pos_color))
        elif financials['cash_on_cash_roi'] > 5 and financials['cap_rate'] > 4 and cashflow_per_unit > 100:
            print(colorize("SUMMARY: Decent investment with moderate returns.", pos_color))
        elif financials['net_monthly_cashflow'] > 0:
            print(colorize("SUMMARY: Marginal investment. Consider negotiating better terms.", pos_color))
        else:
            print(colorize("SUMMARY: Negative cashflow. Not recommended as a rental investment.", neg_color))
        
        print(hr())

if __name__ == "__main__":
    print("DEBUG: Script __main__ block started.", flush=True)

    # Initial, minimal parsing to get config_path.
    # This allows the config file to influence other argument defaults.
    # We use add_help=False to avoid conflicts with the main parser later.
    temp_parser = argparse.ArgumentParser(add_help=False) 
    temp_parser.add_argument(
        "--config-path", 
        default=str(DEFAULT_CONFIG_PATH), 
        type=str,
        help="Path to the JSON configuration file." # Help text for completeness
    )
    # Parse only known args to avoid errors if other required args are missing at this stage
    known_args, _ = temp_parser.parse_known_args() 
    
    actual_config_path = Path(known_args.config_path)
    print(f"DEBUG: Determined config path to use for loading defaults: {actual_config_path}", flush=True)
    
    # Load the configuration using the determined path
    config = load_config(actual_config_path)
    if not config and actual_config_path != DEFAULT_CONFIG_PATH:
        print(f"Warning: Specified config file '{actual_config_path}' was not found or is invalid. Trying default config.")
        config = load_config(DEFAULT_CONFIG_PATH)
        if config:
            print(f"DEBUG: Successfully loaded default config from '{DEFAULT_CONFIG_PATH}': {config}", flush=True)
        else:
            print(f"DEBUG: Default config '{DEFAULT_CONFIG_PATH}' also not found or invalid. Proceeding with empty config.", flush=True)
            config = {} # Ensure config is a dict
    elif not config and actual_config_path == DEFAULT_CONFIG_PATH:
         print(f"DEBUG: Default config file '{actual_config_path}' not found or invalid. Proceeding with empty config.", flush=True)
         config = {} # Ensure config is a dict
    else:
        print(f"DEBUG: Config loaded successfully from '{actual_config_path}': {config}", flush=True)

    # Now parse all arguments. The 'parse_arguments' function will use the
    # 'config' dictionary (loaded above) to set default values for arguments.
    args = parse_arguments(config) 
    print(f"DEBUG: Arguments parsed: {args}", flush=True)
    print(f"DEBUG: Using effective config path for operations: {args.config_path}", flush=True)
    print(f"DEBUG: use_dynamic_capex set to: {args.use_dynamic_capex}", flush=True)


    if args.capex_guide:
        print("DEBUG: --capex-guide flag detected. Printing guide.", flush=True)
        print_capex_guide()
        sys.exit(0) # Normal exit after printing guide

    print(f"DEBUG: Fetching property data for address: '{args.address}' from DB: '{args.db_path}'", flush=True)
    property_data = fetch_property_data(args.db_path, args.address)

    if property_data:
        print(f"DEBUG: Property data fetched: {property_data}", flush=True)
        print("DEBUG: Calling calculate_and_print_cashflow...", flush=True)
        calculate_and_print_cashflow(args, property_data)
        print("DEBUG: calculate_and_print_cashflow finished.", flush=True)
    else:
        # Error message is printed by fetch_property_data if address not found
        print(f"DEBUG: Main block: Failed to fetch property data for address '{args.address}'. Script will exit.", flush=True)
        sys.exit(1) # Exit if property data is not found

    print("DEBUG: Script finished successfully.", flush=True)