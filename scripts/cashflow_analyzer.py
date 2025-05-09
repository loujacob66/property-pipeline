#!/usr/bin/env python3
"""
Cashflow Analyzer for Real Estate Properties

This script calculates the estimated monthly cashflow for a property based on
purchase price, loan details, taxes, rent, and other expenses.

Usage:
    python scripts/cashflow_analyzer.py --address "<FULL_ADDRESS>" \
                                        --down-payment <PERCENT> \
                                        --rate <ANNUAL_RATE> \
                                        --insurance <ANNUAL_INSURANCE> \
                                        --misc-monthly <MONTHLY_MISC> \
                                        [--loan-term <YEARS>] \
                                        [--db-path <PATH_TO_DB>]
"""

import argparse
import sqlite3
import re
import json
from pathlib import Path

# Constants
ROOT = Path(__file__).parent.parent
DEFAULT_DB_PATH = ROOT / "data" / "listings.db"
DEFAULT_CONFIG_PATH = ROOT / "config" / "cashflow_config.json"

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
    parser = argparse.ArgumentParser(description="Real Estate Cashflow Analyzer")
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
        help=f"Down payment amount in dollars (e.g., 50000). Default from config: {config.get('down_payment')}"
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
        cursor.execute(
            "SELECT price, tax_information, estimated_rent, id FROM listings WHERE address = ?",
            (address,)
        )
        property_data_row = cursor.fetchone()
        if property_data_row:
            return {
                "price": property_data_row[0],
                "tax_information_raw": property_data_row[1],
                "estimated_rent_raw": property_data_row[2],
                "id": property_data_row[3] # Also fetching ID for potential future use or logging
            }
        else:
            print(f"Error: Property with address '{address}' not found.")
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

def calculate_financial_components(
    purchase_price, 
    tax_info_raw, 
    estimated_monthly_rent, 
    down_payment_input_dollars, 
    annual_interest_rate_percent, 
    loan_term_years, 
    annual_insurance_cost, 
    misc_monthly_cost
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

    # 5. Total Monthly Expenses
    total_monthly_expenses = monthly_p_and_i + monthly_taxes + monthly_insurance + misc_monthly_cost

    # 6. Net Monthly Cashflow
    net_monthly_cashflow = effective_rent - total_monthly_expenses
    
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
        "estimated_monthly_rent": effective_rent, # Use effective_rent
        "monthly_p_and_i": monthly_p_and_i,
        "annual_taxes": annual_taxes,
        "monthly_taxes": monthly_taxes,
        "monthly_insurance": monthly_insurance,
        "total_monthly_expenses": total_monthly_expenses,
        "net_monthly_cashflow": net_monthly_cashflow
    }

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
        misc_monthly_cost=args.misc_monthly
    )

    if financials is None:
        # Error message already printed by calculate_financial_components or preceding checks
        return

    # Output
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

def main():
    """Main function to drive the script."""
    
    # Temporarily parse config_path first if provided, to load config before full parsing
    temp_parser = argparse.ArgumentParser(add_help=False)
    temp_parser.add_argument("--config-path", type=str, default=str(DEFAULT_CONFIG_PATH))
    temp_args, _ = temp_parser.parse_known_args()

    config = load_config(temp_args.config_path)
    args = parse_arguments(config) # Pass loaded config to arg parser
    
    print(f"Analyzing property at address: {args.address}")
    print(f"Using database: {args.db_path}")
    if Path(args.config_path).exists():
        print(f"Using configuration file: {args.config_path}")
    else:
        print(f"Info: Configuration file '{args.config_path}' not found. Using command-line arguments and built-in defaults.")


    property_data = fetch_property_data(args.db_path, args.address)

    if property_data:
        calculate_and_print_cashflow(args, property_data)
    else:
        # Error message already printed by fetch_property_data
        return

if __name__ == "__main__":
    main() 