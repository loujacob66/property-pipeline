#!/usr/bin/env python3
"""
Price Change Analysis Script

This script analyzes price changes for properties grouped by their Gmail labels.
It shows statistics about price decreases and trends over time for each label group.

Usage:
    python analyze_price_changes.py [--days DAYS] [--min-change MIN_CHANGE]

Options:
    --days DAYS         Number of days to look back (default: 30)
    --min-change MIN    Minimum price change percentage to consider (default: 1.0)
"""

import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
import statistics

# Constants
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "listings.db"
MTN_TZ = pytz.timezone('America/Denver')

def get_price_changes(days_back=30, min_change_pct=1.0):
    """
    Get price changes grouped by Gmail label.
    
    Args:
        days_back (int): Number of days to look back
        min_change_pct (float): Minimum price change percentage to consider
        
    Returns:
        dict: Dictionary of label groups and their price change statistics
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Calculate the cutoff date
    cutoff_date = datetime.now(MTN_TZ) - timedelta(days=days_back)
    cutoff_timestamp = cutoff_date.timestamp()
    
    # Get all price changes within the time period
    cursor.execute("""
        WITH PriceChanges AS (
            SELECT 
                l.id,
                l.address,
                l.source,
                lc.field_name,
                lc.old_value,
                lc.new_value,
                lc.changed_at,
                CAST(lc.new_value AS FLOAT) - CAST(lc.old_value AS FLOAT) as price_diff,
                (CAST(lc.new_value AS FLOAT) - CAST(lc.old_value AS FLOAT)) / CAST(lc.old_value AS FLOAT) * 100 as change_pct
            FROM listing_changes lc
            JOIN listings l ON l.id = lc.listing_id
            WHERE lc.field_name = 'price'
            AND lc.changed_at >= ?
            AND ABS((CAST(lc.new_value AS FLOAT) - CAST(lc.old_value AS FLOAT)) / CAST(lc.old_value AS FLOAT) * 100) >= ?
            ORDER BY lc.changed_at DESC
        )
        SELECT * FROM PriceChanges
    """, (cutoff_timestamp, min_change_pct))
    
    changes = cursor.fetchall()
    
    # Group changes by label
    label_stats = defaultdict(lambda: {
        'total_changes': 0,
        'price_changes': [],
        'decreases': 0,
        'increases': 0,
        'avg_decrease_pct': 0,
        'avg_increase_pct': 0,
        'max_decrease': 0,
        'max_increase': 0,
        'properties': set()
    })
    
    for change in changes:
        listing_id, address, label, field, old_val, new_val, timestamp, diff, pct = change
        
        if label not in label_stats:
            label_stats[label] = {
                'total_changes': 0,
                'price_changes': [],
                'decreases': 0,
                'increases': 0,
                'avg_decrease_pct': 0,
                'avg_increase_pct': 0,
                'max_decrease': 0,
                'max_increase': 0,
                'properties': set()
            }
        
        stats = label_stats[label]
        stats['total_changes'] += 1
        stats['price_changes'].append((timestamp, pct))
        stats['properties'].add(address)
        
        if pct < 0:
            stats['decreases'] += 1
            stats['max_decrease'] = min(stats['max_decrease'], pct)
        else:
            stats['increases'] += 1
            stats['max_increase'] = max(stats['max_increase'], pct)
    
    # Calculate averages for each label
    for label, stats in label_stats.items():
        decrease_pcts = [pct for _, pct in stats['price_changes'] if pct < 0]
        increase_pcts = [pct for _, pct in stats['price_changes'] if pct > 0]
        
        if decrease_pcts:
            stats['avg_decrease_pct'] = statistics.mean(decrease_pcts)
        if increase_pcts:
            stats['avg_increase_pct'] = statistics.mean(increase_pcts)
    
    conn.close()
    return label_stats

def print_analysis(stats, days_back):
    """Print the analysis results in a formatted way, including per-property breakdown."""
    print(f"\n📊 Price Change Analysis (Last {days_back} Days)")
    print("=" * 80)
    
    for label, data in stats.items():
        print(f"\n🏷️ Label Group: {label}")
        print("-" * 40)
        print(f"Total Properties: {len(data['properties'])}")
        print(f"Total Price Changes: {data['total_changes']}")
        print(f"Price Decreases: {data['decreases']}")
        print(f"Price Increases: {data['increases']}")
        
        if data['decreases'] > 0:
            print(f"\n📉 Decrease Statistics:")
            print(f"  Average Decrease: {abs(data['avg_decrease_pct']):.1f}%")
            print(f"  Maximum Decrease: {abs(data['max_decrease']):.1f}%")
        
        if data['increases'] > 0:
            print(f"\n📈 Increase Statistics:")
            print(f"  Average Increase: {data['avg_increase_pct']:.1f}%")
            print(f"  Maximum Increase: {data['max_increase']:.1f}%")
        
        # Show recent changes
        if data['price_changes']:
            print("\n🕒 Recent Changes:")
            for timestamp, pct in sorted(data['price_changes'], reverse=True)[:5]:
                try:
                    dt = datetime.fromtimestamp(float(timestamp), MTN_TZ)
                except (ValueError, TypeError):
                    try:
                        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                        dt = MTN_TZ.localize(dt)
                    except Exception:
                        dt = str(timestamp)
                change_type = "decreased" if pct < 0 else "increased"
                if isinstance(dt, datetime):
                    print(f"  {dt.strftime('%Y-%m-%d %H:%M')} - Price {change_type} by {abs(pct):.1f}%")
                else:
                    print(f"  {dt} - Price {change_type} by {abs(pct):.1f}%")
        
        print("-" * 40)

        # Per-property breakdown
        print("\n📋 Per-Property Price Change Breakdown:")
        # We'll need to re-query for details, since the summary doesn't have all info
        # Connect to DB for this section
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        # Get all price changes for this label group
        cursor.execute("""
            SELECT l.address, lc.old_value, lc.new_value, lc.changed_at
            FROM listing_changes lc
            JOIN listings l ON l.id = lc.listing_id
            WHERE lc.field_name = 'price' AND l.source = ?
            AND lc.changed_at >= ?
            ORDER BY l.address, lc.changed_at DESC
        """, (label, (datetime.now(MTN_TZ) - timedelta(days=days_back)).timestamp()))
        rows = cursor.fetchall()
        # Group by address
        prop_changes = defaultdict(list)
        # Helper function to determine location
        def get_location(address):
            address_lower = address.lower()
            if any(street in address_lower for street in ['street', 'avenue', 'circle', 'court', 'drive', 'way', 'place']):
                return 'NW Denver'
            return 'Arvada'
        for address, old_val, new_val, changed_at in rows:
            # Format date
            try:
                dt = datetime.fromtimestamp(float(changed_at), MTN_TZ)
                date_str = dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                try:
                    dt = datetime.strptime(changed_at, "%Y-%m-%d %H:%M:%S")
                    dt = MTN_TZ.localize(dt)
                    date_str = dt.strftime('%Y-%m-%d')
                except Exception:
                    date_str = str(changed_at)
            # Calculate percent change
            try:
                pct = (float(new_val) - float(old_val)) / float(old_val) * 100
            except Exception:
                pct = None
            location = get_location(address)
            prop_changes[(location, address)].append((date_str, old_val, new_val, pct))
        
        # Print table header
        print(f"{'Location':<12} {'Address':<40} {'#Chg':<5} {'Date':<12} {'Old Price':>14} {'New Price':>14} {'% Change':>10}")
        print('-' * 110)
        
        def fmt_price(val):
            try:
                return f"${int(float(val)):,}"
            except Exception:
                return str(val)
        def fmt_pct(pct):
            if pct is None:
                return ''
            sign = '+' if pct > 0 else ''
            return f"{sign}{pct:.1f}%"
        
        # Sort by location and then by address
        for (location, address), changes in sorted(prop_changes.items()):
            first = True
            for date_str, old_val, new_val, pct in changes:
                old_fmt = fmt_price(old_val)
                new_fmt = fmt_price(new_val)
                pct_fmt = fmt_pct(pct)
                if first:
                    print(f"{location:<12} {address:<40} {len(changes):<5} {date_str:<12} {old_fmt:>14} {new_fmt:>14} {pct_fmt:>10}")
                    first = False
                else:
                    print(f"{'':<12} {address:<40} {len(changes):<5} {date_str:<12} {old_fmt:>14} {new_fmt:>14} {pct_fmt:>10}")
        print('-' * 110)
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Analyze price changes by Gmail label groups")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back")
    parser.add_argument("--min-change", type=float, default=1.0, 
                       help="Minimum price change percentage to consider")
    args = parser.parse_args()
    
    stats = get_price_changes(args.days, args.min_change)
    print_analysis(stats, args.days)

if __name__ == "__main__":
    main() 