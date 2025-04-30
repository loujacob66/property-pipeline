#!/usr/bin/env python3
"""
Backup the database before making changes
"""

import sqlite3
import os
import shutil
from pathlib import Path
from datetime import datetime

def backup_database():
    """Create a timestamped backup of the database"""
    db_path = Path(__file__).parent.parent / "data" / "listings.db"
    backup_dir = Path(__file__).parent.parent / "data" / "backups"
    
    # Create backup directory if it doesn't exist
    backup_dir.mkdir(exist_ok=True)
    
    # Create timestamped backup filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"listings_{timestamp}.db"
    
    try:
        # Copy the database file
        shutil.copy2(db_path, backup_path)
        print(f"✅ Database backed up to: {backup_path}")
        
        # Verify the backup
        conn = sqlite3.connect(backup_path)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM listings")
        count = c.fetchone()[0]
        conn.close()
        
        print(f"✅ Backup verified - contains {count} listings")
        return backup_path
        
    except Exception as e:
        print(f"❌ Error creating backup: {str(e)}")
        return None

if __name__ == "__main__":
    backup_database() 