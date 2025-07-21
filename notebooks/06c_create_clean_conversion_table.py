#!/usr/bin/env python3
"""
Script 06c: Create Clean Conversion Table

This script creates a clean table from the conversion_06b_log table:
1. Extracts product_name and gpkg_path from conversion_06b_log
2. Extracts filename without path and extension from gpkg_path
3. Creates a new table with these fields

Prerequisites:
    - Script 06b: Converted shapefiles with conversion_06b_log entries
    - config.json file must exist

Usage:
    python notebooks/06c_create_clean_conversion_table.py
    
    # With custom configuration:
    python notebooks/06c_create_clean_conversion_table.py --config custom_config.json
    
    # Force rebuild (drop and recreate table):
    python notebooks/06c_create_clean_conversion_table.py --force-rebuild
"""

import sqlite3
import os
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime

def setup_logging(verbose=False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('clean_conversion_06c.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config(config_path='config.json'):
    """Load configuration from JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}. Please create config.json file.")
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required sections
        required_sections = ['database']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required configuration section: {section}")
            
        return config
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
    except Exception as e:
        raise ValueError(f"Error reading config file {config_path}: {e}")

def get_db_connection(config):
    """Create a database connection."""
    db_path = config['database']['path']
    conn = sqlite3.connect(db_path)
    return conn

def setup_database(conn, force_rebuild=False):
    """Setup database connection and create clean conversion table."""
    cursor = conn.cursor()
    
    # Drop table if force rebuild
    if force_rebuild:
        cursor.execute('DROP TABLE IF EXISTS clean_conversion_table')
    
    # Create clean conversion table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clean_conversion_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            gpkg_path TEXT NOT NULL,
            filename TEXT NOT NULL
        )
    ''')
    
    # Create filename groups table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS gpkg_filename_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            count INTEGER NOT NULL
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_clean_product ON clean_conversion_table (product_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_clean_filename ON clean_conversion_table (filename)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_gpkg_filename_groups ON gpkg_filename_groups (filename)')
    
    conn.commit()

def extract_filename(gpkg_path):
    """Extract filename without path and extension from gpkg_path."""
    # Get the basename (filename with extension)
    basename = os.path.basename(gpkg_path)
    
    # Remove the extension
    filename = os.path.splitext(basename)[0]
    
    # Convert to lowercase to handle case sensitivity
    filename = filename.lower()
    
    return filename

def populate_clean_table(conn, logger):
    """Populate clean_conversion_table from conversion_06b_log."""
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute('DELETE FROM clean_conversion_table')
    cursor.execute('DELETE FROM gpkg_filename_groups')
    
    # Get successful conversions from conversion_06b_log
    cursor.execute('''
        SELECT product_name, gpkg_path
        FROM conversion_06b_log
        WHERE conversion_success = 1
    ''')
    
    # Process results
    rows = cursor.fetchall()
    total_rows = len(rows)
    logger.info(f"Found {total_rows} successful conversions to process")
    
    # Insert into clean_conversion_table
    for i, (product_name, gpkg_path) in enumerate(rows, 1):
        filename = extract_filename(gpkg_path)
        
        cursor.execute('''
            INSERT INTO clean_conversion_table (product_name, gpkg_path, filename)
            VALUES (?, ?, ?)
        ''', (product_name, gpkg_path, filename))
        
        # Log progress
        if i % 100 == 0 or i == total_rows:
            logger.info(f"Progress: {i}/{total_rows} rows processed")
    
    conn.commit()
    
    # Get count of inserted rows
    cursor.execute('SELECT COUNT(*) FROM clean_conversion_table')
    inserted_count = cursor.fetchone()[0]
    
    logger.info(f"Successfully inserted {inserted_count} rows into clean_conversion_table")
    
    # Populate gpkg_filename_groups table
    logger.info("Populating gpkg_filename_groups table...")
    cursor.execute('''
        INSERT INTO gpkg_filename_groups (filename, count)
        SELECT filename, COUNT(*) as count
        FROM clean_conversion_table
        GROUP BY filename
        ORDER BY count DESC
    ''')
    
    conn.commit()
    
    # Get count of filename groups
    cursor.execute('SELECT COUNT(*) FROM gpkg_filename_groups')
    groups_count = cursor.fetchone()[0]
    
    logger.info(f"Successfully created {groups_count} filename groups")
    
    return inserted_count, groups_count

def generate_report(conn, inserted_count, groups_count, logger):
    """Generate report with table statistics."""
    cursor = conn.cursor()
    
    logger.info("\n" + "=" * 80)
    logger.info("CLEAN CONVERSION TABLE REPORT")
    logger.info("=" * 80)
    
    # Count by product
    cursor.execute('''
        SELECT product_name, COUNT(*) as count
        FROM clean_conversion_table
        GROUP BY product_name
        ORDER BY count DESC
        LIMIT 10
    ''')
    
    product_counts = cursor.fetchall()
    
    logger.info(f"\n[PRODUCT STATISTICS (Top 10)]:")
    for product_name, count in product_counts:
        logger.info(f"  {product_name}: {count} files")
    
    # Count by filename pattern
    cursor.execute('''
        SELECT filename, count
        FROM gpkg_filename_groups
        ORDER BY count DESC
        LIMIT 10
    ''')
    
    filename_counts = cursor.fetchall()
    
    logger.info(f"\n[FILENAME GROUPS (Top 10)]:")
    for filename, count in filename_counts:
        logger.info(f"  {filename}: {count} occurrences")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"CLEAN CONVERSION TABLE CREATED - {inserted_count} rows total")
    logger.info(f"GPKG FILENAME GROUPS CREATED - {groups_count} unique filenames")
    logger.info("=" * 80)

def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Create clean conversion table')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    parser.add_argument('--force-rebuild', action='store_true', help='Force rebuild - drop and recreate table')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.verbose)
    
    try:
        # Phase 1: Setup and Initialization
        logger.info("=== Creating Clean Conversion Table (Script 06c) ===")
        logger.info(f"Start time: {datetime.now()}")
        
        config = load_config(args.config)
        conn = get_db_connection(config)
        
        # Setup database
        setup_database(conn, args.force_rebuild)
        
        # Phase 2: Populate clean table
        logger.info("\n=== Populating Clean Conversion Table ===")
        inserted_count, groups_count = populate_clean_table(conn, logger)
        
        # Phase 3: Generate report
        generate_report(conn, inserted_count, groups_count, logger)
        
        logger.info(f"\nProcess completed at: {datetime.now()}")
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()