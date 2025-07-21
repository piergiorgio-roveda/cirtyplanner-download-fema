#!/usr/bin/env python3
"""
Script 06d: Merge GeoPackage Files by Filename Group

This script merges GeoPackage files from the clean_conversion_table based on filename groups:
1. Reads clean_conversion_table to get files with a specific filename
2. Uses ogr2ogr to merge these files into a single GeoPackage file
3. Creates a merged output for each specified filename group

Prerequisites:
    - Script 06c: Created clean_conversion_table and gpkg_filename_groups
    - OSGeo4W console or environment with ogr2ogr available in PATH
      (This script must be run from an OSGeo4W console to have access to ogr2ogr)
    - config.json file must exist

Usage:
    python notebooks/06d_merge_gpkg_files.py
    
    # With custom configuration:
    python notebooks/06d_merge_gpkg_files.py --config custom_config.json
    
    # Process specific filename groups only:
    python notebooks/06d_merge_gpkg_files.py --filenames S_FRD_Proj_Ar,S_HUC_Ar
    
    # Force rebuild (overwrite existing merged files):
    python notebooks/06d_merge_gpkg_files.py --force-rebuild
"""

import sqlite3
import os
import json
import argparse
import logging
import subprocess
import uuid
from pathlib import Path
from datetime import datetime

# Hardcoded list of filename groups to process
# These are the filenames without extension that will be merged
DEFAULT_FILENAME_GROUPS = [
    'S_FRD_Proj_Ar',
    'S_HUC_Ar',
    'S_CSLF_Ar'
]

def setup_logging(verbose=False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('merge_gpkg_06d.log'),
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
        required_sections = ['database', 'processing']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Add merged_gpkg_path if not present
        if 'processing' in config and 'merged_gpkg_path' not in config['processing']:
            config['processing']['merged_gpkg_path'] = 'E:\\FEMA_MERGED'
            
        # Add temp_merge_path if not present
        if 'processing' in config and 'temp_merge_path' not in config['processing']:
            config['processing']['temp_merge_path'] = os.path.join(
                'D:\\git\\cityplanner-desktop\\download-fema\\.TMP_MERGE'
            )
            
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

def setup_database(conn):
    """Setup database connection and create merge tracking table."""
    cursor = conn.cursor()
    
    # Create merge tracking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS merge_06d_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename_group TEXT NOT NULL,
            source_files_count INTEGER NOT NULL,
            merged_gpkg_path TEXT NOT NULL,
            merge_success BOOLEAN NOT NULL,
            merge_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_merge_06d_filename ON merge_06d_log (filename_group)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_merge_06d_success ON merge_06d_log (merge_success)')
    
    conn.commit()

def get_files_by_filename(conn, filename, logger):
    """Get all GeoPackage files with the specified filename from clean_conversion_table."""
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT product_name, gpkg_path
        FROM clean_conversion_table
        WHERE filename = ?
    ''', (filename,))
    
    files = cursor.fetchall()
    logger.info(f"Found {len(files)} files with filename '{filename}'")
    
    return files

def merge_gpkg_files(files, filename, output_path, temp_dir, logger, force_rebuild=False):
    """Merge GeoPackage files using ogr2ogr."""
    if not files:
        logger.warning(f"No files to merge for filename '{filename}'")
        return False, "No files to merge"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Create temporary directory if it doesn't exist
    os.makedirs(temp_dir, exist_ok=True)
    
    # Check if output file already exists
    if os.path.exists(output_path) and not force_rebuild:
        logger.info(f"Output file already exists: {output_path}")
        logger.info(f"Use --force-rebuild to overwrite")
        return False, "Output file already exists"
    
    try:
        # Create a unique temporary file
        unique_id = str(uuid.uuid4())[:8]
        temp_output = os.path.join(temp_dir, f"{filename}_{unique_id}.gpkg")
        
        # First file is handled differently - we copy it to the output
        first_file = files[0][1]  # gpkg_path of first file
        first_product = files[0][0]  # product_name of first file
        
        logger.info(f"Starting with first file: {first_product} - {os.path.basename(first_file)}")
        
        # Copy first file to temp output
        cmd = [
            'ogr2ogr',
            '-f', 'GPKG',
            temp_output,
            first_file
        ]
        
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        # Append each subsequent file
        for i, (product_name, gpkg_path) in enumerate(files[1:], 2):
            logger.info(f"Appending file {i}/{len(files)}: {product_name} - {os.path.basename(gpkg_path)}")
            
            cmd = [
                'ogr2ogr',
                '-f', 'GPKG',
                '-update',
                '-append',
                temp_output,
                gpkg_path
            ]
            
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        # Move the temp file to the final destination
        if os.path.exists(output_path):
            os.remove(output_path)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        os.rename(temp_output, output_path)
        
        logger.info(f"Successfully merged {len(files)} files into {output_path}")
        
        return True, None
        
    except subprocess.CalledProcessError as e:
        error_msg = f"ogr2ogr error: {e.stderr}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Error merging files: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def process_filename_group(conn, filename, config, logger, force_rebuild=False):
    """Process a single filename group."""
    logger.info(f"Processing filename group: {filename}")
    
    # Get files for this filename
    files = get_files_by_filename(conn, filename, logger)
    
    if not files:
        logger.warning(f"No files found for filename '{filename}'")
        return False
    
    # Setup output path
    merged_gpkg_path = config['processing']['merged_gpkg_path']
    output_path = os.path.join(merged_gpkg_path, f"{filename}.gpkg")
    
    # Setup temp directory
    temp_dir = config['processing']['temp_merge_path']
    
    # Merge files
    success, error_msg = merge_gpkg_files(files, filename, output_path, temp_dir, logger, force_rebuild)
    
    # Log result to database
    cursor = conn.cursor()
    
    if success:
        cursor.execute('''
            INSERT INTO merge_06d_log 
            (filename_group, source_files_count, merged_gpkg_path, merge_success)
            VALUES (?, ?, ?, ?)
        ''', (filename, len(files), output_path, True))
    else:
        cursor.execute('''
            INSERT INTO merge_06d_log 
            (filename_group, source_files_count, merged_gpkg_path, merge_success, error_message)
            VALUES (?, ?, ?, ?, ?)
        ''', (filename, len(files), output_path, False, error_msg))
    
    conn.commit()
    
    return success

def generate_report(conn, processed_filenames, logger):
    """Generate report with merge statistics."""
    cursor = conn.cursor()
    
    logger.info("\n" + "=" * 80)
    logger.info("GPKG MERGE REPORT")
    logger.info("=" * 80)
    
    # Get merge statistics
    cursor.execute('''
        SELECT 
            filename_group, 
            source_files_count, 
            merged_gpkg_path,
            merge_success
        FROM merge_06d_log
        WHERE filename_group IN ({})
        ORDER BY merge_timestamp DESC
    '''.format(','.join(['?'] * len(processed_filenames))), processed_filenames)
    
    results = cursor.fetchall()
    
    logger.info(f"\n[MERGE RESULTS]:")
    
    success_count = 0
    failed_count = 0
    
    for filename, source_count, path, success in results:
        status = "SUCCESS" if success else "FAILED"
        if success:
            success_count += 1
        else:
            failed_count += 1
            
        logger.info(f"  {filename}: {status} - {source_count} source files -> {os.path.basename(path)}")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"MERGE COMPLETED - {success_count} successful, {failed_count} failed")
    logger.info("=" * 80)

def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Merge GeoPackage files by filename group')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    parser.add_argument('--filenames', help='Comma-separated list of filename groups to process')
    parser.add_argument('--force-rebuild', action='store_true', help='Force rebuild - overwrite existing merged files')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.verbose)
    
    try:
        # Phase 1: Setup and Initialization
        logger.info("=== Merging GeoPackage Files by Filename Group (Script 06d) ===")
        logger.info(f"Start time: {datetime.now()}")
        
        config = load_config(args.config)
        conn = get_db_connection(config)
        
        # Setup database
        setup_database(conn)
        
        # Determine which filename groups to process
        if args.filenames:
            filename_groups = [f.strip() for f in args.filenames.split(',')]
            logger.info(f"Processing specific filename groups: {filename_groups}")
        else:
            filename_groups = DEFAULT_FILENAME_GROUPS
            logger.info(f"Processing default filename groups: {filename_groups}")
        
        # Phase 2: Process each filename group
        logger.info("\n=== Processing Filename Groups ===")
        
        success_count = 0
        failed_count = 0
        
        for filename in filename_groups:
            success = process_filename_group(conn, filename, config, logger, args.force_rebuild)
            
            if success:
                success_count += 1
            else:
                failed_count += 1
        
        # Phase 3: Generate report
        generate_report(conn, filename_groups, logger)
        
        logger.info(f"\nProcess completed at: {datetime.now()}")
        logger.info(f"Summary: {success_count} successful, {failed_count} failed")
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()