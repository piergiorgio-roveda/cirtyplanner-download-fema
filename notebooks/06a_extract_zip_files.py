#!/usr/bin/env python3
"""
Script 06a: Extract FEMA Flood Risk ZIP Files Only

This script processes ZIP files downloaded by script 05:
1. Extracts ZIP files containing FEMA shapefiles
2. Logs extraction results to database with detailed shapefile tracking
3. Stops after extraction (no merging)

This is a simplified version of script 06 that only handles extraction.
Each ZIP file is extracted to a folder named after the product name.
No categorization or geometry type detection is performed.

Prerequisites:
    - Script 04: Database with shapefile metadata
    - Script 05: Downloaded ZIP files
    - config.json file must exist

Dependencies:
    Standard Python libraries only (no GeoPandas/Fiona required)

Usage:
    python notebooks/06a_extract_zip_files.py
    
    # With custom configuration:
    python notebooks/06a_extract_zip_files.py --config custom_config.json
    
    # Process specific states only:
    python notebooks/06a_extract_zip_files.py --states 01,02,04
    
    # Resume interrupted processing:
    python notebooks/06a_extract_zip_files.py --resume
    
    # Force rebuild from scratch:
    python notebooks/06a_extract_zip_files.py --force-rebuild
"""

import sqlite3
import zipfile
import os
import json
import shutil
import argparse
import logging
from pathlib import Path
from datetime import datetime
import psutil
import gc
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

class ProcessingError(Exception):
    """Base class for processing errors."""
    pass

class ZipExtractionError(ProcessingError):
    """ZIP file extraction failed."""
    pass

class MemoryMonitor:
    """Monitor and manage memory usage during processing."""
    
    def __init__(self, limit_mb=2048):
        self.limit_mb = limit_mb
        
    def check_memory_usage(self):
        """Check current memory usage and trigger cleanup if needed."""
        memory_usage = psutil.virtual_memory().percent
        if memory_usage > 80:  # If using more than 80% of available memory
            self.force_garbage_collection()
            return True
        return False
        
    def force_garbage_collection(self):
        """Force garbage collection and memory cleanup."""
        gc.collect()

def setup_logging(verbose=False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('extraction_06a.log'),
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
        required_sections = ['download', 'processing', 'database']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        return config
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
    except Exception as e:
        raise ValueError(f"Error reading config file {config_path}: {e}")

def setup_database(config):
    """Setup database connection and create processing tables."""
    db_path = config['database']['path']
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}. Please run scripts 04-05 first.")
    
    conn = sqlite3.connect(db_path)
    create_processing_tables(conn)
    return conn

def create_processing_tables(conn):
    """Create tables for tracking extraction."""
    cursor = conn.cursor()
    
    # Track ZIP extraction status with detailed shapefile logging
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS extraction_06a_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            zip_file_path TEXT NOT NULL,
            extracted_path TEXT,
            shapefile_name TEXT,
            extraction_success BOOLEAN NOT NULL,
            extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_extraction_06a_product ON extraction_06a_log (product_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_extraction_06a_success ON extraction_06a_log (extraction_success)')
    
    conn.commit()

def extract_zip_file(product_name, zip_path, extract_to, conn, logger):
    """Extract ZIP file and log each shapefile found."""
    try:
        logger.info(f"Extracting: {product_name}")
        
        # Create extraction directory named after product
        extract_dir = os.path.join(extract_to, product_name)
        os.makedirs(extract_dir, exist_ok=True)
        
        extracted_shapefiles = []
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract all files
            zip_ref.extractall(extract_dir)
            extracted_files = zip_ref.namelist()
            
            # Find and log each shapefile
            for filename in extracted_files:
                if filename.lower().endswith('.shp'):
                    # Get the relative path within the ZIP
                    extracted_path = filename
                    # Get just the shapefile name
                    shapefile_name = os.path.basename(filename)
                    
                    extracted_shapefiles.append({
                        'extracted_path': extracted_path,
                        'shapefile_name': shapefile_name
                    })
        
        # Log each shapefile to database
        cursor = conn.cursor()
        for shapefile in extracted_shapefiles:
            cursor.execute('''
                INSERT INTO extraction_06a_log 
                (product_name, zip_file_path, extracted_path, shapefile_name, extraction_success)
                VALUES (?, ?, ?, ?, ?)
            ''', (product_name, zip_path, shapefile['extracted_path'], 
                  shapefile['shapefile_name'], True))
        
        # If no shapefiles found, still log the successful extraction
        if not extracted_shapefiles:
            cursor.execute('''
                INSERT INTO extraction_06a_log 
                (product_name, zip_file_path, extracted_path, shapefile_name, extraction_success)
                VALUES (?, ?, ?, ?, ?)
            ''', (product_name, zip_path, None, None, True))
        
        conn.commit()
        
        logger.info(f"  ✓ Extracted {len(extracted_files)} files, found {len(extracted_shapefiles)} shapefiles")
        
        return {
            'success': True,
            'extract_dir': extract_dir,
            'extracted_files': len(extracted_files),
            'shapefiles_found': len(extracted_shapefiles)
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"  ✗ Extraction failed: {error_msg}")
        
        # Log failed extraction
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO extraction_06a_log 
            (product_name, zip_file_path, extracted_path, shapefile_name, extraction_success, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (product_name, zip_path, None, None, False, error_msg))
        conn.commit()
        
        return {
            'success': False,
            'error': error_msg
        }

def get_unique_zip_files(config, conn, target_states=None):
    """Get unique ZIP files using the specified query to avoid duplicates."""
    zip_files = []
    
    # Get unique downloaded files from database using the provided query
    cursor = conn.cursor()
    if target_states:
        # Modified query to include state filtering
        state_placeholders = ','.join(['?' for _ in target_states])
        cursor.execute(f'''
            SELECT dl.product_name, MIN(dl.file_path) AS file_path
            FROM download_log dl
            WHERE dl.file_path IS NOT NULL 
            AND dl.download_success = 1
            AND dl.state_code IN ({state_placeholders})
            GROUP BY dl.product_name
        ''', target_states)
    else:
        # Original query as specified
        cursor.execute('''
            SELECT dl.product_name, MIN(dl.file_path) AS file_path
            FROM download_log dl
            WHERE dl.file_path IS NOT NULL
            AND dl.download_success = 1
            GROUP BY dl.product_name
        ''')
    
    downloaded_files = cursor.fetchall()
    
    # Build ZIP file list from database records
    for product_name, file_path in downloaded_files:
        if file_path and os.path.exists(file_path) and file_path.lower().endswith('.zip'):
            zip_files.append({
                'product_name': product_name,
                'zip_path': file_path,
                'file_size': os.path.getsize(file_path)
            })
    
    return zip_files

def extract_all_zip_files(config, conn, target_states=None, logger=None):
    """Extract all unique ZIP files."""
    logger = logger or logging.getLogger(__name__)
    
    # Get list of unique ZIP files to process
    zip_files = get_unique_zip_files(config, conn, target_states)
    total_files = len(zip_files)
    
    if total_files == 0:
        logger.warning("No ZIP files found to extract")
        return {'extracted': 0, 'failed': 0, 'total': 0}
    
    logger.info(f"Found {total_files} unique ZIP files to extract")
    
    # Check for already extracted files
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT product_name FROM extraction_06a_log WHERE extraction_success = 1')
    already_extracted = set(row[0] for row in cursor.fetchall())
    
    # Filter out already extracted files
    zip_files_to_process = [zf for zf in zip_files if zf['product_name'] not in already_extracted]
    logger.info(f"Skipping {total_files - len(zip_files_to_process)} already extracted files")
    
    if len(zip_files_to_process) == 0:
        logger.info("All ZIP files already extracted")
        return {'extracted': 0, 'failed': 0, 'total': total_files, 'skipped': total_files}
    
    # Setup extraction directory
    extraction_base = config['processing']['extraction_base_path']
    os.makedirs(extraction_base, exist_ok=True)
    
    # Process ZIP files
    extracted_count = 0
    failed_count = 0
    memory_monitor = MemoryMonitor(config['processing']['memory_limit_mb'])
    
    for i, zip_file in enumerate(zip_files_to_process, 1):
        logger.info(f"[{i}/{len(zip_files_to_process)}] Processing: {zip_file['product_name']}")
        
        result = extract_zip_file(
            zip_file['product_name'],
            zip_file['zip_path'],
            extraction_base,
            conn,
            logger
        )
        
        if result['success']:
            extracted_count += 1
        else:
            failed_count += 1
        
        # Memory management
        if i % 10 == 0:
            memory_monitor.check_memory_usage()
            logger.info(f"Progress: {i}/{len(zip_files_to_process)} - Extracted: {extracted_count}, Failed: {failed_count}")
    
    logger.info(f"Extraction complete: {extracted_count} successful, {failed_count} failed")
    
    return {
        'extracted': extracted_count,
        'failed': failed_count,
        'total': len(zip_files_to_process),
        'skipped': total_files - len(zip_files_to_process)
    }

def generate_extraction_report(extraction_results, conn, logger=None):
    """Generate extraction summary report with database statistics."""
    logger = logger or logging.getLogger(__name__)
    
    logger.info("\n" + "=" * 80)
    logger.info("ZIP EXTRACTION REPORT")
    logger.info("=" * 80)
    
    # Extraction summary
    logger.info(f"\n📦 EXTRACTION SUMMARY:")
    logger.info(f"  Total unique ZIP files found: {extraction_results.get('total', 0)}")
    logger.info(f"  Successfully extracted: {extraction_results.get('extracted', 0)}")
    logger.info(f"  Failed extractions: {extraction_results.get('failed', 0)}")
    logger.info(f"  Already extracted (skipped): {extraction_results.get('skipped', 0)}")
    
    # Database statistics
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM extraction_06a_log WHERE extraction_success = 1')
    total_successful_shapefiles = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT product_name) FROM extraction_06a_log WHERE extraction_success = 1')
    total_products_extracted = cursor.fetchone()[0]
    
    logger.info(f"\n📊 DATABASE STATISTICS:")
    logger.info(f"  Total products extracted: {total_products_extracted}")
    logger.info(f"  Total shapefiles logged: {total_successful_shapefiles}")
    
    # Success rate
    total_processed = extraction_results.get('total', 0)
    if total_processed > 0:
        extraction_success_rate = (extraction_results.get('extracted', 0) / total_processed) * 100
        logger.info(f"\n📈 SUCCESS RATE:")
        logger.info(f"  Extraction success: {extraction_success_rate:.1f}%")
    
    logger.info("\n" + "=" * 80)
    logger.info("EXTRACTION COMPLETED - Files are ready for merging with script 06")
    logger.info("=" * 80)

def clear_extraction_logs(conn, target_states=None, logger=None):
    """Clear extraction logs to force rebuild from scratch."""
    logger = logger or logging.getLogger(__name__)
    
    cursor = conn.cursor()
    
    if target_states:
        # For state-specific clearing, we need to identify products from those states
        state_placeholders = ','.join(['?' for _ in target_states])
        cursor.execute(f'''
            DELETE FROM extraction_06a_log
            WHERE product_name IN (
                SELECT DISTINCT dl.product_name
                FROM download_log dl
                WHERE dl.state_code IN ({state_placeholders})
            )
        ''', target_states)
        
        logger.info(f"Cleared extraction logs for states: {target_states}")
    else:
        # Clear all extraction logs
        cursor.execute('DELETE FROM extraction_06a_log')
        
        logger.info("Cleared all extraction logs")
    
    conn.commit()
    
    # Get counts after clearing
    cursor.execute('SELECT COUNT(*) FROM extraction_06a_log')
    extraction_count = cursor.fetchone()[0]
    
    logger.info(f"Remaining extraction logs: {extraction_count}")

def cleanup_temporary_files(config, logger=None):
    """Clean up temporary extraction files if needed.
    
    IMPORTANT: This function ONLY removes temporary processing files.
    It NEVER touches the original ZIP files in the download directory.
    """
    logger = logger or logging.getLogger(__name__)
    
    temp_dir = config['processing'].get('temp_directory')
    
    # SAFETY CHECK: Never clean up the download directory with original ZIP files
    download_dir = config['download']['base_path']
    extraction_dir = config['processing']['extraction_base_path']
    
    # Only clean temp directory, preserve extraction results and downloads
    if temp_dir and temp_dir != download_dir and temp_dir != extraction_dir:
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up {temp_dir}: {e}")

def main():
    """Main extraction function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract FEMA flood risk ZIP files')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    parser.add_argument('--states', help='Comma-separated list of state codes to process (e.g., 01,02,04)')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted processing')
    parser.add_argument('--force-rebuild', action='store_true', help='Force rebuild - clear extraction logs and start from scratch')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without doing it')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--no-cleanup', action='store_true', help='Skip cleanup of temporary files')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.verbose)
    
    # Parse target states
    target_states = None
    if args.states:
        target_states = [s.strip() for s in args.states.split(',')]
        logger.info(f"Processing specific states: {target_states}")
    
    try:
        # Phase 1: Setup and Initialization
        logger.info("=== FEMA ZIP File Extraction (Script 06a) ===")
        logger.info(f"Start time: {datetime.now()}")
        
        config = load_config(args.config)
        conn = setup_database(config)
        
        # Handle force rebuild option
        if args.force_rebuild:
            logger.info("FORCE REBUILD MODE - Clearing extraction logs")
            clear_extraction_logs(conn, target_states, logger)
            
            # Also remove existing extracted files for target states
            extraction_base = config['processing']['extraction_base_path']
            download_dir = config['download']['base_path']
            
            # Safety check: Ensure we're not accidentally targeting the download directory
            if extraction_base == download_dir:
                logger.error(f"SAFETY ERROR: Extraction directory same as download directory - ABORTING FORCE REBUILD")
                return
            
            if target_states:
                # For state-specific rebuild, we need to identify products from those states
                cursor = conn.cursor()
                state_placeholders = ','.join(['?' for _ in target_states])
                cursor.execute(f'''
                    SELECT DISTINCT dl.product_name
                    FROM download_log dl
                    WHERE dl.state_code IN ({state_placeholders})
                ''', target_states)
                products_to_remove = [row[0] for row in cursor.fetchall()]
                
                for product_name in products_to_remove:
                    product_dir = os.path.join(extraction_base, product_name)
                    if os.path.exists(product_dir):
                        try:
                            shutil.rmtree(product_dir)
                            logger.info(f"Removed existing extraction directory: {product_dir}")
                        except Exception as e:
                            logger.warning(f"Failed to remove {product_dir}: {e}")
            else:
                if os.path.exists(extraction_base):
                    try:
                        shutil.rmtree(extraction_base)
                        logger.info(f"Removed existing extraction directory: {extraction_base}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {extraction_base}: {e}")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No actual processing will be performed")
            zip_files = get_unique_zip_files(config, conn, target_states)
            logger.info(f"Would process {len(zip_files)} unique ZIP files")
            return
        
        # Phase 2: ZIP File Extraction
        logger.info("\n=== ZIP File Extraction ===")
        extraction_results = extract_all_zip_files(config, conn, target_states, logger)
        
        # Final Summary
        generate_extraction_report(extraction_results, conn, logger)
        
        # Cleanup temporary files
        if not args.no_cleanup:
            logger.info("\n=== Cleanup ===")
            cleanup_temporary_files(config, logger)
        
        logger.info(f"\nExtraction completed at: {datetime.now()}")
        logger.info("Next step: Run script 06 for shapefile merging")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()