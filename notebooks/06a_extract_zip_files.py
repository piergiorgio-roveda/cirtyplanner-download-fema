#!/usr/bin/env python3
"""
Script 06a: Extract FEMA Flood Risk ZIP Files Only

This script processes ZIP files downloaded by script 05:
1. Extracts ZIP files containing FEMA shapefiles
2. Categorizes shapefiles by type and geometry
3. Logs extraction results to database
4. Stops after extraction (no merging)

This is a simplified version of script 06 that only handles extraction.

Prerequisites:
    - Script 04: Database with shapefile metadata
    - Script 05: Downloaded ZIP files

Dependencies:
    pip install geopandas fiona shapely pyproj psutil

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

# Shapefile type categories and expected geometries
SHAPEFILE_CATEGORIES = {
    'RISK': {
        'R_UDF_Losses_by_Building': 'POLYGON',
        'R_UDF_Losses_by_Parcel': 'POLYGON', 
        'R_UDF_Losses_by_Point': 'POINT'
    },
    'SPATIAL': {
        'S_AOMI_Pt': 'POINT',
        'S_Carto_Ar': 'POLYGON',
        'S_Carto_Ln': 'LINESTRING',
        'S_CenBlk_Ar': 'POLYGON',
        'S_CSLF_Ar': 'POLYGON',
        'S_FRD_Pol_Ar': 'POLYGON',
        'S_FRD_Proj_Ar': 'POLYGON',
        'S_FRM_Callout_Ln': 'LINESTRING',
        'S_HUC_Ar': 'POLYGON',
        'S_UDF_Pt': 'POINT'
    }
}

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
            logging.FileHandler('extraction.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config(config_path='config.json'):
    """Load configuration from JSON file with processing extensions."""
    # Default configuration with all required sections
    default_config = {
        "download": {
            "base_path": "E:\\FEMA_DOWNLOAD",
            "rate_limit_seconds": 0.2,
            "chunk_size_bytes": 8192,
            "timeout_seconds": 30
        },
        "processing": {
            "extraction_base_path": "E:\\FEMA_EXTRACTED",
            "merged_output_path": "E:\\FEMA_MERGED",
            "temp_directory": "E:\\FEMA_TEMP",
            "target_crs": "EPSG:4326",
            "chunk_size_features": 10000,
            "memory_limit_mb": 2048,
            "parallel_processing": True,
            "max_workers": 4
        },
        "validation": {
            "geometry_validation": True,
            "fix_invalid_geometries": True,
            "skip_empty_geometries": True,
            "coordinate_precision": 6
        },
        "database": {
            "path": "meta_results/flood_risk_shapefiles.db"
        },
        "api": {
            "base_url": "https://msc.fema.gov",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    }
    
    if not os.path.exists(config_path):
        # Create default config file
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        print(f"Created default configuration file: {config_path}")
        return default_config
    
    try:
        with open(config_path, 'r') as f:
            user_config = json.load(f)
        
        # Merge user config with defaults (user config takes precedence)
        config = default_config.copy()
        for section, values in user_config.items():
            if section in config and isinstance(values, dict):
                config[section].update(values)
            else:
                config[section] = values
        
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
    """Create tables for tracking extraction and processing."""
    cursor = conn.cursor()
    
    # Track ZIP extraction status
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS extraction_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            county_code TEXT NOT NULL,
            product_name TEXT NOT NULL,
            zip_file_path TEXT NOT NULL,
            extraction_success BOOLEAN NOT NULL,
            extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extracted_files_count INTEGER DEFAULT 0,
            shapefiles_found TEXT,
            error_message TEXT,
            FOREIGN KEY (state_code) REFERENCES states (state_code),
            FOREIGN KEY (county_code) REFERENCES counties (county_code)
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_extraction_log_state ON extraction_log (state_code)')
    
    conn.commit()

def categorize_shapefile(filename):
    """Determine shapefile type and expected geometry."""
    base_name = os.path.splitext(filename)[0]
    for category, types in SHAPEFILE_CATEGORIES.items():
        if base_name in types:
            return base_name, types[base_name], category
    return base_name, 'UNKNOWN', 'OTHER'

def extract_zip_file(zip_path, extract_to, state_code, county_code, product_name, conn, logger):
    """Extract ZIP file and log results."""
    try:
        logger.info(f"Extracting: {os.path.basename(zip_path)}")
        
        # Create extraction directory
        extract_dir = os.path.join(extract_to, state_code, county_code, product_name)
        os.makedirs(extract_dir, exist_ok=True)
        
        extracted_files = []
        shapefiles_found = []
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract all files
            zip_ref.extractall(extract_dir)
            extracted_files = zip_ref.namelist()
            
            # Find shapefiles
            for filename in extracted_files:
                if filename.lower().endswith('.shp'):
                    shapefile_type, geometry_type, category = categorize_shapefile(filename)
                    shapefiles_found.append({
                        'filename': filename,
                        'type': shapefile_type,
                        'geometry': geometry_type,
                        'category': category
                    })
        
        # Log successful extraction
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO extraction_log 
            (state_code, county_code, product_name, zip_file_path, extraction_success, 
             extracted_files_count, shapefiles_found)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (state_code, county_code, product_name, zip_path, True, 
              len(extracted_files), json.dumps(shapefiles_found)))
        conn.commit()
        
        logger.info(f"  ✓ Extracted {len(extracted_files)} files, found {len(shapefiles_found)} shapefiles")
        
        return {
            'success': True,
            'extract_dir': extract_dir,
            'extracted_files': extracted_files,
            'shapefiles_found': shapefiles_found
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"  ✗ Extraction failed: {error_msg}")
        
        # Log failed extraction
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO extraction_log 
            (state_code, county_code, product_name, zip_file_path, extraction_success, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (state_code, county_code, product_name, zip_path, False, error_msg))
        conn.commit()
        
        return {
            'success': False,
            'error': error_msg
        }

def get_downloaded_zip_files(config, conn, target_states=None):
    """Get successfully downloaded ZIP files from database."""
    download_path = config['download']['base_path']
    zip_files = []
    
    # Get downloaded files from database (from script 05)
    cursor = conn.cursor()
    if target_states:
        state_placeholders = ','.join(['?' for _ in target_states])
        cursor.execute(f'''
            SELECT DISTINCT dl.state_code, dl.county_code, dl.product_name, dl.file_path, s.state_name
            FROM download_log dl
            JOIN states s ON dl.state_code = s.state_code
            WHERE dl.download_success = 1 AND dl.state_code IN ({state_placeholders})
        ''', target_states)
    else:
        cursor.execute('''
            SELECT DISTINCT dl.state_code, dl.county_code, dl.product_name, dl.file_path, s.state_name
            FROM download_log dl
            JOIN states s ON dl.state_code = s.state_code
            WHERE dl.download_success = 1
        ''')
    
    downloaded_files = cursor.fetchall()
    
    # Build ZIP file list from database records
    for state_code, county_code, product_name, file_path, state_name in downloaded_files:
        if file_path and os.path.exists(file_path) and file_path.lower().endswith('.zip'):
            zip_files.append({
                'state_code': state_code,
                'state_name': state_name,
                'county_code': county_code,
                'product_name': product_name,
                'zip_path': file_path,
                'file_size': os.path.getsize(file_path)
            })
    
    return zip_files

def extract_all_zip_files(config, conn, target_states=None, logger=None):
    """Extract all downloaded ZIP files."""
    logger = logger or logging.getLogger(__name__)
    
    # Get list of ZIP files to process
    zip_files = get_downloaded_zip_files(config, conn, target_states)
    total_files = len(zip_files)
    
    if total_files == 0:
        logger.warning("No ZIP files found to extract")
        return {'extracted': 0, 'failed': 0, 'total': 0}
    
    logger.info(f"Found {total_files} ZIP files to extract")
    
    # Check for already extracted files
    cursor = conn.cursor()
    cursor.execute('SELECT zip_file_path FROM extraction_log WHERE extraction_success = 1')
    already_extracted = set(row[0] for row in cursor.fetchall())
    
    # Filter out already extracted files
    zip_files_to_process = [zf for zf in zip_files if zf['zip_path'] not in already_extracted]
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
            zip_file['zip_path'],
            extraction_base,
            zip_file['state_code'],
            zip_file['county_code'],
            zip_file['product_name'],
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

def generate_extraction_report(extraction_results, logger=None):
    """Generate extraction summary report."""
    logger = logger or logging.getLogger(__name__)
    
    logger.info("\n" + "=" * 80)
    logger.info("ZIP EXTRACTION REPORT")
    logger.info("=" * 80)
    
    # Extraction summary
    logger.info(f"\n📦 EXTRACTION SUMMARY:")
    logger.info(f"  Total ZIP files found: {extraction_results.get('total', 0)}")
    logger.info(f"  Successfully extracted: {extraction_results.get('extracted', 0)}")
    logger.info(f"  Failed extractions: {extraction_results.get('failed', 0)}")
    logger.info(f"  Already extracted (skipped): {extraction_results.get('skipped', 0)}")
    
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
        state_placeholders = ','.join(['?' for _ in target_states])
        
        # Clear extraction logs for target states
        cursor.execute(f'''
            DELETE FROM extraction_log
            WHERE state_code IN ({state_placeholders})
        ''', target_states)
        
        logger.info(f"Cleared extraction logs for states: {target_states}")
    else:
        # Clear all extraction logs
        cursor.execute('DELETE FROM extraction_log')
        
        logger.info("Cleared all extraction logs")
    
    conn.commit()
    
    # Get counts after clearing
    cursor.execute('SELECT COUNT(*) FROM extraction_log')
    extraction_count = cursor.fetchone()[0]
    
    logger.info(f"Remaining extraction logs: {extraction_count}")

def cleanup_temporary_files(config, logger=None):
    """Clean up temporary extraction files if needed.
    
    IMPORTANT: This function ONLY removes temporary processing files.
    It NEVER touches the original ZIP files in the download directory.
    """
    logger = logger or logging.getLogger(__name__)
    
    temp_dir = config['processing']['temp_directory']
    
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
        logger.info("=== FEMA ZIP File Extraction ===")
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
                for state_code in target_states:
                    state_extraction_dir = os.path.join(extraction_base, state_code)
                    if os.path.exists(state_extraction_dir):
                        try:
                            shutil.rmtree(state_extraction_dir)
                            logger.info(f"Removed existing extraction directory: {state_extraction_dir}")
                        except Exception as e:
                            logger.warning(f"Failed to remove {state_extraction_dir}: {e}")
            else:
                if os.path.exists(extraction_base):
                    try:
                        shutil.rmtree(extraction_base)
                        logger.info(f"Removed existing extraction directory: {extraction_base}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {extraction_base}: {e}")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No actual processing will be performed")
            zip_files = get_downloaded_zip_files(config, conn, target_states)
            logger.info(f"Would process {len(zip_files)} downloaded ZIP files")
            return
        
        # Phase 2: ZIP File Extraction
        logger.info("\n=== ZIP File Extraction ===")
        extraction_results = extract_all_zip_files(config, conn, target_states, logger)
        
        # Final Summary
        generate_extraction_report(extraction_results, logger)
        
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