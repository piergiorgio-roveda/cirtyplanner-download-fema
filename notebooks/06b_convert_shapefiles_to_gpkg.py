#!/usr/bin/env python3
"""
Script 06b: Convert Extracted Shapefiles to GeoPackage Format

This script processes shapefiles extracted by script 06a:
1. Reads extraction_06a_log to find all extracted shapefiles
2. Converts each shapefile to GeoPackage (.gpkg) format
3. Organizes output in a clean directory structure by product name

Prerequisites:
    - Script 06a: Extracted shapefiles with extraction_06a_log entries
    - config.json file must exist
    - OSGeo4W console or environment with ogr2ogr available in PATH
      (This script must be run from an OSGeo4W console to have access to ogr2ogr)

Dependencies:
    No special Python packages required (uses ogr2ogr command-line tool from OSGeo4W)

Usage:
    python notebooks/06b_convert_shapefiles_to_gpkg.py
    
    # With custom configuration:
    python notebooks/06b_convert_shapefiles_to_gpkg.py --config custom_config.json
    
    # Process specific products only:
    python notebooks/06b_convert_shapefiles_to_gpkg.py --products product1,product2
    
    # Resume interrupted processing:
    python notebooks/06b_convert_shapefiles_to_gpkg.py --resume
    
    # Force rebuild from scratch:
    python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild
"""

import sqlite3
import os
import json
import shutil
import argparse
import logging
import subprocess
import uuid
from pathlib import Path
from datetime import datetime
import psutil
import gc
import warnings
from concurrent.futures import ThreadPoolExecutor
warnings.filterwarnings('ignore', category=UserWarning)

class ProcessingError(Exception):
    """Base class for processing errors."""
    pass

class ConversionError(ProcessingError):
    """Shapefile to GPKG conversion failed."""
    pass

class StrictModeError(ProcessingError):
    """Error raised when strict mode detects warnings or errors."""
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
    # Create .log directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.log')
    os.makedirs(log_dir, exist_ok=True)
    
    # Set up log file path in the .log directory
    log_file = os.path.join(log_dir, 'conversion_06b.log')
    
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
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
        
        # Add shapefile_to_gpkg_path if not present
        if 'processing' in config and 'shapefile_to_gpkg_path' not in config['processing']:
            config['processing']['shapefile_to_gpkg_path'] = os.path.join(
                os.path.dirname(config['processing']['extraction_base_path']),
                'FEMA_SHAPEFILE_TO_GPKG'
            )
            
        # Add temp_conversion_path if not present
        if 'processing' in config and 'temp_conversion_path' not in config['processing']:
            config['processing']['temp_conversion_path'] = os.path.join(
                'D:\\git\\cityplanner-desktop\\download-fema\\.TMP'
            )
            
        # Add strict_mode if not present
        if 'processing' in config and 'strict_mode' not in config['processing']:
            config['processing']['strict_mode'] = False
            
        return config
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
    except Exception as e:
        raise ValueError(f"Error reading config file {config_path}: {e}")

def get_db_connection(config):
    """Create a new database connection for thread safety."""
    db_path = config['database']['path']
    conn = sqlite3.connect(db_path)
    return conn

def setup_database(config):
    """Setup database connection and create processing tables."""
    conn = get_db_connection(config)
    create_processing_tables(conn)
    return conn

def create_processing_tables(conn):
    """Create tables for tracking conversion."""
    cursor = conn.cursor()
    
    # Track shapefile to GPKG conversion status
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS conversion_06b_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            shapefile_path TEXT NOT NULL,
            gpkg_path TEXT NOT NULL,
            conversion_success BOOLEAN NOT NULL,
            conversion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversion_06b_product ON conversion_06b_log (product_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversion_06b_success ON conversion_06b_log (conversion_success)')
    
    conn.commit()

def get_shapefiles_to_convert(config, conn, target_products=None):
    """Get list of shapefiles to convert from extraction_06a_log."""
    extraction_base = config['processing']['extraction_base_path']
    gpkg_base = config['processing']['shapefile_to_gpkg_path']
    
    cursor = conn.cursor()
    
    # Get already converted files
    cursor.execute('SELECT shapefile_path FROM conversion_06b_log WHERE conversion_success = 1')
    already_converted = set(row[0] for row in cursor.fetchall())
    
    # Build query to get shapefiles to convert
    query = '''
        SELECT product_name, extracted_path, shapefile_name
        FROM extraction_06a_log
        WHERE extraction_success = 1
        AND shapefile_name IS NOT NULL
    '''
    
    params = []
    if target_products:
        placeholders = ','.join(['?' for _ in target_products])
        query += f' AND product_name IN ({placeholders})'
        params.extend(target_products)
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    shapefiles_to_convert = []
    for product_name, extracted_path, shapefile_name in results:
        # Build full source path
        source_path = os.path.join(extraction_base, product_name, extracted_path)
        
        # Build destination path
        gpkg_name = os.path.splitext(shapefile_name)[0] + '.gpkg'
        dest_path = os.path.join(gpkg_base, product_name, gpkg_name)
        
        # Skip if already converted
        if source_path in already_converted:
            continue
            
        shapefiles_to_convert.append({
            'product_name': product_name,
            'source_path': source_path,
            'dest_path': dest_path,
            'shapefile_name': shapefile_name
        })
    
    return shapefiles_to_convert

def convert_with_ogr2ogr(source_path, dest_path, temp_dir, product_name, strict_mode=False, encoding='UTF-8'):
    """Convert shapefile to GPKG using ogr2ogr (fixes polygon winding order issues)."""
    try:
        # Create temporary directory if it doesn't exist
        os.makedirs(temp_dir, exist_ok=True)
        
        # Create a unique temporary destination path using product name and UUID
        unique_id = str(uuid.uuid4())[:8]  # Use first 8 characters of UUID
        temp_filename = f"{product_name}_{unique_id}_{os.path.basename(dest_path)}"
        temp_dest_path = os.path.join(temp_dir, temp_filename)
        
        # Build ogr2ogr command
        cmd = [
            'ogr2ogr',
            '-f', 'GPKG',
            '-nlt', 'PROMOTE_TO_MULTI',  # Convert to multi-geometries
            '-nln', os.path.splitext(os.path.basename(dest_path))[0],  # Layer name
            '-a_srs', 'EPSG:4326',  # Assign output coordinate system
            '--config', 'OGR_ENABLE_PARTIAL_REPROJECTION', 'TRUE',  # Enable partial reprojection
            '--config', 'CPL_TMPDIR', temp_dir,  # Set temp directory
            '--config', 'SHAPE_ENCODING', encoding,  # Set encoding
            '-lco', 'ENCODING=UTF-8',  # Force UTF-8 output encoding
        ]
        
        # Only skip failures in non-strict mode
        if not strict_mode:
            cmd.append('-skipfailures')
            
        # Add destination and source
        cmd.extend([temp_dest_path, source_path])
        
        # Execute command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        # In strict mode, check for warnings in stderr
        if strict_mode and result.stderr:
            raise StrictModeError(f"Warnings detected in strict mode: {result.stderr}")
        
        # Create destination directory
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        # Move the file from temporary location to final destination
        shutil.move(temp_dest_path, dest_path)
        
        return True
    except subprocess.CalledProcessError as e:
        raise ConversionError(f"ogr2ogr failed: {e.stderr}")
    except Exception as e:
        raise ConversionError(f"Error during conversion or file move: {str(e)}")

def convert_shapefile_to_gpkg(shapefile_info, config, logger):
    """Convert a single shapefile to GPKG format using ogr2ogr."""
    # Create a new database connection for this thread
    thread_conn = None
    
    try:
        product_name = shapefile_info['product_name']
        source_path = shapefile_info['source_path']
        dest_path = shapefile_info['dest_path']
        shapefile_name = shapefile_info['shapefile_name']
        encoding = config.get('processing', {}).get('shapefile_encoding', 'UTF-8')
        temp_dir = config.get('processing', {}).get('temp_conversion_path', 'D:\\git\\cityplanner-desktop\\download-fema\\.TMP')
        strict_mode = config.get('processing', {}).get('strict_mode', False)
        
        logger.info(f"Converting: {shapefile_name} from {product_name}")
        
        # Check if source file exists
        if not os.path.exists(source_path):
            raise ConversionError(f"Source shapefile not found: {source_path}")
        
        # Convert using ogr2ogr with temporary directory
        logger.debug(f"  Using ogr2ogr for conversion with encoding {encoding}")
        logger.debug(f"  Using temporary directory: {temp_dir}")
        logger.debug(f"  Strict mode: {'enabled' if strict_mode else 'disabled'}")
        convert_with_ogr2ogr(source_path, dest_path, temp_dir, product_name, strict_mode, encoding)
        
        # Create thread-local database connection
        thread_conn = get_db_connection(config)
        
        # Log successful conversion
        cursor = thread_conn.cursor()
        cursor.execute('''
            INSERT INTO conversion_06b_log 
            (product_name, shapefile_path, gpkg_path, conversion_success)
            VALUES (?, ?, ?, ?)
        ''', (product_name, source_path, dest_path, True))
        thread_conn.commit()
        
        logger.info(f"  [SUCCESS] Converted {shapefile_name} to {os.path.basename(dest_path)}")
        
        return {
            'success': True,
            'source_path': source_path,
            'dest_path': dest_path
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"  [ERROR] Conversion failed: {error_msg}")
        
        try:
            # Create thread-local database connection if not already created
            if thread_conn is None:
                thread_conn = get_db_connection(config)
                
            # Log failed conversion
            cursor = thread_conn.cursor()
            cursor.execute('''
                INSERT INTO conversion_06b_log 
                (product_name, shapefile_path, gpkg_path, conversion_success, error_message)
                VALUES (?, ?, ?, ?, ?)
            ''', (product_name, source_path, dest_path, False, error_msg))
            thread_conn.commit()
        except Exception as db_error:
            logger.error(f"  Database error: {db_error}")
        
        return {
            'success': False,
            'error': error_msg,
            'source_path': source_path
        }
    finally:
        # Close the thread-local connection
        if thread_conn is not None:
            thread_conn.close()

def convert_all_shapefiles(config, conn, target_products=None, max_workers=4, logger=None):
    """Convert all shapefiles to GPKG format."""
    logger = logger or logging.getLogger(__name__)
    
    # Get list of shapefiles to convert
    shapefiles = get_shapefiles_to_convert(config, conn, target_products)
    total_files = len(shapefiles)
    
    if total_files == 0:
        logger.warning("No shapefiles found to convert")
        return {'converted': 0, 'failed': 0, 'total': 0}
    
    logger.info(f"Found {total_files} shapefiles to convert")
    
    # Setup conversion directory
    gpkg_base = config['processing']['shapefile_to_gpkg_path']
    os.makedirs(gpkg_base, exist_ok=True)
    
    # Process shapefiles
    converted_count = 0
    failed_count = 0
    memory_monitor = MemoryMonitor(config['processing']['memory_limit_mb'])
    strict_mode = config.get('processing', {}).get('strict_mode', False)
    
    # In strict mode, always use sequential processing
    if strict_mode:
        logger.info("Strict mode enabled - using sequential processing")
        max_workers = 1
        config['processing']['parallel_processing'] = False
    
    # Use parallel processing if configured and not in strict mode
    if config['processing'].get('parallel_processing', False) and max_workers > 1:
        logger.info(f"Using parallel processing with {max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_shapefile = {
                executor.submit(
                    convert_shapefile_to_gpkg, shapefile, config, logger
                ): shapefile for shapefile in shapefiles
            }
            
            for i, future in enumerate(future_to_shapefile, 1):
                try:
                    result = future.result()
                    if result['success']:
                        converted_count += 1
                    else:
                        failed_count += 1
                        # In strict mode, stop on first failure
                        if strict_mode:
                            logger.error("Strict mode enabled - stopping on first failure")
                            raise StrictModeError("Conversion failed in strict mode")
                except StrictModeError as e:
                    logger.error(f"Strict mode error: {e}")
                    raise  # Re-raise to stop processing
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    failed_count += 1
                    # In strict mode, stop on first error
                    if strict_mode:
                        logger.error("Strict mode enabled - stopping on first error")
                        raise StrictModeError(f"Unexpected error in strict mode: {e}")
                
                # Memory management
                if i % 10 == 0:
                    memory_monitor.check_memory_usage()
                    logger.info(f"Progress: {i}/{total_files} - Converted: {converted_count}, Failed: {failed_count}")
    else:
        # Sequential processing
        for i, shapefile in enumerate(shapefiles, 1):
            try:
                result = convert_shapefile_to_gpkg(shapefile, config, logger)
                
                if result['success']:
                    converted_count += 1
                else:
                    failed_count += 1
                    # In strict mode, stop on first failure
                    if strict_mode:
                        logger.error("Strict mode enabled - stopping on first failure")
                        raise StrictModeError("Conversion failed in strict mode")
            except StrictModeError as e:
                logger.error(f"Strict mode error: {e}")
                raise  # Re-raise to stop processing
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                failed_count += 1
                # In strict mode, stop on first error
                if strict_mode:
                    logger.error("Strict mode enabled - stopping on first error")
                    raise StrictModeError(f"Unexpected error in strict mode: {e}")
            
            # Memory management
            if i % 10 == 0:
                memory_monitor.check_memory_usage()
                logger.info(f"Progress: {i}/{total_files} - Converted: {converted_count}, Failed: {failed_count}")
    
    logger.info(f"Conversion complete: {converted_count} successful, {failed_count} failed")
    
    return {
        'converted': converted_count,
        'failed': failed_count,
        'total': total_files
    }

def generate_conversion_report(conversion_results, conn, logger=None):
    """Generate conversion summary report with database statistics."""
    logger = logger or logging.getLogger(__name__)
    
    logger.info("\n" + "=" * 80)
    logger.info("SHAPEFILE TO GPKG CONVERSION REPORT")
    logger.info("=" * 80)
    
    # Conversion summary
    logger.info(f"\n[CONVERSION SUMMARY]:")
    logger.info(f"  Total shapefiles found: {conversion_results.get('total', 0)}")
    logger.info(f"  Successfully converted: {conversion_results.get('converted', 0)}")
    logger.info(f"  Failed conversions: {conversion_results.get('failed', 0)}")
    
    # Database statistics
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM conversion_06b_log WHERE conversion_success = 1')
    total_successful_conversions = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT product_name) FROM conversion_06b_log WHERE conversion_success = 1')
    total_products_converted = cursor.fetchone()[0]
    
    logger.info(f"\n[DATABASE STATISTICS]:")
    logger.info(f"  Total products with conversions: {total_products_converted}")
    logger.info(f"  Total successful conversions: {total_successful_conversions}")
    
    # Success rate
    total_processed = conversion_results.get('total', 0)
    if total_processed > 0:
        conversion_success_rate = (conversion_results.get('converted', 0) / total_processed) * 100
        logger.info(f"\n[SUCCESS RATE]:")
        logger.info(f"  Conversion success: {conversion_success_rate:.1f}%")
    
    logger.info("\n" + "=" * 80)
    logger.info("CONVERSION COMPLETED - GeoPackage files are ready for use")
    logger.info("=" * 80)

def clear_conversion_logs(conn, target_products=None, logger=None):
    """Clear conversion logs to force rebuild from scratch."""
    logger = logger or logging.getLogger(__name__)
    
    cursor = conn.cursor()
    
    if target_products:
        # For product-specific clearing
        placeholders = ','.join(['?' for _ in target_products])
        cursor.execute(f'''
            DELETE FROM conversion_06b_log
            WHERE product_name IN ({placeholders})
        ''', target_products)
        
        logger.info(f"Cleared conversion logs for products: {target_products}")
    else:
        # Clear all conversion logs
        cursor.execute('DELETE FROM conversion_06b_log')
        
        logger.info("Cleared all conversion logs")
    
    conn.commit()
    
    # Get counts after clearing
    cursor.execute('SELECT COUNT(*) FROM conversion_06b_log')
    conversion_count = cursor.fetchone()[0]
    
    logger.info(f"Remaining conversion logs: {conversion_count}")

def main():
    """Main conversion function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Convert FEMA shapefiles to GeoPackage format')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    parser.add_argument('--products', help='Comma-separated list of product names to process')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted processing')
    parser.add_argument('--force-rebuild', action='store_true', help='Force rebuild - clear conversion logs and start from scratch')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without doing it')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of worker threads for parallel processing')
    parser.add_argument('--encoding', default='UTF-8', help='Encoding to use for shapefile attribute data (default: UTF-8, try latin-1 for encoding issues)')
    parser.add_argument('--temp-dir', help='Temporary directory for faster conversion (default: D:\\git\\cityplanner-desktop\\download-fema\\.TMP)')
    parser.add_argument('--strict', action='store_true', help='Enable strict mode - stop on first error or warning')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.verbose)
    
    # Parse target products
    target_products = None
    if args.products:
        target_products = [p.strip() for p in args.products.split(',')]
        logger.info(f"Processing specific products: {target_products}")
    
    try:
        # Phase 1: Setup and Initialization
        logger.info("=== FEMA Shapefile to GeoPackage Conversion (Script 06b) ===")
        logger.info(f"Start time: {datetime.now()}")
        
        config = load_config(args.config)
        
        # Log conversion method
        logger.info("Using ogr2ogr for shapefile conversion (fixes polygon winding order issues)")
            
        # Add encoding to config if specified
        if args.encoding:
            if 'processing' not in config:
                config['processing'] = {}
            config['processing']['shapefile_encoding'] = args.encoding
            logger.info(f"Using encoding: {args.encoding} for shapefile conversion")
            
        # Add temp directory to config if specified
        if args.temp_dir:
            if 'processing' not in config:
                config['processing'] = {}
            config['processing']['temp_conversion_path'] = args.temp_dir
            logger.info(f"Using temporary directory: {args.temp_dir} for faster conversion")
        else:
            temp_dir = config.get('processing', {}).get('temp_conversion_path')
            logger.info(f"Using temporary directory: {temp_dir} for faster conversion")
            
        # Add strict mode to config if specified
        if args.strict:
            if 'processing' not in config:
                config['processing'] = {}
            config['processing']['strict_mode'] = True
            logger.info("STRICT MODE ENABLED - Will stop on first error or warning")
        conn = setup_database(config)
        
        # Handle force rebuild option
        if args.force_rebuild:
            logger.info("FORCE REBUILD MODE - Clearing conversion logs")
            clear_conversion_logs(conn, target_products, logger)
            
            # Also remove existing GPKG files for target products
            gpkg_base = config['processing']['shapefile_to_gpkg_path']
            
            if target_products:
                for product_name in target_products:
                    product_dir = os.path.join(gpkg_base, product_name)
                    if os.path.exists(product_dir):
                        try:
                            shutil.rmtree(product_dir)
                            logger.info(f"Removed existing GPKG directory: {product_dir}")
                        except Exception as e:
                            logger.warning(f"Failed to remove {product_dir}: {e}")
            else:
                if os.path.exists(gpkg_base):
                    try:
                        shutil.rmtree(gpkg_base)
                        logger.info(f"Removed existing GPKG directory: {gpkg_base}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {gpkg_base}: {e}")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No actual processing will be performed")
            shapefiles = get_shapefiles_to_convert(config, conn, target_products)
            logger.info(f"Would convert {len(shapefiles)} shapefiles")
            return
        
        # Phase 2: Shapefile to GPKG Conversion
        logger.info("\n=== Shapefile to GeoPackage Conversion ===")
        conversion_results = convert_all_shapefiles(
            config, conn, target_products, args.max_workers, logger
        )
        
        # Final Summary
        generate_conversion_report(conversion_results, conn, logger)
        
        logger.info(f"\nConversion completed at: {datetime.now()}")
        
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()