#!/usr/bin/env python3
"""
Script 06: Extract and Merge FEMA Flood Risk Shapefiles

This script processes ZIP files downloaded by script 05:
1. Extracts ZIP files containing FEMA shapefiles
2. Categorizes shapefiles by type and geometry
3. Merges shapefiles by state and type into GPKG format
4. Adds metadata columns for geographic traceability
5. Validates output integrity and completeness

Prerequisites:
    - Script 04: Database with shapefile metadata
    - Script 05: Downloaded ZIP files

Dependencies:
    pip install geopandas fiona shapely pyproj psutil

Usage:
    python notebooks/06_extract_and_merge_shapefiles.py
    
    # With custom configuration:
    python notebooks/06_extract_and_merge_shapefiles.py --config custom_config.json
    
    # Process specific states only:
    python notebooks/06_extract_and_merge_shapefiles.py --states 01,02,04
    
    # Resume interrupted processing:
    python notebooks/06_extract_and_merge_shapefiles.py --resume
    
    # Force rebuild from scratch (if script 04/05 found new data):
    python notebooks/06_extract_and_merge_shapefiles.py --force-rebuild
    
    # Force rebuild specific states only:
    python notebooks/06_extract_and_merge_shapefiles.py --force-rebuild --states 01,02
"""

import geopandas as gpd
import pandas as pd
import fiona
from shapely.geometry import Point, LineString, Polygon
from shapely.validation import make_valid
from pyproj import CRS, Transformer
import sqlite3
import zipfile
import os
import json
import shutil
import argparse
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
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

# Enhanced metadata columns to add to merged shapefiles
ENHANCED_METADATA_COLUMNS = {
    'fema_state_code': 'str:2',
    'fema_state_name': 'str:50',
    'fema_county_code': 'str:5',
    'fema_county_name': 'str:100',
    'fema_community_code': 'str:10',
    'fema_community_name': 'str:100',
    'fema_product_name': 'str:100',
    'fema_source_file': 'str:200',
    'fema_processing_date': 'datetime',
    'fema_original_crs': 'str:50'
}

class ProcessingError(Exception):
    """Base class for processing errors."""
    pass

class ZipExtractionError(ProcessingError):
    """ZIP file extraction failed."""
    pass

class ShapefileValidationError(ProcessingError):
    """Shapefile validation failed."""
    pass

class GPKGMergingError(ProcessingError):
    """GPKG merging operation failed."""
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
            logging.FileHandler('processing.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config(config_path='config.json'):
    """Load configuration from JSON file with processing extensions."""
    if not os.path.exists(config_path):
        # Create default config with processing extensions
        default_config = {
            "download": {
                "base_path": "E:\\FEMA_DOWNLOAD"
            },
            "processing": {
                "extraction_base_path": "E:\\FEMA_EXTRACTED",
                "merged_output_path": "merged",
                "temp_directory": "temp_processing",
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
        
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        print(f"Created default configuration file: {config_path}")
        return default_config
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
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
    
    # Track shapefile processing and merging
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS shapefile_processing_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            shapefile_type TEXT NOT NULL,
            geometry_type TEXT,
            source_files_count INTEGER DEFAULT 0,
            total_features_merged INTEGER DEFAULT 0,
            output_gpkg_path TEXT,
            processing_success BOOLEAN NOT NULL,
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_size_bytes INTEGER,
            coordinate_system TEXT,
            error_message TEXT,
            FOREIGN KEY (state_code) REFERENCES states (state_code)
        )
    ''')
    
    # Track individual shapefile contributions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS shapefile_contributions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            county_code TEXT NOT NULL,
            community_code TEXT NOT NULL,
            product_name TEXT NOT NULL,
            shapefile_type TEXT NOT NULL,
            source_shapefile_path TEXT NOT NULL,
            features_count INTEGER DEFAULT 0,
            merged_into_gpkg TEXT,
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (state_code) REFERENCES states (state_code),
            FOREIGN KEY (county_code) REFERENCES counties (county_code),
            FOREIGN KEY (community_code) REFERENCES communities (community_code)
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_extraction_log_state ON extraction_log (state_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_processing_log_state ON shapefile_processing_log (state_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_contributions_state ON shapefile_contributions (state_code)')
    
    conn.commit()

def categorize_shapefile(filename):
    """Determine shapefile type and expected geometry."""
    base_name = os.path.splitext(filename)[0]
    for category, types in SHAPEFILE_CATEGORIES.items():
        if base_name in types:
            return base_name, types[base_name], category
    return base_name, 'UNKNOWN', 'OTHER'

def validate_shapefile_integrity(shapefile_path):
    """Validate shapefile completeness (.shp, .shx, .dbf, .prj)."""
    base_path = os.path.splitext(shapefile_path)[0]
    required_extensions = ['.shp', '.shx', '.dbf']
    optional_extensions = ['.prj', '.cpg']
    
    missing_required = []
    for ext in required_extensions:
        if not os.path.exists(base_path + ext):
            missing_required.append(ext)
    
    if missing_required:
        raise ShapefileValidationError(f"Missing required files: {missing_required}")
    
    # Check if .prj file exists (coordinate system info)
    has_projection = os.path.exists(base_path + '.prj')
    
    return {
        'valid': True,
        'has_projection': has_projection,
        'base_path': base_path
    }

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

def discover_and_categorize_shapefiles(config, conn, target_states=None, logger=None):
    """Discover and categorize all extracted shapefiles."""
    logger = logger or logging.getLogger(__name__)
    
    extraction_base = config['processing']['extraction_base_path']
    shapefile_inventory = {}
    
    # Get successful extractions from database
    cursor = conn.cursor()
    if target_states:
        state_placeholders = ','.join(['?' for _ in target_states])
        cursor.execute(f'''
            SELECT state_code, county_code, product_name, shapefiles_found
            FROM extraction_log 
            WHERE extraction_success = 1 AND state_code IN ({state_placeholders})
        ''', target_states)
    else:
        cursor.execute('''
            SELECT state_code, county_code, product_name, shapefiles_found
            FROM extraction_log 
            WHERE extraction_success = 1
        ''')
    
    extractions = cursor.fetchall()
    logger.info(f"Discovering shapefiles from {len(extractions)} successful extractions")
    
    for state_code, county_code, product_name, shapefiles_json in extractions:
        if shapefiles_json:
            shapefiles = json.loads(shapefiles_json)
            
            for shapefile_info in shapefiles:
                shapefile_type = shapefile_info['type']
                
                # Initialize state in inventory
                if state_code not in shapefile_inventory:
                    shapefile_inventory[state_code] = {}
                
                # Initialize shapefile type in state
                if shapefile_type not in shapefile_inventory[state_code]:
                    shapefile_inventory[state_code][shapefile_type] = {
                        'geometry_type': shapefile_info['geometry'],
                        'category': shapefile_info['category'],
                        'source_files': []
                    }
                
                # Add source file info
                extract_dir = os.path.join(extraction_base, state_code, county_code, product_name)
                shapefile_path = os.path.join(extract_dir, shapefile_info['filename'])
                
                if os.path.exists(shapefile_path):
                    try:
                        # Validate shapefile integrity
                        validation_result = validate_shapefile_integrity(shapefile_path)
                        
                        shapefile_inventory[state_code][shapefile_type]['source_files'].append({
                            'state_code': state_code,
                            'county_code': county_code,
                            'product_name': product_name,
                            'shapefile_path': shapefile_path,
                            'has_projection': validation_result['has_projection']
                        })
                        
                    except ShapefileValidationError as e:
                        logger.warning(f"Invalid shapefile {shapefile_path}: {e}")
    
    # Log discovery results
    total_types = 0
    total_files = 0
    for state_code, types in shapefile_inventory.items():
        state_types = len(types)
        state_files = sum(len(info['source_files']) for info in types.values())
        total_types += state_types
        total_files += state_files
        logger.info(f"State {state_code}: {state_types} shapefile types, {state_files} source files")
    
    logger.info(f"Discovery complete: {total_types} shapefile types, {total_files} source files")
    
    return shapefile_inventory

def create_enhanced_schema(original_schema):
    """Add metadata columns to original shapefile schema."""
    enhanced_schema = original_schema.copy()
    
    # Add FEMA metadata columns
    enhanced_schema['properties'].update(ENHANCED_METADATA_COLUMNS)
    
    return enhanced_schema

def merge_shapefiles_to_gpkg(state_code, shapefile_type, source_files, output_path, config, conn, logger):
    """Merge multiple shapefiles of the same type into a single GPKG."""
    try:
        logger.info(f"  Merging {len(source_files)} files for {shapefile_type}")
        
        # Get state and geographic info from database
        cursor = conn.cursor()
        cursor.execute('SELECT state_name FROM states WHERE state_code = ?', (state_code,))
        state_name = cursor.fetchone()[0]
        
        # Initialize variables for merging
        merged_gdf = None
        total_features = 0
        target_crs = config['processing']['target_crs']
        chunk_size = config['processing']['chunk_size_features']
        
        # Process each source file
        for i, source_file in enumerate(source_files):
            try:
                # Get geographic metadata
                cursor.execute('''
                    SELECT county_name FROM counties 
                    WHERE county_code = ? AND state_code = ?
                ''', (source_file['county_code'], state_code))
                county_result = cursor.fetchone()
                county_name = county_result[0] if county_result else 'Unknown'
                
                # Try to get community info (may not exist for all files)
                cursor.execute('''
                    SELECT community_name FROM communities 
                    WHERE community_code LIKE ? AND county_code = ? AND state_code = ?
                ''', (f"{source_file['county_code']}%", source_file['county_code'], state_code))
                community_result = cursor.fetchone()
                community_name = community_result[0] if community_result else 'Unknown'
                community_code = f"{source_file['county_code']}C" if community_result else 'Unknown'
                
                # Read shapefile
                gdf = gpd.read_file(source_file['shapefile_path'])
                
                if len(gdf) == 0:
                    logger.warning(f"    Empty shapefile: {source_file['shapefile_path']}")
                    continue
                
                # Get original CRS
                original_crs = str(gdf.crs) if gdf.crs else 'Unknown'
                
                # Transform to target CRS if needed
                if gdf.crs and str(gdf.crs) != target_crs:
                    gdf = gdf.to_crs(target_crs)
                
                # Validate and fix geometries if configured
                if config['validation']['geometry_validation']:
                    if config['validation']['fix_invalid_geometries']:
                        gdf['geometry'] = gdf['geometry'].apply(lambda geom: make_valid(geom) if geom and not geom.is_valid else geom)
                    
                    if config['validation']['skip_empty_geometries']:
                        gdf = gdf[~gdf['geometry'].is_empty]
                
                # Add FEMA metadata columns
                gdf['fema_state_code'] = state_code
                gdf['fema_state_name'] = state_name
                gdf['fema_county_code'] = source_file['county_code']
                gdf['fema_county_name'] = county_name
                gdf['fema_community_code'] = community_code
                gdf['fema_community_name'] = community_name
                gdf['fema_product_name'] = source_file['product_name']
                gdf['fema_source_file'] = os.path.basename(source_file['shapefile_path'])
                gdf['fema_processing_date'] = datetime.now()
                gdf['fema_original_crs'] = original_crs
                
                # Merge with existing data
                if merged_gdf is None:
                    merged_gdf = gdf
                else:
                    merged_gdf = gpd.GeoDataFrame(pd.concat([merged_gdf, gdf], ignore_index=True))
                
                total_features += len(gdf)
                
                # Log contribution to database
                cursor.execute('''
                    INSERT INTO shapefile_contributions
                    (state_code, county_code, community_code, product_name, shapefile_type,
                     source_shapefile_path, features_count, merged_into_gpkg)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (state_code, source_file['county_code'], community_code, 
                      source_file['product_name'], shapefile_type,
                      source_file['shapefile_path'], len(gdf), output_path))
                
                logger.info(f"    [{i+1}/{len(source_files)}] Added {len(gdf)} features from {os.path.basename(source_file['shapefile_path'])}")
                
            except Exception as e:
                logger.error(f"    Error processing {source_file['shapefile_path']}: {e}")
                continue
        
        if merged_gdf is None or len(merged_gdf) == 0:
            raise GPKGMergingError("No valid features to merge")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write to GPKG
        merged_gdf.to_file(output_path, driver='GPKG')
        
        # Get file size
        file_size = os.path.getsize(output_path)
        
        # Log successful processing
        cursor.execute('''
            INSERT INTO shapefile_processing_log
            (state_code, shapefile_type, geometry_type, source_files_count,
             total_features_merged, output_gpkg_path, processing_success,
             file_size_bytes, coordinate_system)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (state_code, shapefile_type, merged_gdf.geometry.geom_type.iloc[0],
              len(source_files), total_features, output_path, True,
              file_size, target_crs))
        
        conn.commit()
        
        logger.info(f"  ✓ Created {shapefile_type}.gpkg: {total_features} features, {file_size // (1024*1024)}MB")
        
        return {
            'success': True,
            'output_path': output_path,
            'total_features': total_features,
            'file_size': file_size
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"  ✗ Failed to merge {shapefile_type}: {error_msg}")
        
        # Log failed processing
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO shapefile_processing_log
            (state_code, shapefile_type, source_files_count, processing_success, error_message)
            VALUES (?, ?, ?, ?, ?)
        ''', (state_code, shapefile_type, len(source_files), False, error_msg))
        conn.commit()
        
        return {
            'success': False,
            'error': error_msg
        }

def merge_shapefiles_by_state(config, conn, shapefile_inventory, logger=None):
    """Merge shapefiles by state and type."""
    logger = logger or logging.getLogger(__name__)
    
    output_base = config['processing']['merged_output_path']
    os.makedirs(output_base, exist_ok=True)
    
    results = {
        'states_processed': 0,
        'gpkg_files_created': 0,
        'total_features_merged': 0,
        'failed_merges': 0
    }
    
    memory_monitor = MemoryMonitor(config['processing']['memory_limit_mb'])
    
    # Process each state
    for state_code, shapefile_types in shapefile_inventory.items():
        logger.info(f"Processing state: {state_code}")
        
        # Create state output directory
        state_output_dir = os.path.join(output_base, state_code)
        os.makedirs(state_output_dir, exist_ok=True)
        
        state_results = {
            'gpkg_files': 0,
            'total_features': 0,
            'failed_types': []
        }
        
        # Process each shapefile type in the state
        for shapefile_type, type_info in shapefile_types.items():
            if len(type_info['source_files']) == 0:
                continue
                
            output_path = os.path.join(state_output_dir, f"{shapefile_type}.gpkg")
            
            # Check if already processed
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id FROM shapefile_processing_log 
                WHERE state_code = ? AND shapefile_type = ? AND processing_success = 1
            ''', (state_code, shapefile_type))
            
            if cursor.fetchone():
                logger.info(f"  Skipping {shapefile_type} (already processed)")
                continue
            
            # Merge shapefiles
            merge_result = merge_shapefiles_to_gpkg(
                state_code, shapefile_type, type_info['source_files'],
                output_path, config, conn, logger
            )
            
            if merge_result['success']:
                state_results['gpkg_files'] += 1
                state_results['total_features'] += merge_result['total_features']
                results['gpkg_files_created'] += 1
                results['total_features_merged'] += merge_result['total_features']
            else:
                state_results['failed_types'].append(shapefile_type)
                results['failed_merges'] += 1
            
            # Memory management
            memory_monitor.check_memory_usage()
        
        # Create state processing summary
        summary_path = os.path.join(state_output_dir, 'processing_summary.json')
        summary = {
            'state_code': state_code,
            'processing_date': datetime.now().isoformat(),
            'gpkg_files_created': state_results['gpkg_files'],
            'total_features_merged': state_results['total_features'],
            'failed_types': state_results['failed_types'],
            'shapefile_types_processed': list(shapefile_types.keys())
        }
        
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        results['states_processed'] += 1
        logger.info(f"✓ Completed state {state_code}: {state_results['gpkg_files']} GPKG files, {state_results['total_features']} features")
    
    return results

def validate_gpkg_output(gpkg_path, expected_features_count=None, logger=None):
    """Validate GPKG file integrity and completeness."""
    logger = logger or logging.getLogger(__name__)
    
    try:
        # Check file exists and is readable
        if not os.path.exists(gpkg_path):
            return {'valid': False, 'error': 'File does not exist'}
        
        # Try to read with geopandas
        gdf = gpd.read_file(gpkg_path)
        
        # Basic validation checks
        validation_results = {
            'valid': True,
            'file_size_mb': os.path.getsize(gpkg_path) / (1024 * 1024),
            'feature_count': len(gdf),
            'geometry_types': list(gdf.geometry.geom_type.unique()),
            'has_spatial_index': True,  # GPKG automatically creates spatial index
            'crs': str(gdf.crs) if gdf.crs else None,
            'bounds': gdf.total_bounds.tolist() if len(gdf) > 0 else None
        }
        
        # Check feature count if expected
        if expected_features_count and len(gdf) != expected_features_count:
            validation_results['warning'] = f"Feature count mismatch: expected {expected_features_count}, got {len(gdf)}"
        
        # Check for required FEMA metadata columns
        required_columns = ['fema_state_code', 'fema_county_code', 'fema_source_file']
        missing_columns = [col for col in required_columns if col not in gdf.columns]
        if missing_columns:
            validation_results['warning'] = f"Missing metadata columns: {missing_columns}"
        
        # Check geometry validity
        if len(gdf) > 0:
            invalid_geometries = (~gdf.geometry.is_valid).sum()
            if invalid_geometries > 0:
                validation_results['warning'] = f"{invalid_geometries} invalid geometries found"
        
        return validation_results
        
    except Exception as e:
        return {'valid': False, 'error': str(e)}

def validate_all_outputs(config, conn, logger=None):
    """Validate all GPKG outputs."""
    logger = logger or logging.getLogger(__name__)
    
    output_base = config['processing']['merged_output_path']
    validation_results = {
        'total_files_validated': 0,
        'valid_files': 0,
        'invalid_files': 0,
        'warnings': 0,
        'file_details': []
    }
    
    # Get all processing log entries
    cursor = conn.cursor()
    cursor.execute('''
        SELECT state_code, shapefile_type, output_gpkg_path, total_features_merged
        FROM shapefile_processing_log
        WHERE processing_success = 1
    ''')
    
    processed_files = cursor.fetchall()
    
    for state_code, shapefile_type, output_path, expected_features in processed_files:
        if not output_path or not os.path.exists(output_path):
            continue
            
        validation_result = validate_gpkg_output(output_path, expected_features, logger)
        
        file_detail = {
            'state_code': state_code,
            'shapefile_type': shapefile_type,
            'output_path': output_path,
            'validation_result': validation_result
        }
        
        validation_results['file_details'].append(file_detail)
        validation_results['total_files_validated'] += 1
        
        if validation_result['valid']:
            validation_results['valid_files'] += 1
            if 'warning' in validation_result:
                validation_results['warnings'] += 1
                logger.warning(f"  {shapefile_type}: {validation_result['warning']}")
            else:
                logger.info(f"  ✓ {shapefile_type}: {validation_result['feature_count']} features, {validation_result['file_size_mb']:.1f}MB")
        else:
            validation_results['invalid_files'] += 1
            logger.error(f"  ✗ {shapefile_type}: {validation_result['error']}")
    
    return validation_results

def generate_final_report(extraction_results, merging_results, validation_results, logger=None):
    """Generate comprehensive final processing report."""
    logger = logger or logging.getLogger(__name__)
    
    logger.info("\n" + "=" * 80)
    logger.info("FINAL PROCESSING REPORT")
    logger.info("=" * 80)
    
    # Extraction summary
    logger.info(f"\n📦 ZIP EXTRACTION SUMMARY:")
    logger.info(f"  Total ZIP files found: {extraction_results.get('total', 0)}")
    logger.info(f"  Successfully extracted: {extraction_results.get('extracted', 0)}")
    logger.info(f"  Failed extractions: {extraction_results.get('failed', 0)}")
    logger.info(f"  Already extracted (skipped): {extraction_results.get('skipped', 0)}")
    
    # Merging summary
    logger.info(f"\n🗺️  SHAPEFILE MERGING SUMMARY:")
    logger.info(f"  States processed: {merging_results.get('states_processed', 0)}")
    logger.info(f"  GPKG files created: {merging_results.get('gpkg_files_created', 0)}")
    logger.info(f"  Total features merged: {merging_results.get('total_features_merged', 0):,}")
    logger.info(f"  Failed merges: {merging_results.get('failed_merges', 0)}")
    
    # Validation summary
    logger.info(f"\n✅ VALIDATION SUMMARY:")
    logger.info(f"  Files validated: {validation_results.get('total_files_validated', 0)}")
    logger.info(f"  Valid files: {validation_results.get('valid_files', 0)}")
    logger.info(f"  Invalid files: {validation_results.get('invalid_files', 0)}")
    logger.info(f"  Files with warnings: {validation_results.get('warnings', 0)}")
    
    # Calculate total output size
    total_size_mb = 0
    for file_detail in validation_results.get('file_details', []):
        if file_detail['validation_result']['valid']:
            total_size_mb += file_detail['validation_result']['file_size_mb']
    
    logger.info(f"\n📊 OUTPUT STATISTICS:")
    logger.info(f"  Total output size: {total_size_mb:.1f} MB ({total_size_mb/1024:.1f} GB)")
    logger.info(f"  Average file size: {total_size_mb/max(1, validation_results.get('valid_files', 1)):.1f} MB")
    
    # Success rate
    extraction_success_rate = (extraction_results.get('extracted', 0) / max(1, extraction_results.get('total', 1))) * 100
    merging_success_rate = ((merging_results.get('gpkg_files_created', 0)) / max(1, merging_results.get('gpkg_files_created', 0) + merging_results.get('failed_merges', 0))) * 100
    validation_success_rate = (validation_results.get('valid_files', 0) / max(1, validation_results.get('total_files_validated', 1))) * 100
    
    logger.info(f"\n📈 SUCCESS RATES:")
    logger.info(f"  Extraction success: {extraction_success_rate:.1f}%")
    logger.info(f"  Merging success: {merging_success_rate:.1f}%")
    logger.info(f"  Validation success: {validation_success_rate:.1f}%")
    
    logger.info("\n" + "=" * 80)

def cleanup_temporary_files(config, logger=None):
    """Clean up temporary extraction files."""
    logger = logger or logging.getLogger(__name__)
    
    extraction_base = config['processing']['extraction_base_path']
    temp_dir = config['processing']['temp_directory']
    
    cleanup_dirs = [extraction_base, temp_dir]
    
    for cleanup_dir in cleanup_dirs:
        if os.path.exists(cleanup_dir):
            try:
                shutil.rmtree(cleanup_dir)
                logger.info(f"Cleaned up temporary directory: {cleanup_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up {cleanup_dir}: {e}")

def clear_processing_logs(conn, target_states=None, logger=None):
    """Clear all processing logs to force rebuild from scratch."""
    logger = logger or logging.getLogger(__name__)
    
    cursor = conn.cursor()
    
    if target_states:
        state_placeholders = ','.join(['?' for _ in target_states])
        
        # Clear extraction logs for target states
        cursor.execute(f'''
            DELETE FROM extraction_log
            WHERE state_code IN ({state_placeholders})
        ''', target_states)
        
        # Clear processing logs for target states
        cursor.execute(f'''
            DELETE FROM shapefile_processing_log
            WHERE state_code IN ({state_placeholders})
        ''', target_states)
        
        # Clear contribution logs for target states
        cursor.execute(f'''
            DELETE FROM shapefile_contributions
            WHERE state_code IN ({state_placeholders})
        ''', target_states)
        
        logger.info(f"Cleared processing logs for states: {target_states}")
    else:
        # Clear all processing logs
        cursor.execute('DELETE FROM extraction_log')
        cursor.execute('DELETE FROM shapefile_processing_log')
        cursor.execute('DELETE FROM shapefile_contributions')
        
        logger.info("Cleared all processing logs")
    
    conn.commit()
    
    # Get counts after clearing
    cursor.execute('SELECT COUNT(*) FROM extraction_log')
    extraction_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM shapefile_processing_log')
    processing_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM shapefile_contributions')
    contribution_count = cursor.fetchone()[0]
    
    logger.info(f"Remaining logs: {extraction_count} extractions, {processing_count} processing, {contribution_count} contributions")

def main():
    """Main processing function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract and merge FEMA flood risk shapefiles')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    parser.add_argument('--states', help='Comma-separated list of state codes to process (e.g., 01,02,04)')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted processing')
    parser.add_argument('--force-rebuild', action='store_true', help='Force rebuild - clear all processing logs and start from scratch')
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
        logger.info("=== FEMA Shapefile Extraction and Merging ===")
        logger.info(f"Start time: {datetime.now()}")
        
        config = load_config(args.config)
        conn = setup_database(config)
        
        # Handle force rebuild option
        if args.force_rebuild:
            logger.info("FORCE REBUILD MODE - Clearing all processing logs")
            clear_processing_logs(conn, target_states, logger)
            
            # Also remove existing GPKG files for target states
            output_base = config['processing']['merged_output_path']
            if target_states:
                for state_code in target_states:
                    state_output_dir = os.path.join(output_base, state_code)
                    if os.path.exists(state_output_dir):
                        try:
                            shutil.rmtree(state_output_dir)
                            logger.info(f"Removed existing output directory: {state_output_dir}")
                        except Exception as e:
                            logger.warning(f"Failed to remove {state_output_dir}: {e}")
            else:
                if os.path.exists(output_base):
                    try:
                        shutil.rmtree(output_base)
                        logger.info(f"Removed existing output directory: {output_base}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {output_base}: {e}")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No actual processing will be performed")
            zip_files = get_downloaded_zip_files(config, conn, target_states)
            logger.info(f"Would process {len(zip_files)} downloaded ZIP files")
            return
        
        # Phase 2: ZIP File Extraction
        logger.info("\n=== Phase 1: ZIP File Extraction ===")
        extraction_results = extract_all_zip_files(config, conn, target_states, logger)
        
        # Phase 3: Shapefile Discovery and Categorization
        logger.info("\n=== Phase 2: Shapefile Discovery ===")
        shapefile_inventory = discover_and_categorize_shapefiles(config, conn, target_states, logger)
        
        # Phase 4: GPKG Merging by State and Type
        logger.info("\n=== Phase 3: GPKG Merging ===")
        merging_results = merge_shapefiles_by_state(config, conn, shapefile_inventory, logger)
        
        # Phase 5: Validation and Reporting
        logger.info("\n=== Phase 4: Validation and Reporting ===")
        validation_results = validate_all_outputs(config, conn, logger)
        
        # Final Summary
        generate_final_report(extraction_results, merging_results, validation_results, logger)
        
        # Cleanup temporary files
        if not args.no_cleanup:
            logger.info("\n=== Cleanup ===")
            cleanup_temporary_files(config, logger)
        
        logger.info(f"\nProcessing completed at: {datetime.now()}")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()