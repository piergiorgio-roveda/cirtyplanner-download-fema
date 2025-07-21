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
import shutil
import tempfile
from pathlib import Path
from datetime import datetime

# Hardcoded list of filename groups to process
# These are the filenames without extension that will be merged
DEFAULT_FILENAME_GROUPS = [
  's_frd_proj_ar', # 535
  's_huc_ar', # 509
  's_cslf_ar', # 457
  's_frd_pol_ar', # 282
  's_carto_ln', # 265
  's_carto_ar', # 261
  's_cenblk_ar', # 223
  's_frm_callout_ln', # 211
  's_aomi_pt', # 200
  's_frac_ar', # 164
  's_carto_pt', # 158
  's_aomi_ar', # 128
  's_udf_pt', # 112
  's_cr_fac_pt', # 57
  's_levee_ln', # 57
  's_inc_flood_scen_ar', # 52
  's_simpl_cst_zone_ar', # 52
  's_simple_cst_zone_ar', # 52
  's_rm_dams_pt', # 48
  's_dams_xs_ln', # 47
  's_ds_inundation_ar', # 47
  's_us_inundation_ar', # 47
  's_easement_ar', # 46
  's_lev_breach_pt', # 46
  's_lev_elements_pt', # 46
  's_lev_freeboard_ln', # 46
  's_lev_inundation_ar', # 46
  's_lev_rating_curve_pt', # 46
  's_pfd_ar', # 45
  's_fras_pt', # 39
  's_cenblk_ar_2000', # 26
  's_cenblk_ar_2010', # 22
  's_cenblk_ar_teif1', # 18
  'county', # 17
  'nativeprj_cslf_change_bypanel', # 16
  'state', # 13
  'watershed', # 13
  's_cslf_ar_countywide', # 10
  's_cst_inc_inundation_ar', # 9
  'nativeprj_cslf_change', # 6
  'counties', # 5
  's_cst_wave_haz_ar', # 4
  's_erdune_pk_ln', # 4
  's_pfd_ersn_ar', # 4
  's_pol_ar', # 4
  's_pot_high_risk_pt', # 4
  's_res_cslf', # 4
  's_res_sfha_new', # 4
  'states', # 4
  'r_buildingfootprints', # 3
  'r_parcels', # 3
  's_cenblk_ar_teif2_lu', # 3
  's_cslf_countywide', # 3
  's_fld_haz_ar', # 3
  's_frd_prj_ar', # 3
  's_res_aomi', # 3
  's_res_sfha_effective', # 3
  's_tile_index_01pct', # 3
  's_tile_index_02pct', # 3
  's_tile_index_04pct', # 3
  's_tile_index_0_2pct', # 3
  's_tile_index_10pct', # 3
  's_tile_index_pct30yrcha', # 3
  's_tile_index_pctanncha', # 3
  'frd_harris_cslf_shapefiles_20140228', # 2
  'ocean', # 2
  'pol_ar_census', # 2
  'r_udf_by_point', # 2
  's_carto_lnanno', # 2
  's_cenblk_ar_teif2_cb', # 2
  's_cenblk_ar_teif2_lbf', # 2
  's_cenblk_teif1', # 2
  's_frd_pol_aranno', # 2
  's_frm_callout', # 2
  's_simple_cst_zone', # 2
  'studyarea_mask', # 2
  'address_points_20170409', # 1
  'aomi_pt', # 1
  'block_all', # 1
  'building_all', # 1
  'building_input', # 1
  'census_block', # 1
  'county_boundaries', # 1
  'criticalfacilities_all', # 1
  'cslf', # 1
  'cslf_change', # 1
  'cslf_change_bypanel', # 1
  'cslf_sum', # 1
  'dekalb_highwatermarks', # 1
  'disseff', # 1
  'dissnew', # 1
  'dp_01pct_tile_index', # 1
  'dp_02pct_tile_index', # 1
  'dp_04pct_tile_index', # 1
  'dp_0_2pct_tile_index', # 1
  'dp_10pct_tile_index', # 1
  'ef_firestations_201703', # 1
  'ef_hospitals_201703', # 1
  'ef_medicalfacilities_201703', # 1
  'ef_physicianclinics_201703', # 1
  'ef_police_201703', # 1
  'ef_schools_201703', # 1
  'erosion_hazard_area_wa_kingcounty', # 1
  'essentialfacilities_all', # 1
  'flood_hazard_area_preliminary_fema', # 1
  'fp_03061430', # 1
  'fp_03061500', # 1
  'fp_03062224_03062225', # 1
  'fp_03062245_03062250', # 1
  'fp_03062445', # 1
  'fp_03062450', # 1
  'fp_03062500', # 1
  'fp_03062998_03063000', # 1
  'frd_40121c_cslf_shapefiles_20140223', # 1
  'frd_brazos_cslf_shapefiles_20140228', # 1
  'frd_kay_osage_cslf_shapefiles_20140223', # 1
  'frd_lubbock_cslf_shapefiles_20140228', # 1
  'frd_nowata_cslf_shapefiles_20140223', # 1
  'frm_cartoln_anno', # 1
  'frm_polar_anno', # 1
  'frm_watershed_anno', # 1
  'greenwood_mask', # 1
  'huc12_clip', # 1
  'hydroforce_villagefonda', # 1
  'hydroforce_villagefortplain', # 1
  'hydrostaticforce_villagefonda', # 1
  'ii_m71_tacoma', # 1
  'ii_m72_seattle_middle', # 1
  'ii_m72_seattle_north', # 1
  'ii_m72_seattle_south', # 1
  'ii_m90_cascadia', # 1
  'increasedsfha_zone', # 1
  'landslide_hazard_area', # 1
  'landslide_hazard_area_wa_dnr', # 1
  'landslide_hazard_area_wa_kingcounty', # 1
  'liquefaction_susceptibility_wa_dnr', # 1
  'lomc_pts', # 1
  'losses4frm_cenblk_lrarefined_join', # 1
  'nbi_laurens', # 1
  'newberry_mask', # 1
  'overtoppedculverts', # 1
  'parcel_201703', # 1
  'parcels_withinincreasedsfha', # 1
  'pct30yrch_tile_index', # 1
  'pctanncha_tile_index', # 1
  'pga_m71_tacoma', # 1
  'pga_m72_seattle_middle', # 1
  'pga_m72_seattle_north', # 1
  'pga_m72_seattle_south', # 1
  'pga_m90_cascadia', # 1
  'pgv_m71_tacoma', # 1
  'pgv_m72_seattle_middle', # 1
  'pgv_m72_seattle_north', # 1
  'pgv_m72_seattle_south', # 1
  'pgv_m90_cascadia', # 1
  'place_community', # 1
  'place_county', # 1
  'place_tribe', # 1
  'political', # 1
  'projeffect1', # 1
  'projnew', # 1
  'projpolar', # 1
  'psa03_m71_tacoma', # 1
  'psa03_m72_seattle_middle', # 1
  'psa03_m72_seattle_north', # 1
  'psa03_m72_seattle_south', # 1
  'psa03_m90_cascadia', # 1
  'psa10_m71_tacoma', # 1
  'psa10_m72_seattle_middle', # 1
  'psa10_m72_seattle_north', # 1
  'psa10_m72_seattle_south', # 1
  'psa10_m90_cascadia', # 1
  'psa30_m71_tacoma', # 1
  'psa30_m72_seattle_middle', # 1
  'psa30_m72_seattle_north', # 1
  'psa30_m72_seattle_south', # 1
  'psa30_m90_cascadia', # 1
  'r_udf_losses_by_building', # 1
  'r_udf_losses_by_parcel', # 1
  'r_udf_losses_by_point', # 1
  'refinedloss_bycensusblock_frm', # 1
  'repetitiveloss_structures', # 1
  'rivers_and_streams', # 1
  's_aomi_pt_1', # 1
  's_carto_ar_trns_buffer', # 1
  's_carto_aranno', # 1
  's_carto_ln__hydro_anno', # 1
  's_carto_ln__trans_anno', # 1
  's_cenblik_ar', # 1
  's_cenblk_ar_countywide', # 1
  's_cenblk_ar_newcastle', # 1
  's_cenblk_ar_teif2', # 1
  's_cenblk_ar_udf', # 1
  's_clsf_countywide', # 1
  's_cslf', # 1
  's_cslf_ar_1', # 1
  's_cslf_ar_cw', # 1
  's_cslf_q3', # 1
  's_cst_erosion_ar', # 1
  's_cst_overtop_ar', # 1
  's_cst_tsct_ln', # 1
  's_frac_ar_teif2_1', # 1
  's_frac_udf_ar', # 1
  's_frap_udf_ar', # 1
  's_frd_callout_ln', # 1
  's_frd_proj_a', # 1
  's_gen_struct', # 1
  's_lidar_bf_ar', # 1
  's_pfd_ln', # 1
  's_polaranno', # 1
  's_proj_ar', # 1
  's_res_aomi_jefferson', # 1
  's_res_aomi_orange', # 1
  's_res_aomi_washington', # 1
  's_res_sfha', # 1
  's_tsunami_ar', # 1
  's_tsunami_pt', # 1
  'selected_watershed_boundaryanno', # 1
  'sioux_cslf_05222019', # 1
  'state_boundaries', # 1
  'study_area_mask', # 1
  'temp_cslf', # 1
  'tile_all', # 1
  'tsunami_hazard_area_wa_dnr', # 1
  'velocity_direction', # 1
  'volcanic_hazard_areas_king_usgs', # 1
  'volcanic_hazard_areas_usgs', # 1
  'water_bodies', # 1
  'watershed_1', # 1
  'watershed_mask100', # 1
  'wsel_01pct_bfe_plus_1', # 1
  'wsel_01pct_bfe_plus_2', # 1
  'wsel_01pct_bfe_plus_3', # 1
  'x_aomi_pt', # 1
  'x_frd_pol_ar', # 1
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
                'D:\\git\\cityplanner-desktop\\download-fema\\.TMP'
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
    
    # Convert filename to lowercase to ensure case-insensitive matching
    filename = filename.lower()
    
    cursor.execute('''
        SELECT product_name, gpkg_path
        FROM clean_conversion_table
        WHERE filename = ?
    ''', (filename,))
    
    files = cursor.fetchall()
    logger.info(f"Found {len(files)} files with filename '{filename}'")
    
    return files

def sanitize_field_name(field_name):
    """
    Sanitize field name by:
    1. Converting to lowercase
    2. Replacing non-alphanumeric, non-underscore characters with underscores
    """
    import re
    # Convert to lowercase
    field_name = field_name.lower()
    # Replace non-alphanumeric, non-underscore characters with underscores
    sanitized = re.sub(r'[^a-z0-9_]', '_', field_name)
    return sanitized

def should_filter_column(column_name):
    """Check if a column name should be filtered out."""
    # Convert to lowercase for case-insensitive comparison
    col_lower = column_name.lower()
    
    # List of patterns to filter out
    filter_patterns = [
        'shape_len',
        'shape_length',
        'shape_area',
        'st_length',
        'st_area'
    ]
    
    # Check if the column name contains any of the filter patterns
    for pattern in filter_patterns:
        if pattern in col_lower:
            return True
    
    return False

def analyze_schema(gpkg_path, logger):
    """Analyze the schema of a GeoPackage file to get column names and geometry column."""
    try:
        # Create a temporary directory for ogrinfo output
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, "schema_info.txt")
            
            # Get the layer name (same as filename without extension)
            layer_name = os.path.splitext(os.path.basename(gpkg_path))[0]
            
            # Run ogrinfo to get schema information
            cmd = [
                'ogrinfo',
                '-so',  # Summary only
                gpkg_path,
                layer_name
            ]
            
            with open(output_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
            
            if result.returncode != 0:
                logger.error(f"ogrinfo error: {result.stderr}")
                return [], None
            
            # Parse the output to extract column names and geometry column
            columns = []
            geometry_column = None
            filtered_columns = []
            with open(output_file, 'r') as f:
                for line in f:
                    if "Geometry Column" in line:
                        # Extract geometry column name
                        geometry_column = line.split("=")[1].strip()
                    elif ":" in line and not line.strip().startswith("INFO"):
                        # This is likely a column definition
                        column_name = line.split(':')[0].strip()
                        if column_name and column_name not in ['Geometry', 'FID']:
                            # Check if the column should be filtered out
                            if should_filter_column(column_name):
                                filtered_columns.append(column_name)
                            else:
                                # Sanitize column name (lowercase and replace non-alphanumeric chars)
                                columns.append(sanitize_field_name(column_name))
            
            # Don't log filtered columns as per user request
            
            if not geometry_column:
                logger.warning(f"No geometry column found in {gpkg_path}")
                # Default to common geometry column names
                geometry_column = "GEOMETRY"
            
            return columns, geometry_column
    except Exception as e:
        logger.error(f"Error analyzing schema: {str(e)}")
        return []

def merge_gpkg_files(files, filename, output_path, temp_dir, logger, force_rebuild=False):
    """Merge GeoPackage files using ogr2ogr with improved schema handling."""
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
        # Analyze schemas of all files to identify all possible columns and geometry column
        logger.info(f"Analyzing schemas for {len(files)} files with filename {filename}...")
        all_columns = set()
        file_columns = {}
        geometry_columns = {}
        
        for product_name, gpkg_path in files:
            columns, geom_col = analyze_schema(gpkg_path, logger)
            file_columns[(product_name, gpkg_path)] = columns
            geometry_columns[(product_name, gpkg_path)] = geom_col
            all_columns.update(columns)
        
        # Remove 'product_name' if it exists as we'll add it ourselves
        if 'product_name' in all_columns:
            all_columns.remove('product_name')
        
        logger.info(f"Found {len(all_columns)} unique columns across all files")
        
        # Determine the most common geometry column name
        geom_col_counts = {}
        for geom_col in geometry_columns.values():
            if geom_col not in geom_col_counts:
                geom_col_counts[geom_col] = 0
            geom_col_counts[geom_col] += 1
        
        # Use the most common geometry column name
        common_geom_col = max(geom_col_counts.items(), key=lambda x: x[1])[0]
        logger.info(f"Using geometry column name: {common_geom_col}")
        
        # Create a unique temporary file
        unique_id = str(uuid.uuid4())[:8]
        temp_output = os.path.join(temp_dir, f"{filename}_{unique_id}.gpkg")
        
        # First file is handled differently - we copy it to the output
        first_file = files[0][1]  # gpkg_path of first file
        first_product = files[0][0]  # product_name of first file
        
        # Remove verbose logging
        
        # Build SQL query with all columns (using NULL for missing columns)
        first_file_columns = file_columns[(first_product, first_file)]
        first_geom_col = geometry_columns[(first_product, first_file)]
        sql_columns = []
        
        # Add geometry column first with sanitized name
        sql_columns.append(f"{first_geom_col} AS {sanitize_field_name(first_geom_col)}")
        
        for col in all_columns:
            if col in first_file_columns:
                sql_columns.append(f'"{col}" AS "{sanitize_field_name(col)}"')
            else:
                sql_columns.append(f'NULL AS "{sanitize_field_name(col)}"')
        
        # Add product_name column
        sql_columns.append(f"'{first_product}' AS product_name")
        
        # Build the SQL query
        layer_name = os.path.splitext(os.path.basename(first_file))[0]
        sql_query = f"SELECT {', '.join(sql_columns)} FROM {layer_name}"
        
        # Copy first file to temp output with SQL to handle schema
        cmd = [
            'ogr2ogr',
            '-f', 'GPKG',
            '-sql', sql_query,
            '-nln', filename,  # Explicitly set the layer name to avoid "SELECT" layer
            '-geomfield', first_geom_col,  # Explicitly specify the geometry column
            temp_output,
            first_file
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"ogr2ogr error: {result.stderr}")
            return False, result.stderr
        
        # Append each subsequent file
        for i, (product_name, gpkg_path) in enumerate(files[1:], 2):
            # Remove verbose logging
            
            # Build SQL query with all columns (using NULL for missing columns)
            file_cols = file_columns[(product_name, gpkg_path)]
            file_geom_col = geometry_columns[(product_name, gpkg_path)]
            sql_columns = []
            
            # Add geometry column first with sanitized name
            sql_columns.append(f"{file_geom_col} AS {sanitize_field_name(file_geom_col)}")
            
            for col in all_columns:
                if col in file_cols:
                    sql_columns.append(f'"{col}" AS "{sanitize_field_name(col)}"')
                else:
                    sql_columns.append(f'NULL AS "{sanitize_field_name(col)}"')
            
            # Add product_name column
            sql_columns.append(f"'{product_name}' AS product_name")
            
            # Build the SQL query
            layer_name = os.path.splitext(os.path.basename(gpkg_path))[0]
            sql_query = f"SELECT {', '.join(sql_columns)} FROM {layer_name}"
            
            # Build ogr2ogr command with SQL to handle schema differences
            cmd = [
                'ogr2ogr',
                '-f', 'GPKG',
                '-update',
                '-append',
                '-skipfailures',  # Add skipfailures option for append operations
                '-sql', sql_query,
                '-nln', filename,  # Explicitly set the layer name to avoid "SELECT" layer
                '-geomfield', file_geom_col,  # Explicitly specify the geometry column
                temp_output,
                gpkg_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"ogr2ogr error: {result.stderr}")
                # Continue with next file despite error
                continue
        
        # Move the temp file to the final destination
        if os.path.exists(output_path):
            os.remove(output_path)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Use copy2 and remove instead of rename for cross-drive operations
        # This handles the case where temp_dir and output_path are on different drives
        shutil.copy2(temp_output, output_path)
        os.remove(temp_output)
        
        # Simplified success message
        logger.info(f"Merged {len(files)} files into {output_path}")
        
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
    
    # Get merge statistics - get only the most recent entry for each filename_group
    cursor.execute('''
        SELECT
            m1.filename_group,
            m1.source_files_count,
            m1.merged_gpkg_path,
            m1.merge_success
        FROM merge_06d_log m1
        INNER JOIN (
            SELECT filename_group, MAX(merge_timestamp) as latest_timestamp
            FROM merge_06d_log
            WHERE filename_group IN ({})
            GROUP BY filename_group
        ) m2 ON m1.filename_group = m2.filename_group AND m1.merge_timestamp = m2.latest_timestamp
        ORDER BY m1.filename_group
    '''.format(','.join(['?'] * len(processed_filenames))), processed_filenames)
    
    results = cursor.fetchall()
    
    logger.info(f"\n[MERGE RESULTS]:")
    
    success_count = 0
    failed_count = 0
    
    # Track processed filenames to avoid duplicates
    processed = set()
    
    for filename, source_count, path, success in results:
        # Skip if we've already processed this filename
        if filename in processed:
            continue
            
        processed.add(filename)
        
        status = "SUCCESS" if success else "FAILED"
        if success:
            success_count += 1
        else:
            failed_count += 1
            
        logger.info(f"  {filename}: {status} - {source_count} source files -> {os.path.basename(path)}")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"MERGE COMPLETED - {success_count} successful, {failed_count} failed")
    logger.info("=" * 80)
    
    # Add note about layer naming and schema handling
    logger.info("\nNOTE ON LAYER NAMING AND SCHEMA HANDLING:")
    logger.info("- Using explicit layer naming (-nln option) to prevent multiple tables in output")
    logger.info("- All features are stored in a single layer named after the filename group")
    logger.info("- The 'product_name' field tracks the source of each feature")
    logger.info("- Schema differences are handled by analyzing all files before merging")
    logger.info("- All columns from all files are included in the merged output")
    logger.info("- Missing columns are filled with NULL values")
    logger.info("- New fields in subsequent files are properly included in the merged output")
    logger.info("- Field names are sanitized: lowercase and non-alphanumeric chars replaced with underscores")

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