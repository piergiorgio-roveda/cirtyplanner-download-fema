# Script 06b: Convert Shapefiles to GeoPackage Implementation

**File:** `notebooks/06b_convert_shapefiles_to_gpkg.py`  
**Created:** 2025-07-21  
**Status:** ✅ Implementation Complete

## Purpose

Converts extracted FEMA flood risk shapefiles to GeoPackage (GPKG) format. This script focuses solely on the conversion process, reading from the extraction_06a_log table to find shapefiles, and using ogr2ogr by default for robust conversion with proper handling of polygon winding order issues.

**Important:** This script does NOT extract ZIP files - it processes shapefiles already extracted by script 06a.

## Input Sources

- **Extracted Shapefiles:** Located in directories created by script 06a
- **Extraction Log:** `extraction_06a_log` table (from script 06a)
- **SQLite Database:** `meta_results/flood_risk_shapefiles.db`
- **Configuration:** `config.json` for processing parameters

**Note:** The script reads shapefile paths from the `extraction_06a_log` table rather than scanning the filesystem directly, ensuring it processes only verified extractions.

## Output Structure

```
E:\FEMA_SHAPEFILE_TO_GPKG\
├── FRD_01001C_Shapefiles\        # Product name as folder
│   ├── S_CSLF_Ar.gpkg
│   ├── S_FRD_Proj_Ar.gpkg
│   ├── S_HUC_Ar.gpkg
│   └── ... (all converted files)
├── FRD_03150201_shapefiles_20140221\
│   ├── R_UDF_Losses_by_Building.gpkg
│   └── ... (other converted files)
└── ... (all products)
```

## Process Flow

### Phase 1: Setup and Initialization
1. **Parse Arguments** - Process command-line options
2. **Load Configuration** - Read settings from config.json
3. **Setup Database** - Connect to SQLite database and create tables
4. **Setup Logging** - Configure logging system

### Phase 2: Shapefile Discovery
1. **Query Extraction Log** - Get shapefiles from extraction_06a_log:
   ```sql
   SELECT product_name, extracted_path, shapefile_name
   FROM extraction_06a_log
   WHERE extraction_success = 1
   AND shapefile_name IS NOT NULL
   ```
2. **Check Already Converted** - Skip shapefiles already in conversion_06b_log
3. **Build Processing List** - Create list of shapefiles to convert

### Phase 3: Shapefile Conversion
1. **Process Each Shapefile** - Convert to GPKG format
2. **Use ogr2ogr or GeoPandas** - Default to ogr2ogr for better handling of polygon issues
3. **Handle Encoding** - Support different encodings for attribute data
4. **Add Metadata** - Enhance with product information
5. **Log Results** - Record conversion details in database

### Phase 4: Reporting and Validation
1. **Generate Report** - Create conversion summary
2. **Validate Output** - Verify GPKG integrity
3. **Update Database** - Log final conversion results

## Database Schema Extensions

### New Table

```sql
-- Track shapefile to GPKG conversion status
CREATE TABLE conversion_06b_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    shapefile_path TEXT NOT NULL,
    gpkg_path TEXT NOT NULL,
    conversion_success BOOLEAN NOT NULL,
    conversion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

-- Indexes for better performance
CREATE INDEX idx_conversion_06b_product ON conversion_06b_log (product_name);
CREATE INDEX idx_conversion_06b_success ON conversion_06b_log (conversion_success);
```

## Configuration Extensions

```json
{
  "processing": {
    "extraction_base_path": "E:\\FEMA_EXTRACTED",
    "shapefile_to_gpkg_path": "E:\\FEMA_SHAPEFILE_TO_GPKG",
    "memory_limit_mb": 2048,
    "parallel_processing": true,
    "max_workers": 4,
    "use_geopandas": false,
    "shapefile_encoding": "UTF-8"
  },
  "database": {
    "path": "meta_results/flood_risk_shapefiles.db"
  }
}
```

## Conversion Methods

### ogr2ogr (Default)
```python
def convert_with_ogr2ogr(source_path, dest_path, encoding='UTF-8'):
    """Convert shapefile to GPKG using ogr2ogr (fixes polygon winding order issues)."""
    cmd = [
        'ogr2ogr',
        '-f', 'GPKG',
        '-nlt', 'PROMOTE_TO_MULTI',  # Convert to multi-geometries
        '-nln', os.path.splitext(os.path.basename(dest_path))[0],  # Layer name
        '-a_srs', 'EPSG:4326',  # Assign output coordinate system
        '--config', 'OGR_ENABLE_PARTIAL_REPROJECTION', 'TRUE',  # Enable partial reprojection
        '--config', 'CPL_TMPDIR', os.path.dirname(dest_path),  # Set temp directory
        '--config', 'SHAPE_ENCODING', encoding,  # Set encoding
        '-lco', 'ENCODING=UTF-8',  # Force UTF-8 output encoding
        '-skipfailures',  # Skip failures
        dest_path,  # Destination
        source_path  # Source
    ]
    
    # Execute command
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return True
```

### GeoPandas (Optional)
```python
def convert_with_geopandas(source_path, dest_path, product_name, encoding='utf-8'):
    """Convert shapefile to GPKG using GeoPandas."""
    try:
        # Read shapefile with geopandas and specified encoding
        gdf = gpd.read_file(source_path, encoding=encoding)
        
        # Add metadata
        gdf['fema_product_name'] = product_name
        gdf['fema_source_file'] = source_path
        gdf['fema_processing_date'] = datetime.now()
        
        # Write to GeoPackage
        gdf.to_file(dest_path, driver='GPKG')
        
        return True
    except UnicodeDecodeError:
        # If UTF-8 fails, try with a more permissive encoding
        if encoding == 'utf-8':
            return convert_with_geopandas(source_path, dest_path, product_name, encoding='latin-1')
        else:
            # If we've already tried an alternative encoding, raise the error
            raise
```

## Dependencies

```python
import sqlite3
import os
import json
import shutil
import argparse
import logging
import subprocess
from pathlib import Path
from datetime import datetime
import psutil
import gc
import warnings
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor
```

## Thread Safety

The script implements thread-safe database operations by creating a separate connection for each thread:

```python
def get_db_connection(config):
    """Create a new database connection for thread safety."""
    db_path = config['database']['path']
    conn = sqlite3.connect(db_path)
    return conn

def convert_shapefile_to_gpkg(shapefile_info, config, logger):
    # Create a new database connection for this thread
    thread_conn = None
    
    try:
        # Conversion logic...
        
        # Create thread-local database connection
        thread_conn = get_db_connection(config)
        
        # Use thread_conn for database operations...
    finally:
        # Close the thread-local connection
        if thread_conn is not None:
            thread_conn.close()
```

## Performance Expectations

### Processing Time Estimates
- **Small dataset** (< 100 shapefiles): 5-15 minutes
- **Medium dataset** (100-500 shapefiles): 15-45 minutes
- **Large dataset** (> 500 shapefiles): 45-120 minutes

### Resource Requirements
- **Memory**: 4GB minimum, 8GB+ recommended
- **Disk space**: 1-2x the size of extracted shapefiles
- **CPU**: Multi-core recommended for parallel processing

### Expected Output Sizes
- **Input shapefiles**: ~150-300GB
- **Output GPKG files**: ~30-60GB

## Usage Examples

```bash
# Basic usage - convert all shapefiles using ogr2ogr (default)
python notebooks/06b_convert_shapefiles_to_gpkg.py

# Use GeoPandas instead of ogr2ogr
python notebooks/06b_convert_shapefiles_to_gpkg.py --use-geopandas

# Specify encoding for handling special characters
python notebooks/06b_convert_shapefiles_to_gpkg.py --encoding latin-1

# Process specific products only
python notebooks/06b_convert_shapefiles_to_gpkg.py --products FRD_01001C_Shapefiles,FRD_01003C_Shapefiles

# Resume interrupted processing
python notebooks/06b_convert_shapefiles_to_gpkg.py --resume

# Force rebuild all conversions
python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild

# Dry run - show what would be processed
python notebooks/06b_convert_shapefiles_to_gpkg.py --dry-run

# Verbose output for debugging
python notebooks/06b_convert_shapefiles_to_gpkg.py --verbose

# Specify maximum worker threads
python notebooks/06b_convert_shapefiles_to_gpkg.py --max-workers 8
```

## Error Handling

### Conversion Errors
- Invalid geometries handled by ogr2ogr
- Encoding issues handled with fallback encodings
- Conversion errors logged with detailed messages

### Memory Management
- Automatic garbage collection
- Memory usage monitoring
- Thread-safe database operations

## Validation and Quality Assurance

### Conversion Validation
- GPKG file integrity verification
- Conversion success verification
- Detailed logging of each conversion

### Quality Reports
- Conversion summary statistics
- Success/failure counts
- Database statistics

## Implementation Status

- **Architecture**: ✅ Complete
- **Database Schema**: ✅ Complete
- **Core Functions**: ✅ Complete
- **Error Handling**: ✅ Complete
- **Testing**: ✅ Complete
- **Documentation**: ✅ Complete

## Key Improvements

1. **ogr2ogr Integration** - Uses ogr2ogr by default for robust conversion
2. **Encoding Handling** - Supports different encodings for problematic files
3. **Thread-Safe Processing** - Creates thread-local database connections
4. **Parallel Processing** - Optional multi-threaded conversion
5. **Product-Based Organization** - Organizes by product name for easier tracking