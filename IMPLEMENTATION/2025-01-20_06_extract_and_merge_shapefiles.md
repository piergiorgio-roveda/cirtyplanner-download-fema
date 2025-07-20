# Script 06: Extract and Merge Shapefiles Implementation

**File:** `notebooks/06_extract_and_merge_shapefiles.py`  
**Created:** 2025-07-20  
**Status:** 🚧 Implementation in Progress

## Purpose

Processes ZIP files downloaded by script 05 to extract and merge FEMA flood risk shapefiles by type within each state, creating consolidated GPKG files with enhanced metadata for geographic traceability.

**Important:** This script does NOT download files - it processes files already downloaded by script 05.

## Input Sources

- **Downloaded ZIPs:** Retrieved from `download_log` table (from script 05)
- **SQLite Database:** `meta_results/flood_risk_shapefiles.db` (from scripts 04-05)
- **Configuration:** `config.json` for processing parameters

**Note:** The script reads successfully downloaded ZIP files from the `download_log` table rather than scanning the filesystem directly, ensuring it processes only verified downloads.

## Output Structure

```
merged/
├── 01/                           # Alabama
│   ├── R_UDF_Losses_by_Building.gpkg
│   ├── R_UDF_Losses_by_Parcel.gpkg
│   ├── R_UDF_Losses_by_Point.gpkg
│   ├── S_AOMI_Pt.gpkg
│   ├── S_Carto_Ar.gpkg
│   ├── S_Carto_Ln.gpkg
│   ├── S_CenBlk_Ar.gpkg
│   ├── S_CSLF_Ar.gpkg
│   ├── S_FRD_Pol_Ar.gpkg
│   ├── S_FRD_Proj_Ar.gpkg
│   ├── S_FRM_Callout_Ln.gpkg
│   ├── S_HUC_Ar.gpkg
│   ├── S_UDF_Pt.gpkg
│   └── processing_summary.json
├── 02/                           # Alaska
└── ...                           # All other states
```

## Process Flow

### Phase 1: ZIP File Extraction
1. **Query Downloaded Files** - Get successfully downloaded ZIP files from `download_log` table
2. **Extract ZIP Files** - Extract to temporary processing directory
3. **Validate Extraction** - Verify file integrity and completeness
4. **Log Results** - Track extraction success/failure in database

### Phase 2: Shapefile Discovery
1. **Scan Extracted Files** - Find all .shp files in extracted directories
2. **Categorize by Type** - Group shapefiles by type (R_UDF_*, S_CSLF_*, etc.)
3. **Validate Shapefiles** - Check for required components (.shp, .shx, .dbf, .prj)
4. **Build Inventory** - Create processing inventory by state and type

### Phase 3: GPKG Merging
1. **Process by State** - Handle each state independently
2. **Merge by Type** - Combine all shapefiles of same type within state
3. **Add Metadata** - Enhance with state/county/community source information
4. **Standardize CRS** - Transform to consistent coordinate system (EPSG:4326)
5. **Create Spatial Index** - Add spatial indexing for performance

### Phase 4: Validation and Cleanup
1. **Validate Output** - Verify GPKG integrity and feature counts
2. **Generate Reports** - Create processing summaries and statistics
3. **Cleanup Temporary Files** - Remove extracted files and temp directories
4. **Update Database** - Log final processing results

## Database Schema Extensions

### New Tables

```sql
-- Track ZIP extraction status
CREATE TABLE extraction_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_code TEXT NOT NULL,
    county_code TEXT NOT NULL,
    product_name TEXT NOT NULL,
    zip_file_path TEXT NOT NULL,
    extraction_success BOOLEAN NOT NULL,
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_files_count INTEGER DEFAULT 0,
    shapefiles_found TEXT, -- JSON array of shapefile names
    error_message TEXT,
    FOREIGN KEY (state_code) REFERENCES states (state_code),
    FOREIGN KEY (county_code) REFERENCES counties (county_code)
);

-- Track shapefile processing and merging
CREATE TABLE shapefile_processing_log (
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
);

-- Track individual shapefile contributions
CREATE TABLE shapefile_contributions (
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
);
```

## Configuration Extensions

```json
{
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
    "parallel_processing": true,
    "max_workers": 4
  },
  "validation": {
    "geometry_validation": true,
    "fix_invalid_geometries": true,
    "skip_empty_geometries": true,
    "coordinate_precision": 6
  },
  "database": {
    "path": "meta_results/flood_risk_shapefiles.db"
  }
}
```

## Shapefile Type Categories

### Risk Assessment Shapefiles (R_*)
- `R_UDF_Losses_by_Building` - Building-level loss estimates (POLYGON)
- `R_UDF_Losses_by_Parcel` - Parcel-level loss estimates (POLYGON)
- `R_UDF_Losses_by_Point` - Point-based loss estimates (POINT)

### Spatial Reference Shapefiles (S_*)
- `S_AOMI_Pt` - AOMI reference points (POINT)
- `S_Carto_Ar` - Cartographic areas (POLYGON)
- `S_Carto_Ln` - Cartographic lines (LINESTRING)
- `S_CenBlk_Ar` - Census block areas (POLYGON)
- `S_CSLF_Ar` - CSLF areas (POLYGON)
- `S_FRD_Pol_Ar` - FRD policy areas (POLYGON)
- `S_FRD_Proj_Ar` - FRD project areas (POLYGON)
- `S_FRM_Callout_Ln` - FRM callout lines (LINESTRING)
- `S_HUC_Ar` - Hydrologic unit code areas (POLYGON)
- `S_UDF_Pt` - UDF reference points (POINT)

## Enhanced Metadata Schema

Each merged GPKG will include original shapefile attributes plus:

```python
ENHANCED_METADATA_COLUMNS = {
    'fema_state_code': 'str:2',        # Source state code
    'fema_state_name': 'str:50',       # Source state name
    'fema_county_code': 'str:5',       # Source county code
    'fema_county_name': 'str:100',     # Source county name
    'fema_community_code': 'str:10',   # Source community code
    'fema_community_name': 'str:100',  # Source community name
    'fema_product_name': 'str:100',    # Original product name
    'fema_source_file': 'str:200',     # Original shapefile path
    'fema_processing_date': 'datetime', # Processing timestamp
    'fema_original_crs': 'str:50'      # Original coordinate system
}
```

## Dependencies

```python
import geopandas as gpd
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
```

## Performance Expectations

### Processing Time Estimates
- **Small states** (< 50 ZIP files): 5-15 minutes
- **Medium states** (50-150 ZIP files): 15-45 minutes
- **Large states** (> 150 ZIP files): 45-120 minutes
- **Total processing time**: 8-24 hours (hardware dependent)

### Resource Requirements
- **Memory**: 4GB minimum, 8GB+ recommended
- **Disk space**: 500GB+ free space recommended
- **CPU**: Multi-core recommended for parallel processing

### Expected Output Sizes
- **Input ZIP files**: ~50-100GB
- **Temporary extraction**: ~150-300GB
- **Final GPKG output**: ~30-60GB

## Usage Examples

```bash
# Basic usage - process all states
python notebooks/06_extract_and_merge_shapefiles.py

# Process specific states only
python notebooks/06_extract_and_merge_shapefiles.py --states 01,02,04

# Resume interrupted processing
python notebooks/06_extract_and_merge_shapefiles.py --resume

# Use custom configuration
python notebooks/06_extract_and_merge_shapefiles.py --config custom_config.json

# Dry run - show what would be processed
python notebooks/06_extract_and_merge_shapefiles.py --dry-run

# Verbose output for debugging
python notebooks/06_extract_and_merge_shapefiles.py --verbose
```

## Error Handling

### ZIP Extraction Errors
- Corrupted ZIP files moved to quarantine folder
- Partial extractions cleaned up and retried
- Missing files logged and skipped

### Shapefile Processing Errors
- Invalid geometries repaired when possible
- Missing components (.prj, .dbf) handled gracefully
- Coordinate system issues logged and transformed

### Memory Management
- Chunked processing for large files
- Automatic garbage collection
- Memory usage monitoring and limits

## Validation and Quality Assurance

### Output Validation
- GPKG file integrity verification
- Feature count validation against source
- Geometry validity checks
- Spatial index verification
- Metadata completeness validation

### Quality Reports
- Processing summary by state
- Error logs and statistics
- Performance metrics
- Data completeness reports

## Implementation Status

- **Architecture**: ✅ Complete
- **Database Schema**: ✅ Designed
- **Core Functions**: 🚧 In Progress
- **Error Handling**: 🚧 In Progress
- **Testing**: ⏳ Pending
- **Documentation**: ✅ Complete

## Next Steps

1. **Implement Core Script** - Create main processing functions
2. **Add Error Handling** - Implement comprehensive error recovery
3. **Performance Testing** - Test with sample data
4. **Memory Optimization** - Tune for large dataset processing
5. **Validation Testing** - Verify output quality and integrity