# Script 06a: Extract ZIP Files Implementation

**File:** `notebooks/06a_extract_zip_files.py`  
**Created:** 2025-07-21  
**Status:** ✅ Implementation Complete

## Purpose

Extracts ZIP files downloaded by script 05 containing FEMA flood risk shapefiles. This script focuses solely on the extraction process, organizing files by product name rather than state/county hierarchy, and provides detailed tracking of each shapefile found within ZIP archives.

**Important:** This script does NOT download files - it processes files already downloaded by script 05.

## Input Sources

- **Downloaded ZIPs:** Retrieved from `download_log` table (from script 05)
- **SQLite Database:** `meta_results/flood_risk_shapefiles.db` (from scripts 04-05)
- **Configuration:** `config.json` for processing parameters

**Note:** The script uses a SQL query to get unique product files, avoiding duplicate processing of the same product from different locations.

## Output Structure

```
E:\FEMA_EXTRACTED\
├── FRD_01001C_Shapefiles\        # Product name as folder
│   ├── S_CSLF_Ar.shp
│   ├── S_CSLF_Ar.dbf
│   ├── S_CSLF_Ar.shx
│   ├── S_CSLF_Ar.prj
│   └── ... (all extracted files)
├── FRD_03150201_shapefiles_20140221\
│   ├── Shapefiles\               # Subfolder if present in ZIP
│   │   ├── R_UDF_Losses_by_Building.shp
│   │   └── ... (other files)
│   └── ... (all extracted files)
└── ... (all products)
```

## Process Flow

### Phase 1: Setup and Initialization
1. **Parse Arguments** - Process command-line options
2. **Load Configuration** - Read settings from config.json
3. **Setup Database** - Connect to SQLite database and create tables
4. **Setup Logging** - Configure logging system

### Phase 2: ZIP File Discovery
1. **Query Unique Products** - Use SQL to get unique product files:
   ```sql
   SELECT dl.product_name, MIN(dl.file_path) AS file_path
   FROM download_log dl
   WHERE dl.file_path IS NOT NULL
   GROUP BY dl.product_name;
   ```
2. **Check Already Extracted** - Skip products already in extraction_06a_log
3. **Build Processing List** - Create list of ZIP files to extract

### Phase 3: ZIP Extraction
1. **Process Each ZIP** - Extract contents to product-named folder
2. **Track Shapefiles** - Log each .shp file found in ZIP
3. **Handle Subfolders** - Preserve internal folder structure
4. **Log Results** - Record extraction details in database

### Phase 4: Reporting and Cleanup
1. **Generate Report** - Create extraction summary
2. **Cleanup Temporary Files** - Remove temporary processing files
3. **Update Database** - Log final extraction results

## Database Schema Extensions

### New Table

```sql
-- Track ZIP extraction with detailed shapefile logging
CREATE TABLE extraction_06a_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    zip_file_path TEXT NOT NULL,
    extracted_path TEXT,
    shapefile_name TEXT,
    extraction_success BOOLEAN NOT NULL,
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

-- Indexes for better performance
CREATE INDEX idx_extraction_06a_product ON extraction_06a_log (product_name);
CREATE INDEX idx_extraction_06a_success ON extraction_06a_log (extraction_success);
```

## Configuration Extensions

```json
{
  "download": {
    "base_path": "E:\\FEMA_DOWNLOAD"
  },
  "processing": {
    "extraction_base_path": "E:\\FEMA_EXTRACTED",
    "temp_directory": "E:\\FEMA_TEMP",
    "memory_limit_mb": 2048
  },
  "database": {
    "path": "meta_results/flood_risk_shapefiles.db"
  }
}
```

## Dependencies

```python
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
```

## Performance Expectations

### Processing Time Estimates
- **Small dataset** (< 100 ZIP files): 5-15 minutes
- **Medium dataset** (100-500 ZIP files): 15-45 minutes
- **Large dataset** (> 500 ZIP files): 45-120 minutes

### Resource Requirements
- **Memory**: 2GB minimum, 4GB+ recommended
- **Disk space**: 2-3x the size of compressed ZIP files
- **CPU**: Single core sufficient, multi-core not utilized for extraction

### Expected Output Sizes
- **Input ZIP files**: ~50-100GB
- **Extracted files**: ~150-300GB

## Usage Examples

```bash
# Basic usage - extract all ZIP files
python notebooks/06a_extract_zip_files.py

# Process specific states only
python notebooks/06a_extract_zip_files.py --states 01,02,04

# Resume interrupted processing
python notebooks/06a_extract_zip_files.py --resume

# Use custom configuration
python notebooks/06a_extract_zip_files.py --config custom_config.json

# Dry run - show what would be processed
python notebooks/06a_extract_zip_files.py --dry-run

# Verbose output for debugging
python notebooks/06a_extract_zip_files.py --verbose
```

## Error Handling

### ZIP Extraction Errors
- Corrupted ZIP files logged with error messages
- Missing files logged and skipped
- Extraction errors recorded in database

### Memory Management
- Automatic garbage collection
- Memory usage monitoring
- Processing in batches to manage memory

## Validation and Quality Assurance

### Extraction Validation
- ZIP file existence verification
- Extraction success verification
- Shapefile discovery and logging

### Quality Reports
- Extraction summary statistics
- Success/failure counts
- Detailed logging of each shapefile

## Implementation Status

- **Architecture**: ✅ Complete
- **Database Schema**: ✅ Complete
- **Core Functions**: ✅ Complete
- **Error Handling**: ✅ Complete
- **Testing**: ✅ Complete
- **Documentation**: ✅ Complete

## Key Improvements

1. **Product-Based Organization** - Organizes by product name rather than state/county hierarchy
2. **Detailed Shapefile Tracking** - Logs each individual shapefile found
3. **Duplicate Prevention** - Uses SQL query to avoid processing duplicate products
4. **Unicode Handling** - Uses ASCII-only logging to avoid encoding issues
5. **Memory Optimization** - Improved memory management for large datasets