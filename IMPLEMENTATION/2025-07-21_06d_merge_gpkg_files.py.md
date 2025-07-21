# Script 06d: Merge GeoPackage Files by Filename Group

## Overview

This script merges GeoPackage files from the clean_conversion_table based on filename groups:
1. Reads clean_conversion_table to get files with a specific filename
2. Uses ogr2ogr to merge these files into a single GeoPackage file
3. Creates a merged output for each specified filename group

## Implementation Details

### Database Schema Extensions

The script creates a new table in the SQLite database:

#### merge_06d_log

```sql
CREATE TABLE IF NOT EXISTS merge_06d_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename_group TEXT NOT NULL,
    source_files_count INTEGER NOT NULL,
    merged_gpkg_path TEXT NOT NULL,
    merge_success BOOLEAN NOT NULL,
    merge_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
)
```

Indexes:
- `idx_merge_06d_filename` on `filename_group`
- `idx_merge_06d_success` on `merge_success`

### Process Flow

1. **Setup and Initialization**
   - Load configuration from config.json
   - Connect to SQLite database
   - Create merge_06d_log table if it doesn't exist
   - Determine which filename groups to process (from command line or defaults)

2. **File Collection**
   - For each filename group, query clean_conversion_table for matching files
   - Group files by filename

3. **Merge Process**
   - For each filename group:
     - Create temporary directory for processing
     - Copy first file to temporary output
     - Append each subsequent file using ogr2ogr
     - Move final merged file to destination

4. **Reporting**
   - Log merge results to database
   - Generate statistics about merge operations
   - Output detailed report to console and log file

### Key Functions

- `get_files_by_filename(conn, filename, logger)`: Gets all files with the specified filename
- `merge_gpkg_files(files, filename, output_path, temp_dir, logger, force_rebuild)`: Merges files using ogr2ogr
- `process_filename_group(conn, filename, config, logger, force_rebuild)`: Processes a single filename group
- `generate_report(conn, processed_filenames, logger)`: Generates merge statistics report

## Configuration Options

The script uses the following configuration options from config.json:

```json
{
  "database": {
    "path": "meta_results/flood_risk_shapefiles.db"
  },
  "processing": {
    "merged_gpkg_path": "E:\\FEMA_MERGED",
    "temp_merge_path": "D:\\git\\cityplanner-desktop\\download-fema\\.TMP_MERGE"
  }
}
```

### Default Filename Groups

The script includes a hardcoded list of default filename groups to process:

```python
DEFAULT_FILENAME_GROUPS = [
    'S_FRD_Proj_Ar',
    'S_HUC_Ar',
    'S_CSLF_Ar'
]
```

### Command Line Options

- `--config PATH`: Use custom configuration file (default: config.json)
- `--filenames NAMES`: Process specific filename groups (comma-separated)
- `--force-rebuild`: Overwrite existing merged files
- `--verbose`: Enable verbose logging

## Usage Examples

### Basic Usage

```bash
# Must be run from OSGeo4W console
cd /d d:\git\cityplanner-desktop\download-fema
python notebooks/06d_merge_gpkg_files.py
```

### Process Specific Filename Groups

```bash
python notebooks/06d_merge_gpkg_files.py --filenames S_FRD_Proj_Ar,S_HUC_Ar
```

### Force Rebuild

```bash
python notebooks/06d_merge_gpkg_files.py --force-rebuild
```

### Verbose Logging

```bash
python notebooks/06d_merge_gpkg_files.py --verbose
```

## Performance Considerations

- The script uses a fast temporary directory (D: drive) for better performance
- Merging large GeoPackage files can be memory and CPU intensive
- The script processes one filename group at a time to manage resources
- For very large datasets, consider increasing available memory

## Error Handling

- Database connection errors are caught and reported
- ogr2ogr errors are captured and logged
- File system errors (access, permissions) are handled
- All errors are logged to both console and log file
- Merge failures are recorded in the database for later analysis

## Dependencies

- Standard Python libraries (sqlite3, os, json, argparse, logging, subprocess, uuid)
- External dependency: ogr2ogr command-line tool (from OSGeo4W)
- Must be run from an OSGeo4W console or environment with ogr2ogr in PATH

## Output Example

```
=== Merging GeoPackage Files by Filename Group (Script 06d) ===
Start time: 2025-07-21 10:30:00.000000

=== Processing Filename Groups ===
Processing filename group: S_FRD_Proj_Ar
Found 1254 files with filename 'S_FRD_Proj_Ar'
Starting with first file: FRD_01001C_Shapefiles - S_FRD_Proj_Ar.gpkg
Appending file 2/1254: FRD_01003C_Shapefiles - S_FRD_Proj_Ar.gpkg
Appending file 3/1254: FRD_01005C_Shapefiles - S_FRD_Proj_Ar.gpkg
...
Appending file 1254/1254: FRD_56045C_Shapefiles - S_FRD_Proj_Ar.gpkg
Successfully merged 1254 files into E:\FEMA_MERGED\S_FRD_Proj_Ar.gpkg

Processing filename group: S_HUC_Ar
Found 1254 files with filename 'S_HUC_Ar'
Starting with first file: FRD_01001C_Shapefiles - S_HUC_Ar.gpkg
Appending file 2/1254: FRD_01003C_Shapefiles - S_HUC_Ar.gpkg
...
Successfully merged 1254 files into E:\FEMA_MERGED\S_HUC_Ar.gpkg

Processing filename group: S_CSLF_Ar
Found 1123 files with filename 'S_CSLF_Ar'
Starting with first file: FRD_01001C_Shapefiles - S_CSLF_Ar.gpkg
Appending file 2/1123: FRD_01003C_Shapefiles - S_CSLF_Ar.gpkg
...
Successfully merged 1123 files into E:\FEMA_MERGED\S_CSLF_Ar.gpkg

================================================================================
GPKG MERGE REPORT
================================================================================

[MERGE RESULTS]:
  S_FRD_Proj_Ar: SUCCESS - 1254 source files -> S_FRD_Proj_Ar.gpkg
  S_HUC_Ar: SUCCESS - 1254 source files -> S_HUC_Ar.gpkg
  S_CSLF_Ar: SUCCESS - 1123 source files -> S_CSLF_Ar.gpkg

================================================================================
MERGE COMPLETED - 3 successful, 0 failed
================================================================================

Process completed at: 2025-07-21 11:45:30.000000
Summary: 3 successful, 0 failed
```

## Output Structure

```
E:\FEMA_MERGED\
├── S_FRD_Proj_Ar.gpkg           # Merged project area files
├── S_HUC_Ar.gpkg                # Merged HUC area files
├── S_CSLF_Ar.gpkg               # Merged CSLF area files
└── ... (all merged files by filename)
```

## Prerequisites

- Script 06c must be run first to create the clean_conversion_table
- OSGeo4W must be installed (provides ogr2ogr)
- Script must be run from an OSGeo4W console