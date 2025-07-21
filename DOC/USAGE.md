# FEMA Data Collection Usage Guide

## Basic Workflow

1. **Extract States, Counties, and Communities**:
   ```bash
   python notebooks/01_get_all_state.py
   python notebooks/02_get_all_counties.py
   python notebooks/03_get_all_communities.py
   ```

2. **Collect and Download Shapefiles**:
   ```bash
   # Collect metadata
   python notebooks/04_get_flood_risk_shapefiles.py
   
   # Download files (use --limit N for testing)
   python notebooks/05_download_shapefiles.py
   ```

3. **Process Shapefiles (Recommended Four-step Process)**:
   ```bash
   # Step 1: Extract ZIP files
   python notebooks/06a_extract_zip_files.py
   
   # Step 2: Convert to GPKG (requires OSGeo4W)
   python notebooks/06b_convert_shapefiles_to_gpkg.py
   
   # Step 3: Create clean conversion tables
   python notebooks/06c_create_clean_conversion_table.py
   
   # Step 4: Merge GPKG files (requires OSGeo4W)
   python notebooks/06d_merge_gpkg_files.py
   ```

   **Note**: Steps 2 and 4 require ogr2ogr from OSGeo4W. Run from OSGeo4W console.

## Common Command Line Options

### Global Options (All Scripts)
- `--config PATH` - Use custom configuration file (default: config.json)
- `--verbose` - Enable detailed logging
- `-h, --help` - Show help message

### Script-Specific Options

#### Download Shapefiles (05)
- `--limit N` - Download only first N files (for testing)

#### Extract ZIP Files (06a)
- `--states CODES` - Process specific states only (comma-separated, e.g., 01,02,04)
- `--force-rebuild` - Clear logs and rebuild from scratch
- `--dry-run` - Show what would be processed without doing it
- `--no-cleanup` - Skip cleanup of temporary files

#### Convert Shapefiles (06b)
- `--products NAMES` - Process specific products only
- `--use-geopandas` - Use GeoPandas instead of ogr2ogr
- `--max-workers N` - Maximum number of worker threads (default: 4)
- `--encoding ENCODING` - Encoding for shapefile attributes (default: UTF-8)
- `--temp-dir PATH` - Temporary directory for faster conversion

#### Merge GPKG Files (06d)
- `--filenames NAMES` - Process specific filename groups only

## Common Workflows

### Testing Setup
```bash
# Test with limited downloads
python notebooks/05_download_shapefiles.py --limit 10

# Test extraction on specific state
python notebooks/06a_extract_zip_files.py --states 01 --verbose
```

### Handling New Data
```bash
# Force rebuild everything
python notebooks/06a_extract_zip_files.py --force-rebuild
python notebooks/06b_convert_shapefiles_to_gpkg.py --force-rebuild
python notebooks/06c_create_clean_conversion_table.py --force-rebuild
python notebooks/06d_merge_gpkg_files.py --force-rebuild

# Process specific states only
python notebooks/06a_extract_zip_files.py --states 01,02,04,05
```

### Troubleshooting
```bash
# Dry run to see what would be processed
python notebooks/06a_extract_zip_files.py --dry-run --verbose

# Handle encoding issues
python notebooks/06b_convert_shapefiles_to_gpkg.py --encoding latin-1 --verbose
```

## Legacy Option

The legacy script combines extraction and merging in one step:
```bash
python notebooks/legacy/06_extract_and_merge_shapefiles.py
```

**Note**: The four-step process is recommended for better control and error handling.