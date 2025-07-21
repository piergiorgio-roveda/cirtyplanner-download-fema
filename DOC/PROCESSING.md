# Download and Processing Functionality

## Download Functionality

### Automated Download
The `05_download_shapefiles.py` script automatically downloads all discovered shapefiles:

**Features:**
- **Organized Storage**: `E:\FEMA_DOWNLOAD\{state_code}\{county_code}\`
- **Resume Capability**: Continues interrupted downloads
- **Progress Tracking**: Shows download progress and statistics
- **Error Handling**: Logs failed downloads for retry
- **Duplicate Prevention**: Skips already downloaded files

**Folder Structure:**
```
E:\FEMA_DOWNLOAD\
├── 01\                    # Alabama
│   ├── 01001\            # Autauga County
│   │   ├── FRD_01001C_shapefiles_20140221.zip
│   │   └── ...
│   └── 01003\            # Baldwin County
├── 02\                    # Alaska
└── ...
```

**Download Tracking:**
- Creates `download_log` table in SQLite database
- Tracks success/failure status for each file
- Records file sizes and error messages
- Enables download resume and retry functionality

### Manual Download URLs

Individual shapefiles can be downloaded using:
```
https://msc.fema.gov/portal/downloadProduct?productTypeID=FLOOD_RISK_PRODUCT&productSubTypeID=FLOOD_RISK_DB&productID={product_name}
```

Example:
```
https://msc.fema.gov/portal/downloadProduct?productTypeID=FLOOD_RISK_PRODUCT&productSubTypeID=FLOOD_RISK_DB&productID=FRD_03150201_shapefiles_20140221
```

## Extraction and Processing Functionality

### Two-Step Processing (Recommended)

#### Step 1: ZIP Extraction
The `06a_extract_zip_files.py` script extracts downloaded ZIP files:

**Features:**
- **ZIP Extraction**: Extracts all downloaded ZIP files to organized directories
- **Duplicate Prevention**: Uses SQL query to avoid processing duplicate products
- **Detailed Logging**: Tracks each shapefile found within ZIP files
- **Simplified Structure**: Organizes by product name rather than state/county

**Output Structure:**
```
E:\FEMA_EXTRACTED\
├── FRD_01001C_Shapefiles\        # Product name as folder
│   ├── S_CSLF_Ar.shp
│   ├── S_CSLF_Ar.dbf
│   ├── S_CSLF_Ar.shx
│   ├── S_CSLF_Ar.prj
│   └── ... (all extracted files)
├── FRD_03150201_shapefiles_20140221\
└── ... (all products)
```

**Processing Tracking:**
- Creates `extraction_06a_log` table with detailed shapefile tracking
- Records both relative path within ZIP and shapefile name
- Enables resume capability for interrupted processing

#### Step 2: Shapefile Conversion
The `06b_convert_shapefiles_to_gpkg.py` script converts shapefiles to GeoPackage:

**Features:**
- **ogr2ogr Integration**: Uses ogr2ogr by default for robust conversion
- **Encoding Handling**: Supports different encodings for problematic files
- **Thread-Safe Processing**: Creates thread-local database connections
- **Parallel Processing**: Optional multi-threaded conversion
- **Detailed Logging**: Tracks conversion success/failure
- **Temporary Directory**: Uses fast disk for temporary operations
- **Strict Mode**: Option to stop on first error or warning

**Output Structure:**
```
E:\FEMA_SHAPEFILE_TO_GPKG\
├── FRD_01001C_Shapefiles\        # Product name as folder
│   ├── S_CSLF_Ar.gpkg
│   ├── S_FRD_Proj_Ar.gpkg
│   └── ... (all converted files)
├── FRD_03150201_shapefiles_20140221\
└── ... (all products)
```

**Processing Tracking:**
- Creates `conversion_06b_log` table with conversion tracking
- Records source shapefile and destination GPKG paths
- Enables resume capability for interrupted processing

#### Step 3: Create Clean Conversion Table
The `06c_create_clean_conversion_table.py` script creates clean tables for analysis:

**Features:**
- **Clean Table Creation**: Creates simplified tables for analysis
- **Filename Extraction**: Extracts filename without path and extension
- **Grouping**: Creates table with filename groups and counts
- **Detailed Reporting**: Generates statistics about filename groups

**Output Tables:**
- `clean_conversion_table`: Contains product_name, gpkg_path, and filename
- `gpkg_filename_groups`: Contains filename and count for each unique filename

#### Step 4: Merge GeoPackage Files by Filename
The `06d_merge_gpkg_files.py` script merges GeoPackage files by filename group:

**Features:**
- **Filename-based Merging**: Merges files with the same filename across products
- **ogr2ogr Integration**: Uses ogr2ogr for efficient merging
- **Temporary Directory**: Uses fast disk for temporary operations
- **Detailed Logging**: Tracks merge success/failure
- **Field Name Sanitization**: Converts all field names to lowercase and replaces non-alphanumeric characters with underscores
- **Field Filtering**: Automatically filters out shape length and area fields

**Output Structure:**
```
E:\FEMA_MERGED\
├── s_frd_proj_ar.gpkg           # Merged project area files
├── s_huc_ar.gpkg                # Merged HUC area files
├── s_cslf_ar.gpkg               # Merged CSLF area files
└── ... (all merged files by filename)
```

**Processing Tracking:**
- Creates `merge_06d_log` table with merge tracking
- Records source files count and destination path
- Enables resume capability for interrupted processing

### Legacy Combined Processing
The `06_extract_and_merge_shapefiles.py` script combines extraction and merging (legacy):

**Features:**
- **Combined Workflow**: Extracts and merges in one step
- **State-based Organization**: Organizes by state rather than product
- **Shapefile Merging**: Combines shapefiles by type within each state

**Output Structure:**
```
E:\FEMA_MERGED\
├── 01\                           # Alabama (legacy organization)
│   ├── R_UDF_Losses_by_Building.gpkg
│   ├── R_UDF_Losses_by_Parcel.gpkg
│   ├── S_CSLF_Ar.gpkg
│   └── ... (all available shapefile types)
├── 02\                           # Alaska
└── ... (all 57 states/territories)
```

**Note:** The legacy script is maintained for backward compatibility, but the two-step process is recommended for better control and error handling.