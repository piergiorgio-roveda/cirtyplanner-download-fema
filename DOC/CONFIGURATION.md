# Configuration

The project uses `config.json` for customizable settings. Copy `config.sample.json` to `config.json` and customize as needed:

```bash
cp config.sample.json config.json
```

```json
{
  "download": {
    "base_path": "E:\\FEMA_DOWNLOAD",
    "rate_limit_seconds": 0.2,
    "chunk_size_bytes": 8192,
    "timeout_seconds": 30
  },
  "processing": {
    "extraction_base_path": "E:\\FEMA_EXTRACTED",
    "merged_output_path": "E:\\FEMA_MERGED",
    "shapefile_to_gpkg_path": "E:\\FEMA_SHAPEFILE_TO_GPKG",
    "temp_directory": "E:\\FEMA_TEMP",
    "temp_conversion_path": "D:\\git\\cityplanner-desktop\\download-fema\\.TMP",
    "temp_merge_path": "D:\\git\\cityplanner-desktop\\download-fema\\.TMP",
    "target_crs": "EPSG:4326",
    "chunk_size_features": 10000,
    "memory_limit_mb": 2048,
    "parallel_processing": true,
    "max_workers": 4,
    "use_geopandas": false,
    "shapefile_encoding": "UTF-8",
    "strict_mode": false
  },
  "validation": {
    "geometry_validation": true,
    "fix_invalid_geometries": true,
    "skip_empty_geometries": true,
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
```

## Configuration Options

### Download Settings
- `download.base_path`: Root directory for downloaded files
- `download.rate_limit_seconds`: Delay between downloads (default: 0.2s)
- `download.chunk_size_bytes`: Download chunk size (default: 8KB)
- `download.timeout_seconds`: Request timeout (default: 30s)

### Processing Settings
- `processing.extraction_base_path`: Directory for extracted shapefiles
- `processing.merged_output_path`: Output directory for merged GPKG files (script 06d)
- `processing.shapefile_to_gpkg_path`: Output directory for converted GPKG files (script 06b)
- `processing.temp_directory`: Temporary directory for general processing
- `processing.temp_conversion_path`: Temporary directory for shapefile conversion (used by `--temp-dir` option in script 06b)
- `processing.temp_merge_path`: Temporary directory for GPKG merging
- `processing.target_crs`: Target coordinate system (default: EPSG:4326)
- `processing.chunk_size_features`: Number of features to process in each chunk
- `processing.memory_limit_mb`: Memory limit for processing (default: 2048MB)
- `processing.parallel_processing`: Enable parallel processing (default: true)
- `processing.max_workers`: Maximum number of worker threads (default: 4)
- `processing.use_geopandas`: Use GeoPandas instead of ogr2ogr (default: false)
- `processing.shapefile_encoding`: Encoding for shapefile attributes (default: UTF-8)
- `processing.strict_mode`: Stop on first error or warning (default: false)

### Validation Settings
- `validation.geometry_validation`: Enable geometry validation
- `validation.fix_invalid_geometries`: Automatically fix invalid geometries
- `validation.skip_empty_geometries`: Skip features with empty geometries
- `validation.coordinate_precision`: Precision for coordinate values

### Database Settings
- `database.path`: SQLite database location

### API Settings
- `api.base_url`: FEMA portal base URL
- `api.user_agent`: HTTP user agent string