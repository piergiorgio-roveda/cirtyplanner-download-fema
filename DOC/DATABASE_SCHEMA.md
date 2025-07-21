# Database Schema

The SQLite database (`meta_results/flood_risk_shapefiles.db`) contains:

## Tables
- **`states`**: State codes and names
- **`counties`**: County information linked to states
- **`communities`**: Community information linked to counties
- **`shapefiles`**: Flood risk shapefile metadata
- **`request_log`**: API call tracking and error logging
- **`download_log`**: Download tracking (from script 05)
- **`extraction_log`**: Legacy ZIP extraction tracking (from script 06)
- **`extraction_06a_log`**: ZIP extraction tracking (from script 06a)
- **`shapefile_processing_log`**: Legacy GPKG creation tracking (from script 06)
- **`conversion_06b_log`**: GPKG conversion tracking (from script 06b)
- **`clean_conversion_table`**: Clean table with product_name, gpkg_path, filename (from script 06c)
- **`gpkg_filename_groups`**: Filename groups with counts (from script 06c)
- **`merge_06d_log`**: Merge tracking for filename groups (from script 06d)
- **`shapefile_contributions`**: Legacy individual shapefile contributions (from script 06)

## Key Database Tables for New Workflow
- **`download_log`**: Tracks download status, file paths, and sizes
- **`extraction_06a_log`**: Tracks ZIP extraction with detailed shapefile logging
- **`conversion_06b_log`**: Tracks shapefile to GPKG conversion status
- **`clean_conversion_table`**: Clean table with product_name, gpkg_path, filename
- **`gpkg_filename_groups`**: Filename groups with counts for merging
- **`merge_06d_log`**: Tracks merge status for filename groups

## Key Fields
- `product_ID`: Unique FEMA product identifier
- `product_NAME`: Shapefile product name
- `product_FILE_PATH`: Download path for shapefile
- `product_FILE_SIZE`: File size information
- `product_POSTING_DATE_STRING`: When data was published