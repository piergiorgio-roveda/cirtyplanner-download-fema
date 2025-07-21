-- clean_conversion_table definition

CREATE TABLE clean_conversion_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            gpkg_path TEXT NOT NULL,
            filename TEXT NOT NULL
        );

CREATE INDEX idx_clean_product ON clean_conversion_table (product_name);
CREATE INDEX idx_clean_filename ON clean_conversion_table (filename);


-- Sample data:
-- |id|product_name|gpkg_path|filename|
-- |--|--|--|--|
-- |4740|03140305_FRD_Shapefiles_20240607|E:\FEMA_SHAPEFILE_TO_GPKG\03140305_FRD_Shapefiles_20240607\S_FRD_Proj_Ar.gpkg|s_frd_proj_ar|
-- |4741|03140305_FRD_Shapefiles_20240607|E:\FEMA_SHAPEFILE_TO_GPKG\03140305_FRD_Shapefiles_20240607\S_HUC_Ar.gpkg|s_huc_ar|
-- |4742|03160105_FRD_Shapefiles_20240513|E:\FEMA_SHAPEFILE_TO_GPKG\03160105_FRD_Shapefiles_20240513\S_FRD_Prj_Ar.gpkg|s_frd_prj_ar|
