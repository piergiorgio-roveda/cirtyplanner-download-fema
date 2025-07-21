-- conversion_06b_log definition

CREATE TABLE conversion_06b_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            shapefile_path TEXT NOT NULL,
            gpkg_path TEXT NOT NULL,
            conversion_success BOOLEAN NOT NULL,
            conversion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        );

CREATE INDEX idx_conversion_06b_product ON conversion_06b_log (product_name);
CREATE INDEX idx_conversion_06b_success ON conversion_06b_log (conversion_success);


-- |id|product_name|shapefile_path|gpkg_path|
-- |--|------------|--------------|---------|
-- |322|03140305_FRD_Shapefiles_20240607|E:\FEMA_EXTRACTED\03140305_FRD_Shapefiles_20240607\03140305_FRD_Shapefiles_20240607/S_FRD_Proj_Ar.shp|E:\FEMA_SHAPEFILE_TO_GPKG\03140305_FRD_Shapefiles_20240607\S_FRD_Proj_Ar.gpkg|
-- |323|03140305_FRD_Shapefiles_20240607|E:\FEMA_EXTRACTED\03140305_FRD_Shapefiles_20240607\03140305_FRD_Shapefiles_20240607/S_HUC_Ar.shp|E:\FEMA_SHAPEFILE_TO_GPKG\03140305_FRD_Shapefiles_20240607\S_HUC_Ar.gpkg|
-- |324|03160105_FRD_Shapefiles_20240513|E:\FEMA_EXTRACTED\03160105_FRD_Shapefiles_20240513\03160105_FRD_Shapefiles_20240513/S_FRD_Prj_Ar.shp|E:\FEMA_SHAPEFILE_TO_GPKG\03160105_FRD_Shapefiles_20240513\S_FRD_Prj_Ar.gpkg|
