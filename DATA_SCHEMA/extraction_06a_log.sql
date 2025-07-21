-- extraction_06a_log definition

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

CREATE INDEX idx_extraction_06a_product ON extraction_06a_log (product_name);
CREATE INDEX idx_extraction_06a_success ON extraction_06a_log (extraction_success);


-- Sample data:
-- |id|product_name|zip_file_path|extracted_path|shapefile_name|extraction_success|extraction_timestamp|error_message|
-- |--|--|--|--|--|--|--|--|
-- |5561|03140305_FRD_Shapefiles_20240607|E:\FEMA_DOWNLOAD\01\01035\03140305_FRD_Shapefiles_20240607.zip|03140305_FRD_Shapefiles_20240607/S_CSLF_Ar.shp|S_CSLF_Ar.shp|1|2025-07-21 07:47:09|NULL|
-- |5562|03140305_FRD_Shapefiles_20240607|E:\FEMA_DOWNLOAD\01\01035\03140305_FRD_Shapefiles_20240607.zip|03140305_FRD_Shapefiles_20240607/S_FRD_Proj_Ar.shp|S_FRD_Proj_Ar.shp|1|2025-07-21 07:47:09|NULL|
-- |5563|03140305_FRD_Shapefiles_20240607|E:\FEMA_DOWNLOAD\01\01035\03140305_FRD_Shapefiles_20240607.zip|03140305_FRD_Shapefiles_20240607/S_HUC_Ar.shp|S_HUC_Ar.shp|1|2025-07-21 07:47:09|NULL|
