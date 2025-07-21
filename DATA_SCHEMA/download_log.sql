-- download_log definition

CREATE TABLE download_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_code TEXT NOT NULL,
            county_code TEXT NOT NULL,
            community_code TEXT NOT NULL,
            product_name TEXT NOT NULL,
            product_file_path TEXT NOT NULL,
            download_success BOOLEAN NOT NULL,
            download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_path TEXT,
            file_size_bytes INTEGER,
            error_message TEXT
        );

CREATE INDEX idx_download_log_product ON download_log (product_name);
CREATE INDEX idx_download_log_success ON download_log (download_success);


-- Sample data:
-- |id|state_code|county_code|community_code|product_name|product_file_path|download_success|download_timestamp|file_path|file_size_bytes|error_message|
-- |--|--|--|--|--|--|--|--|--|--|--|
-- |1|01|01001|010001|FRD_03150201_shapefiles_20140221|/FRP/FRD_03150201_shapefiles_20140221.zip|1|2025-07-20 17:10:24|E:\FEMA_DOWNLOAD\01\01001\FRD_03150201_shapefiles_20140221.zip|260320556|NULL|
-- |2|01|01001|010002|FRD_03150201_shapefiles_20140221|/FRP/FRD_03150201_shapefiles_20140221.zip|1|2025-07-20 17:10:25|E:\FEMA_DOWNLOAD\01\01001\FRD_03150201_shapefiles_20140221.zip|260320556|NULL|
-- |3|01|01001|01001C|FRD_03150201_shapefiles_20140221|/FRP/FRD_03150201_shapefiles_20140221.zip|1|2025-07-20 17:10:25|E:\FEMA_DOWNLOAD\01\01001\FRD_03150201_shapefiles_20140221.zip|260320556|NULL|
