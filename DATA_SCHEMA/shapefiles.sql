-- shapefiles definition

CREATE TABLE shapefiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            community_code TEXT NOT NULL,
            county_code TEXT NOT NULL,
            state_code TEXT NOT NULL,
            product_id INTEGER,
            product_type_id TEXT,
            product_subtype_id TEXT,
            product_name TEXT,
            product_description TEXT,
            product_effective_date INTEGER,
            product_issue_date INTEGER,
            product_effective_date_string TEXT,
            product_posting_date INTEGER,
            product_posting_date_string TEXT,
            product_issue_date_string TEXT,
            product_effective_flag TEXT,
            product_file_path TEXT,
            product_file_size TEXT,
            fetch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (community_code) REFERENCES communities (community_code),
            FOREIGN KEY (county_code) REFERENCES counties (county_code),
            FOREIGN KEY (state_code) REFERENCES states (state_code)
        );

CREATE INDEX idx_shapefiles_state ON shapefiles (state_code);
CREATE INDEX idx_shapefiles_county ON shapefiles (county_code);
CREATE INDEX idx_shapefiles_community ON shapefiles (community_code);
CREATE INDEX idx_shapefiles_product_name ON shapefiles (product_name);


-- Sample data:
-- |id|community_code|county_code|state_code|product_id|product_type_id|product_subtype_id|product_name|product_description|product_effective_date|product_issue_date|product_effective_date_string|product_posting_date|product_posting_date_string|product_issue_date_string|product_effective_flag|product_file_path|product_file_size|fetch_timestamp|
-- |--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
-- |1|01001C|01001|01|656951|FLOOD_RISK_PRODUCT|FLOOD_RISK_DB|FRD_03150201_shapefiles_20140221|ShapeFiles|973918800000|1392958800000|NA|1392958800000|02/21/2014|02/21/2014|0|/FRP/FRD_03150201_shapefiles_20140221.zip|248MB|2025-07-20 08:28:36|
-- |2|010314|01001|01|656951|FLOOD_RISK_PRODUCT|FLOOD_RISK_DB|FRD_03150201_shapefiles_20140221|ShapeFiles|973918800000|1392958800000|NA|1392958800000|02/21/2014|02/21/2014|0|/FRP/FRD_03150201_shapefiles_20140221.zip|248MB|2025-07-20 08:28:38|
-- |3|010001|01001|01|656951|FLOOD_RISK_PRODUCT|FLOOD_RISK_DB|FRD_03150201_shapefiles_20140221|ShapeFiles|973918800000|1392958800000|NA|1392958800000|02/21/2014|02/21/2014|0|/FRP/FRD_03150201_shapefiles_20140221.zip|248MB|2025-07-20 08:28:40|
