-- request_log definition

CREATE TABLE request_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            community_code TEXT NOT NULL,
            county_code TEXT NOT NULL,
            state_code TEXT NOT NULL,
            success BOOLEAN NOT NULL,
            error_message TEXT,
            shapefiles_found INTEGER DEFAULT 0,
            request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (community_code) REFERENCES communities (community_code)
        );

CREATE INDEX idx_request_log_timestamp ON request_log (request_timestamp);


-- Sample data:
-- |id|community_code|county_code|state_code|success|error_message|shapefiles_found|request_timestamp|
-- |--|--|--|--|--|--|--|--|
-- |1|01001C|01001|01|1|NULL|1|2025-07-20 08:28:36|
-- |2|010314|01001|01|1|NULL|1|2025-07-20 08:28:38|
-- |3|010001|01001|01|1|NULL|1|2025-07-20 08:28:40|
