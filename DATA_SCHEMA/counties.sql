-- counties definition

CREATE TABLE counties (
            county_code TEXT PRIMARY KEY,
            county_name TEXT NOT NULL,
            state_code TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (state_code) REFERENCES states (state_code)
        );

-- Sample data:
-- |county_code|county_name|state_code|created_at|
-- |--|--|--|--|
-- |01001|AUTAUGA COUNTY|01|2025-07-20 19:23:35|
-- |01003|BALDWIN COUNTY|01|2025-07-20 19:23:35|
-- |01005|BARBOUR COUNTY|01|2025-07-20 19:23:35|
