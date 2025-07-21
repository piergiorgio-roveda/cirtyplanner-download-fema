-- communities definition

CREATE TABLE communities (
            community_code TEXT PRIMARY KEY,
            community_name TEXT NOT NULL,
            county_code TEXT NOT NULL,
            state_code TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (county_code) REFERENCES counties (county_code),
            FOREIGN KEY (state_code) REFERENCES states (state_code)
        );

-- Sample data:
-- |community_code|community_name|county_code|state_code|created_at|
-- |--|--|--|--|--|
-- |01001C|AUTAUGA COUNTY ALL JURISDICTIONS|01001|01|2025-07-20 19:23:35|
-- |010314|AUTAUGA COUNTY UNINCORPORATED AREAS|01001|01|2025-07-20 19:23:35|
-- |010001|AUTAUGAVILLE, TOWN OF|01001|01|2025-07-20 19:23:35|
