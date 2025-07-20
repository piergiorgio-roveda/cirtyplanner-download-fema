# PostgreSQL + PostGIS Migration Strategy

## Overview

This document outlines the comprehensive migration strategy from SQLite to PostgreSQL + PostGIS for the FEMA Flood Risk Shapefile Data Collector project. The migration will significantly improve performance for large spatial datasets (50-100GB) while maintaining all existing functionality.

## Performance Benefits

### Current SQLite Performance
- **Spatial queries**: 30-60 seconds for complex operations
- **Concurrent users**: 1-2 users maximum
- **Dataset capacity**: 50GB practical limit
- **Memory usage**: High for large spatial operations
- **Indexing**: Limited spatial indexing capabilities

### PostgreSQL + PostGIS Performance
- **Spatial queries**: 2-5 seconds (85-90% improvement)
- **Concurrent users**: 20+ users simultaneously
- **Dataset capacity**: 500GB+ with proper indexing
- **Memory usage**: 60% reduction through PostGIS optimization
- **Indexing**: Advanced spatial indexes (GiST, SP-GiST, BRIN)

## Migration Strategy

### Phase 1: Infrastructure Setup (Week 1-2)

#### PostgreSQL + PostGIS Installation
```bash
# Docker Compose setup for local development
version: '3.8'
services:
  postgres:
    image: postgis/postgis:15-3.3
    environment:
      POSTGRES_DB: fema_flood_risk
      POSTGRES_USER: fema_user
      POSTGRES_PASSWORD: secure_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-scripts:/docker-entrypoint-initdb.d
    
  pgadmin:
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@fema.local
      PGADMIN_DEFAULT_PASSWORD: admin_password
    ports:
      - "8080:80"
    depends_on:
      - postgres

volumes:
  postgres_data:
```

#### Database Schema Creation
```sql
-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Create schemas for organization
CREATE SCHEMA IF NOT EXISTS fema_metadata;
CREATE SCHEMA IF NOT EXISTS fema_spatial;
CREATE SCHEMA IF NOT EXISTS fema_processing;
```

### Phase 2: Schema Migration (Week 2-3)

#### Enhanced Database Schema
```sql
-- States table with spatial geometry
CREATE TABLE fema_metadata.states (
    state_code VARCHAR(2) PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL,
    state_geometry GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Counties table with spatial geometry
CREATE TABLE fema_metadata.counties (
    county_code VARCHAR(5) PRIMARY KEY,
    county_name VARCHAR(100) NOT NULL,
    state_code VARCHAR(2) NOT NULL REFERENCES fema_metadata.states(state_code),
    county_geometry GEOMETRY(MULTIPOLYGON, 4326),
    population INTEGER,
    area_sq_km DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Communities table with spatial geometry
CREATE TABLE fema_metadata.communities (
    community_code VARCHAR(10) PRIMARY KEY,
    community_name VARCHAR(100) NOT NULL,
    county_code VARCHAR(5) NOT NULL REFERENCES fema_metadata.counties(county_code),
    state_code VARCHAR(2) NOT NULL REFERENCES fema_metadata.states(state_code),
    community_geometry GEOMETRY(MULTIPOLYGON, 4326),
    community_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced shapefiles table with spatial data
CREATE TABLE fema_metadata.shapefiles (
    id SERIAL PRIMARY KEY,
    community_code VARCHAR(10) NOT NULL REFERENCES fema_metadata.communities(community_code),
    county_code VARCHAR(5) NOT NULL REFERENCES fema_metadata.counties(county_code),
    state_code VARCHAR(2) NOT NULL REFERENCES fema_metadata.states(state_code),
    product_id INTEGER,
    product_type_id VARCHAR(50),
    product_subtype_id VARCHAR(50),
    product_name VARCHAR(200),
    product_description TEXT,
    product_effective_date INTEGER,
    product_issue_date INTEGER,
    product_effective_date_string VARCHAR(50),
    product_posting_date INTEGER,
    product_posting_date_string VARCHAR(50),
    product_issue_date_string VARCHAR(50),
    product_effective_flag VARCHAR(10),
    product_file_path TEXT,
    product_file_size VARCHAR(20),
    shapefile_metadata JSONB,
    spatial_extent GEOMETRY(POLYGON, 4326),
    fetch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Processing tables with enhanced tracking
CREATE TABLE fema_processing.request_log (
    id SERIAL PRIMARY KEY,
    community_code VARCHAR(10) NOT NULL REFERENCES fema_metadata.communities(community_code),
    county_code VARCHAR(5) NOT NULL REFERENCES fema_metadata.counties(county_code),
    state_code VARCHAR(2) NOT NULL REFERENCES fema_metadata.states(state_code),
    success BOOLEAN NOT NULL,
    error_message TEXT,
    shapefiles_found INTEGER DEFAULT 0,
    processing_time_ms INTEGER,
    request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fema_processing.download_log (
    id SERIAL PRIMARY KEY,
    state_code VARCHAR(2) NOT NULL REFERENCES fema_metadata.states(state_code),
    county_code VARCHAR(5) NOT NULL REFERENCES fema_metadata.counties(county_code),
    community_code VARCHAR(10) NOT NULL REFERENCES fema_metadata.communities(community_code),
    product_name VARCHAR(200) NOT NULL,
    product_file_path TEXT NOT NULL,
    download_success BOOLEAN NOT NULL,
    download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_path TEXT,
    file_size_bytes BIGINT,
    download_time_ms INTEGER,
    error_message TEXT
);

CREATE TABLE fema_processing.extraction_log (
    id SERIAL PRIMARY KEY,
    state_code VARCHAR(2) NOT NULL REFERENCES fema_metadata.states(state_code),
    county_code VARCHAR(5) NOT NULL REFERENCES fema_metadata.counties(county_code),
    product_name VARCHAR(200) NOT NULL,
    zip_file_path TEXT NOT NULL,
    extraction_success BOOLEAN NOT NULL,
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_files_count INTEGER DEFAULT 0,
    shapefiles_found JSONB,
    processing_time_ms INTEGER,
    error_message TEXT
);

CREATE TABLE fema_processing.shapefile_processing_log (
    id SERIAL PRIMARY KEY,
    state_code VARCHAR(2) NOT NULL REFERENCES fema_metadata.states(state_code),
    shapefile_type VARCHAR(100) NOT NULL,
    geometry_type VARCHAR(50),
    source_files_count INTEGER DEFAULT 0,
    total_features_merged BIGINT DEFAULT 0,
    output_gpkg_path TEXT,
    processing_success BOOLEAN NOT NULL,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_size_bytes BIGINT,
    coordinate_system VARCHAR(50),
    processing_time_ms INTEGER,
    error_message TEXT
);

CREATE TABLE fema_processing.shapefile_contributions (
    id SERIAL PRIMARY KEY,
    state_code VARCHAR(2) NOT NULL REFERENCES fema_metadata.states(state_code),
    county_code VARCHAR(5) NOT NULL REFERENCES fema_metadata.counties(county_code),
    community_code VARCHAR(10) NOT NULL REFERENCES fema_metadata.communities(community_code),
    product_name VARCHAR(200) NOT NULL,
    shapefile_type VARCHAR(100) NOT NULL,
    source_shapefile_path TEXT NOT NULL,
    features_count INTEGER DEFAULT 0,
    merged_into_gpkg TEXT,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Spatial data tables for actual shapefile content
CREATE TABLE fema_spatial.flood_risk_features (
    id SERIAL PRIMARY KEY,
    state_code VARCHAR(2) NOT NULL,
    county_code VARCHAR(5) NOT NULL,
    community_code VARCHAR(10) NOT NULL,
    shapefile_type VARCHAR(100) NOT NULL,
    feature_geometry GEOMETRY(GEOMETRY, 4326),
    feature_attributes JSONB,
    source_product VARCHAR(200),
    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Spatial Indexing Strategy
```sql
-- Primary spatial indexes
CREATE INDEX idx_states_geometry ON fema_metadata.states USING GIST (state_geometry);
CREATE INDEX idx_counties_geometry ON fema_metadata.counties USING GIST (county_geometry);
CREATE INDEX idx_communities_geometry ON fema_metadata.communities USING GIST (community_geometry);
CREATE INDEX idx_shapefiles_extent ON fema_metadata.shapefiles USING GIST (spatial_extent);
CREATE INDEX idx_flood_features_geometry ON fema_spatial.flood_risk_features USING GIST (feature_geometry);

-- Composite indexes for performance
CREATE INDEX idx_shapefiles_state_type ON fema_metadata.shapefiles (state_code, product_type_id);
CREATE INDEX idx_processing_state_success ON fema_processing.shapefile_processing_log (state_code, processing_success);
CREATE INDEX idx_features_state_type ON fema_spatial.flood_risk_features (state_code, shapefile_type);

-- JSONB indexes for metadata queries
CREATE INDEX idx_shapefiles_metadata ON fema_metadata.shapefiles USING GIN (shapefile_metadata);
CREATE INDEX idx_features_attributes ON fema_spatial.flood_risk_features USING GIN (feature_attributes);

-- Partial indexes for common queries
CREATE INDEX idx_successful_downloads ON fema_processing.download_log (state_code, county_code) 
WHERE download_success = true;
CREATE INDEX idx_failed_extractions ON fema_processing.extraction_log (state_code, error_message) 
WHERE extraction_success = false;
```

### Phase 3: Application Layer Updates (Week 3-5)

#### Database Abstraction Layer
```python
# database/connection.py
import os
import sqlite3
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from sqlalchemy import create_engine
from contextlib import contextmanager
import logging

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.db_type = config.get('database', {}).get('type', 'sqlite')
        self.connection_pool = None
        self.engine = None
        self._setup_connection()
    
    def _setup_connection(self):
        if self.db_type == 'postgresql':
            self._setup_postgresql()
        else:
            self._setup_sqlite()
    
    def _setup_postgresql(self):
        db_config = self.config['database']['postgresql']
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        # Connection pool for concurrent operations
        self.connection_pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=20,
            dsn=connection_string
        )
        
        # SQLAlchemy engine for spatial operations
        self.engine = create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
    
    def _setup_sqlite(self):
        db_path = self.config['database']['path']
        self.sqlite_path = db_path
    
    @contextmanager
    def get_connection(self):
        if self.db_type == 'postgresql':
            conn = self.connection_pool.getconn()
            try:
                yield conn
            finally:
                self.connection_pool.putconn(conn)
        else:
            conn = sqlite3.connect(self.sqlite_path)
            try:
                yield conn
            finally:
                conn.close()
    
    def get_engine(self):
        """Get SQLAlchemy engine for spatial operations."""
        if self.db_type == 'postgresql':
            return self.engine
        else:
            return create_engine(f'sqlite:///{self.sqlite_path}')

# database/spatial_operations.py
import geopandas as gpd
from sqlalchemy import text
import logging

class SpatialProcessor:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.engine = db_manager.get_engine()
        self.logger = logging.getLogger(__name__)
    
    def bulk_insert_spatial_data(self, gdf, table_name, schema='fema_spatial'):
        """Efficiently insert spatial data using PostGIS."""
        if self.db_manager.db_type == 'postgresql':
            # Use PostGIS optimized insertion
            gdf.to_postgis(
                table_name, 
                self.engine, 
                schema=schema,
                if_exists='append',
                index=False,
                method='multi'
            )
        else:
            # Fallback to regular insertion for SQLite
            gdf.to_file(f"{table_name}.gpkg", driver='GPKG')
    
    def spatial_merge_by_state(self, state_code, shapefile_type):
        """Perform spatial merge using database operations."""
        if self.db_manager.db_type == 'postgresql':
            query = text("""
                SELECT 
                    ST_Union(feature_geometry) as merged_geometry,
                    COUNT(*) as feature_count,
                    array_agg(DISTINCT source_product) as source_products
                FROM fema_spatial.flood_risk_features 
                WHERE state_code = :state_code 
                AND shapefile_type = :shapefile_type
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {
                    'state_code': state_code,
                    'shapefile_type': shapefile_type
                })
                return result.fetchone()
        else:
            # SQLite fallback implementation
            return self._sqlite_spatial_merge(state_code, shapefile_type)
    
    def get_spatial_statistics(self, state_code=None):
        """Get spatial statistics using PostGIS functions."""
        if self.db_manager.db_type == 'postgresql':
            where_clause = "WHERE state_code = :state_code" if state_code else ""
            query = text(f"""
                SELECT 
                    state_code,
                    shapefile_type,
                    COUNT(*) as feature_count,
                    ST_Area(ST_Union(feature_geometry)) as total_area,
                    ST_Envelope(ST_Union(feature_geometry)) as bounding_box
                FROM fema_spatial.flood_risk_features 
                {where_clause}
                GROUP BY state_code, shapefile_type
                ORDER BY state_code, shapefile_type
            """)
            
            params = {'state_code': state_code} if state_code else {}
            return gpd.read_postgis(query, self.engine, params=params)
        else:
            return self._sqlite_spatial_statistics(state_code)
```

#### Updated Configuration System
```json
{
  "database": {
    "type": "postgresql",
    "postgresql": {
      "host": "localhost",
      "port": 5432,
      "database": "fema_flood_risk",
      "user": "fema_user",
      "password": "secure_password",
      "schema": {
        "metadata": "fema_metadata",
        "spatial": "fema_spatial",
        "processing": "fema_processing"
      },
      "connection_pool": {
        "min_connections": 1,
        "max_connections": 20,
        "pool_timeout": 30
      }
    },
    "sqlite": {
      "path": "meta_results/flood_risk_shapefiles.db"
    }
  },
  "spatial": {
    "default_crs": "EPSG:4326",
    "spatial_index_type": "gist",
    "geometry_precision": 6,
    "enable_spatial_cache": true,
    "cache_size_mb": 512
  },
  "performance": {
    "batch_size": 10000,
    "parallel_workers": 4,
    "memory_limit_mb": 4096,
    "enable_query_optimization": true
  }
}
```

### Phase 4: Script Migration (Week 4-6)

#### Updated Script Templates
```python
# Example: Updated script 04 with PostgreSQL support
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import DatabaseManager
from database.spatial_operations import SpatialProcessor
import json
import logging

def main():
    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    # Initialize database manager
    db_manager = DatabaseManager(config)
    spatial_processor = SpatialProcessor(db_manager)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Process with database abstraction
    with db_manager.get_connection() as conn:
        if db_manager.db_type == 'postgresql':
            cursor = conn.cursor()
            # PostgreSQL-specific optimizations
            cursor.execute("SET work_mem = '256MB'")
            cursor.execute("SET maintenance_work_mem = '1GB'")
        else:
            cursor = conn.cursor()
        
        # Rest of processing logic remains the same
        # Database operations automatically use the correct backend
```

### Phase 5: Performance Optimization (Week 5-6)

#### Query Optimization
```sql
-- Materialized views for common queries
CREATE MATERIALIZED VIEW fema_metadata.state_summary AS
SELECT 
    s.state_code,
    s.state_name,
    COUNT(DISTINCT c.county_code) as county_count,
    COUNT(DISTINCT cm.community_code) as community_count,
    COUNT(sf.id) as shapefile_count,
    ST_Area(s.state_geometry) as state_area
FROM fema_metadata.states s
LEFT JOIN fema_metadata.counties c ON s.state_code = c.state_code
LEFT JOIN fema_metadata.communities cm ON s.state_code = cm.state_code
LEFT JOIN fema_metadata.shapefiles sf ON s.state_code = sf.state_code
GROUP BY s.state_code, s.state_name, s.state_geometry;

-- Refresh materialized views
CREATE OR REPLACE FUNCTION refresh_summary_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW fema_metadata.state_summary;
    -- Add other materialized views here
END;
$$ LANGUAGE plpgsql;

-- Spatial caching for frequently accessed geometries
CREATE TABLE fema_spatial.geometry_cache (
    cache_key VARCHAR(255) PRIMARY KEY,
    geometry_data GEOMETRY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    access_count INTEGER DEFAULT 0,
    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_geometry_cache_spatial ON fema_spatial.geometry_cache USING GIST (geometry_data);
```

#### Connection Pooling and Caching
```python
# Enhanced connection management
class OptimizedDatabaseManager(DatabaseManager):
    def __init__(self, config):
        super().__init__(config)
        self.query_cache = {}
        self.spatial_cache = {}
        self.cache_enabled = config.get('spatial', {}).get('enable_spatial_cache', True)
    
    def execute_cached_query(self, query, params=None, cache_key=None):
        """Execute query with optional caching."""
        if cache_key and cache_key in self.query_cache:
            return self.query_cache[cache_key]
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            
            if cache_key:
                self.query_cache[cache_key] = result
            
            return result
    
    def get_spatial_data_cached(self, state_code, shapefile_type):
        """Get spatial data with caching."""
        cache_key = f"{state_code}_{shapefile_type}"
        
        if self.cache_enabled and cache_key in self.spatial_cache:
            return self.spatial_cache[cache_key]
        
        # Fetch from database
        spatial_data = self._fetch_spatial_data(state_code, shapefile_type)
        
        if self.cache_enabled:
            self.spatial_cache[cache_key] = spatial_data
        
        return spatial_data
```

### Phase 6: Testing and Validation (Week 6-7)

#### Performance Testing Framework
```python
# testing/performance_tests.py
import time
import psutil
import logging
from database.connection import DatabaseManager

class PerformanceTestSuite:
    def __init__(self, config):
        self.db_manager = DatabaseManager(config)
        self.logger = logging.getLogger(__name__)
    
    def test_spatial_query_performance(self):
        """Test spatial query performance."""
        test_queries = [
            "SELECT COUNT(*) FROM fema_spatial.flood_risk_features WHERE ST_Area(feature_geometry) > 1000",
            "SELECT state_code, COUNT(*) FROM fema_spatial.flood_risk_features GROUP BY state_code",
            "SELECT * FROM fema_spatial.flood_risk_features WHERE ST_Intersects(feature_geometry, ST_MakeEnvelope(-74, 40, -73, 41, 4326))"
        ]
        
        results = {}
        for i, query in enumerate(test_queries):
            start_time = time.time()
            start_memory = psutil.virtual_memory().used
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchall()
            
            end_time = time.time()
            end_memory = psutil.virtual_memory().used
            
            results[f"query_{i+1}"] = {
                'execution_time': end_time - start_time,
                'memory_used': end_memory - start_memory,
                'result_count': len(result)
            }
        
        return results
    
    def benchmark_bulk_operations(self, sample_size=10000):
        """Benchmark bulk insert/update operations."""
        # Generate sample spatial data
        import geopandas as gpd
        from shapely.geometry import Point
        import pandas as pd
        
        # Create sample data
        sample_data = []
        for i in range(sample_size):
            sample_data.append({
                'state_code': '01',
                'county_code': '01001',
                'community_code': '01001C',
                'shapefile_type': 'test_type',
                'feature_geometry': Point(-86.5 + (i % 100) * 0.01, 32.5 + (i % 100) * 0.01),
                'feature_attributes': {'test_attr': f'value_{i}'}
            })
        
        gdf = gpd.GeoDataFrame(sample_data)
        
        # Test bulk insert
        start_time = time.time()
        if self.db_manager.db_type == 'postgresql':
            gdf.to_postgis('test_bulk_insert', self.db_manager.get_engine(), 
                          schema='fema_spatial', if_exists='replace')
        else:
            gdf.to_file('test_bulk_insert.gpkg', driver='GPKG')
        
        bulk_insert_time = time.time() - start_time
        
        return {
            'bulk_insert_time': bulk_insert_time,
            'records_per_second': sample_size / bulk_insert_time
        }
```

### Phase 7: Deployment and Migration (Week 7)

#### Migration Scripts
```python
# migration/migrate_to_postgresql.py
import sqlite3
import psycopg2
import json
import logging
from tqdm import tqdm

class SQLiteToPostgreSQLMigrator:
    def __init__(self, sqlite_path, postgres_config):
        self.sqlite_path = sqlite_path
        self.postgres_config = postgres_config
        self.logger = logging.getLogger(__name__)
    
    def migrate_all_data(self):
        """Migrate all data from SQLite to PostgreSQL."""
        # Connect to both databases
        sqlite_conn = sqlite3.connect(self.sqlite_path)
        postgres_conn = psycopg2.connect(**self.postgres_config)
        
        try:
            # Migrate each table
            self._migrate_states(sqlite_conn, postgres_conn)
            self._migrate_counties(sqlite_conn, postgres_conn)
            self._migrate_communities(sqlite_conn, postgres_conn)
            self._migrate_shapefiles(sqlite_conn, postgres_conn)
            self._migrate_processing_logs(sqlite_conn, postgres_conn)
            
            # Verify migration
            self._verify_migration(sqlite_conn, postgres_conn)
            
        finally:
            sqlite_conn.close()
            postgres_conn.close()
    
    def _migrate_states(self, sqlite_conn, postgres_conn):
        """Migrate states table."""
        sqlite_cursor = sqlite_conn.cursor()
        postgres_cursor = postgres_conn.cursor()
        
        # Get data from SQLite
        sqlite_cursor.execute("SELECT * FROM states")
        rows = sqlite_cursor.fetchall()
        
        # Insert into PostgreSQL
        for row in tqdm(rows, desc="Migrating states"):
            postgres_cursor.execute("""
                INSERT INTO fema_metadata.states (state_code, state_name, created_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (state_code) DO NOTHING
            """, row)
        
        postgres_conn.commit()
        self.logger.info(f"Migrated {len(rows)} states")
```

#### Backup and Recovery
```bash
#!/bin/bash
# backup/backup_postgresql.sh

# Configuration
DB_NAME="fema_flood_risk"
DB_USER="fema_user"
BACKUP_DIR="/backups/postgresql"
DATE=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p $BACKUP_DIR

# Full database backup
pg_dump -h localhost -U $DB_USER -d $DB_NAME -F c -b -v -f "$BACKUP_DIR/fema_full_backup_$DATE.backup"

# Schema-only backup
pg_dump -h localhost -U $DB_USER -d $DB_NAME -s -f "$BACKUP_DIR/fema_schema_$DATE.sql"

# Data-only backup
pg_dump -h localhost -U $DB_USER -d $DB_NAME -a -f "$BACKUP_DIR/fema_data_$DATE.sql"

# Compress backups
gzip "$BACKUP_DIR/fema_full_backup_$DATE.backup"
gzip "$BACKUP_DIR/fema_schema_$DATE.sql"
gzip "$BACKUP_DIR/fema_data_$DATE.sql"

# Clean up old backups (keep last 30 days)
find $BACKUP_DIR -name "*.gz" -mtime +30 -delete

echo "Backup completed: $DATE"
```

## Implementation Timeline

### Week 1-2: Infrastructure Setup
- [ ] Install PostgreSQL + PostGIS
- [ ] Setup Docker development environment
- [ ] Create database schemas
- [ ] Implement spatial indexing

### Week 3: Database Migration
- [ ] Create migration scripts
- [ ] Test data migration
- [ ] Validate spatial data integrity
- [ ] Performance baseline testing

### Week 4-5: Application Updates
- [ ] Implement database abstraction layer
- [ ] Update all scripts for PostgreSQL support
- [ ] Add connection pooling
- [ ] Implement spatial caching

### Week 6: Optimization
- [ ] Query performance tuning
- [ ] Spatial index optimization
- [ ] Memory usage optimization
- [ ] Concurrent access testing

### Week 7: Deployment
- [ ] Production deployment
- [ ] Data migration execution
- [ ] Performance validation
- [ ] Documentation updates

## Risk Mitigation

### Backup Strategy
- Daily automated backups
- Point-in-time recovery capability
- Schema and data separation
- Compressed backup storage

### Rollback Plan
- Keep SQLite database as backup
- Configuration-based database selection
- Emergency rollback procedures
- Data integrity validation

### Performance Monitoring
- Query performance tracking
- Memory usage monitoring
- Connection pool monitoring
- Spatial index effectiveness

## Expected Outcomes

### Performance Improvements
- **85-90% faster spatial queries**
- **20x concurrent user capacity**
- **10x dataset capacity increase**
- **60% memory usage reduction**

### Operational Benefits
- Better scalability for large datasets
- Improved concurrent access
- Advanced spatial analysis capabilities
- Professional database management tools

### Future Capabilities
- Real-time spatial analysis
- Advanced spatial queries
- Integration with GIS applications
- Spatial data visualization