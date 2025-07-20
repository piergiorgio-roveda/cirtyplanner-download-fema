#!/usr/bin/env python3
"""
Script to fetch flood risk shapefile data for each state/county/community combination.

This script:
1. Loads state/county/community data from sample files
2. Creates SQLite database with proper schema
3. Makes POST requests to FEMA portal for each combination
4. Extracts FLOOD_RISK_DB items where product_DESCRIPTION = "ShapeFiles"
5. Stores the relevant shapefile information in SQLite database
"""

import json
import requests
import time
import sqlite3
from datetime import datetime
import os
from urllib.parse import urlencode

def create_database(db_path):
    """Create SQLite database with proper schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create states table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS states (
            state_code TEXT PRIMARY KEY,
            state_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create counties table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS counties (
            county_code TEXT PRIMARY KEY,
            county_name TEXT NOT NULL,
            state_code TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (state_code) REFERENCES states (state_code)
        )
    ''')
    
    # Create communities table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS communities (
            community_code TEXT PRIMARY KEY,
            community_name TEXT NOT NULL,
            county_code TEXT NOT NULL,
            state_code TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (county_code) REFERENCES counties (county_code),
            FOREIGN KEY (state_code) REFERENCES states (state_code)
        )
    ''')
    
    # Create shapefiles table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS shapefiles (
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
        )
    ''')
    
    # Create request_log table for tracking API calls
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS request_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            community_code TEXT NOT NULL,
            county_code TEXT NOT NULL,
            state_code TEXT NOT NULL,
            success BOOLEAN NOT NULL,
            error_message TEXT,
            shapefiles_found INTEGER DEFAULT 0,
            request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (community_code) REFERENCES communities (community_code)
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_shapefiles_state ON shapefiles (state_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_shapefiles_county ON shapefiles (county_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_shapefiles_community ON shapefiles (community_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_shapefiles_product_name ON shapefiles (product_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_request_log_timestamp ON request_log (request_timestamp)')
    
    conn.commit()
    return conn

def load_data():
    """Load county and community data from meta_results."""
    print("Loading data from meta_results...")
    
    # Load counties data
    with open('meta_results/all_counties_data.json', 'r') as f:
        counties_data = json.load(f)
    
    # Load communities data
    with open('meta_results/all_communities_data.json', 'r') as f:
        communities_data = json.load(f)
    
    return counties_data, communities_data

def populate_base_data(conn, counties_data, communities_data):
    """Populate states, counties, and communities tables."""
    cursor = conn.cursor()
    
    print("Populating base data...")
    
    # Insert states
    for state_code, state_info in communities_data['states'].items():
        cursor.execute('''
            INSERT OR REPLACE INTO states (state_code, state_name)
            VALUES (?, ?)
        ''', (state_code, state_info['state_name']))
    
    # Insert counties and communities
    for state_code, state_info in communities_data['states'].items():
        for county_code, county_info in state_info['counties'].items():
            # Insert county
            cursor.execute('''
                INSERT OR REPLACE INTO counties (county_code, county_name, state_code)
                VALUES (?, ?, ?)
            ''', (county_code, county_info['county_name'], state_code))
            
            # Insert communities
            for community in county_info['communities']:
                cursor.execute('''
                    INSERT OR REPLACE INTO communities (community_code, community_name, county_code, state_code)
                    VALUES (?, ?, ?, ?)
                ''', (community['value'], community['label'], county_code, state_code))
    
    conn.commit()
    print("Base data populated successfully.")

def create_form_data(state_code, county_code, community_code):
    """Create form data for POST request."""
    return {
        'utf8': '✓',
        'affiliate': 'fema',
        'query': '',
        'selstate': state_code,
        'selcounty': county_code,
        'selcommunity': community_code,
        'jurisdictionkey': '',
        'jurisdictionvalue': '',
        'searchedCid': community_code,
        'searchedDateStart': '',
        'searchedDateEnd': '',
        'txtstartdate': '',
        'txtenddate': '',
        'method': 'search'
    }

def fetch_flood_risk_data(conn, state_code, county_code, community_code, community_name):
    """Fetch flood risk data for a specific state/county/community combination."""
    url = 'https://msc.fema.gov/portal/advanceSearch'
    cursor = conn.cursor()
    
    form_data = create_form_data(state_code, county_code, community_code)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    try:
        response = requests.post(url, data=form_data, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract FLOOD_RISK_DB items with ShapeFiles
        shapefiles_found = 0
        if 'FLOOD_RISK_DB' in data:
            for item in data['FLOOD_RISK_DB']:
                if item.get('product_DESCRIPTION') == 'ShapeFiles':
                    # Insert shapefile data
                    cursor.execute('''
                        INSERT INTO shapefiles (
                            community_code, county_code, state_code,
                            product_id, product_type_id, product_subtype_id,
                            product_name, product_description,
                            product_effective_date, product_issue_date,
                            product_effective_date_string, product_posting_date,
                            product_posting_date_string, product_issue_date_string,
                            product_effective_flag, product_file_path, product_file_size
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        community_code, county_code, state_code,
                        item.get('product_ID'), item.get('product_TYPE_ID'), item.get('product_SUBTYPE_ID'),
                        item.get('product_NAME'), item.get('product_DESCRIPTION'),
                        item.get('product_EFFECTIVE_DATE'), item.get('product_ISSUE_DATE'),
                        item.get('product_EFFECTIVE_DATE_STRING'), item.get('product_POSTING_DATE'),
                        item.get('product_POSTING_DATE_STRING'), item.get('product_ISSUE_DATE_STRING'),
                        item.get('product_EFFECTIVE_FLAG'), item.get('product_FILE_PATH'), item.get('product_FILE_SIZE')
                    ))
                    shapefiles_found += 1
        
        # Log successful request
        cursor.execute('''
            INSERT INTO request_log (community_code, county_code, state_code, success, shapefiles_found)
            VALUES (?, ?, ?, ?, ?)
        ''', (community_code, county_code, state_code, True, shapefiles_found))
        
        conn.commit()
        
        return {
            'success': True,
            'shapefiles_found': shapefiles_found
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        print(f"Error fetching data for {community_name} ({community_code}): {error_msg}")
        
        # Log failed request
        cursor.execute('''
            INSERT INTO request_log (community_code, county_code, state_code, success, error_message, shapefiles_found)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (community_code, county_code, state_code, False, error_msg, 0))
        
        conn.commit()
        
        return {
            'success': False,
            'error': error_msg,
            'shapefiles_found': 0
        }
    except json.JSONDecodeError as e:
        error_msg = f"JSON decode error: {str(e)}"
        print(f"Error parsing JSON for {community_name} ({community_code}): {error_msg}")
        
        # Log failed request
        cursor.execute('''
            INSERT INTO request_log (community_code, county_code, state_code, success, error_message, shapefiles_found)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (community_code, county_code, state_code, False, error_msg, 0))
        
        conn.commit()
        
        return {
            'success': False,
            'error': error_msg,
            'shapefiles_found': 0
        }

def get_statistics(conn):
    """Get statistics from the database."""
    cursor = conn.cursor()
    
    # Total counts
    cursor.execute('SELECT COUNT(*) FROM states')
    total_states = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM counties')
    total_counties = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM communities')
    total_communities = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM shapefiles')
    total_shapefiles = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM request_log WHERE success = 1')
    successful_requests = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM request_log WHERE success = 0')
    failed_requests = cursor.fetchone()[0]
    
    # Top states by shapefile count
    cursor.execute('''
        SELECT s.state_name, s.state_code, COUNT(sf.id) as shapefile_count
        FROM states s
        LEFT JOIN shapefiles sf ON s.state_code = sf.state_code
        GROUP BY s.state_code, s.state_name
        ORDER BY shapefile_count DESC
        LIMIT 10
    ''')
    top_states = cursor.fetchall()
    
    # Top counties by shapefile count
    cursor.execute('''
        SELECT c.county_name, c.county_code, s.state_name, COUNT(sf.id) as shapefile_count
        FROM counties c
        JOIN states s ON c.state_code = s.state_code
        LEFT JOIN shapefiles sf ON c.county_code = sf.county_code
        GROUP BY c.county_code, c.county_name, s.state_name
        ORDER BY shapefile_count DESC
        LIMIT 10
    ''')
    top_counties = cursor.fetchall()
    
    # Top communities by shapefile count
    cursor.execute('''
        SELECT cm.community_name, cm.community_code, c.county_name, s.state_name, COUNT(sf.id) as shapefile_count
        FROM communities cm
        JOIN counties c ON cm.county_code = c.county_code
        JOIN states s ON cm.state_code = s.state_code
        LEFT JOIN shapefiles sf ON cm.community_code = sf.community_code
        GROUP BY cm.community_code, cm.community_name, c.county_name, s.state_name
        ORDER BY shapefile_count DESC
        LIMIT 10
    ''')
    top_communities = cursor.fetchall()
    
    return {
        'total_states': total_states,
        'total_counties': total_counties,
        'total_communities': total_communities,
        'total_shapefiles': total_shapefiles,
        'successful_requests': successful_requests,
        'failed_requests': failed_requests,
        'top_states': top_states,
        'top_counties': top_counties,
        'top_communities': top_communities
    }

def main():
    """Main function to process all state/county/community combinations."""
    print("Starting flood risk shapefile data collection...")
    print("=" * 60)
    
    # Create output directory
    os.makedirs('meta_results', exist_ok=True)
    
    # Create database
    db_path = 'meta_results/flood_risk_shapefiles.db'
    conn = create_database(db_path)
    
    # Load data
    counties_data, communities_data = load_data()
    
    # Calculate totals for progress tracking
    total_states = len(communities_data['states'])
    total_counties = sum(len(state_info['counties']) for state_info in communities_data['states'].values())
    total_communities = sum(
        len(county_info['communities'])
        for state_info in communities_data['states'].values()
        for county_info in state_info['counties'].values()
    )
    
    print(f"Dataset Overview:")
    print(f"  Total States: {total_states}")
    print(f"  Total Counties: {total_counties}")
    print(f"  Total Communities: {total_communities}")
    print("=" * 60)
    
    # Populate base data
    populate_base_data(conn, counties_data, communities_data)
    
    total_processed = 0
    total_shapefiles_found = 0
    county_processed = 0
    
    # Process each state
    for state_code, state_info in communities_data['states'].items():
        state_name = state_info['state_name']
        print(f"\nProcessing state: {state_name} ({state_code})")
        
        # Process each county in the state
        for county_code, county_info in state_info['counties'].items():
            county_processed += 1
            county_name = county_info['county_name']
            county_community_count = len(county_info['communities'])
            
            print(f"  Processing county: {county_name} ({county_code}) - County {county_processed}/{total_counties}")
            print(f"    Communities in this county: {county_community_count}")
            
            county_start_processed = total_processed
            
            # Process each community in the county
            for community in county_info['communities']:
                community_code = community['value']
                community_name = community['label']
                
                print(f"    Processing community: {community_name} ({community_code})")
                
                # Fetch flood risk data
                result = fetch_flood_risk_data(conn, state_code, county_code, community_code, community_name)
                
                # Update counters
                total_processed += 1
                total_shapefiles_found += result['shapefiles_found']
                
                # Progress update
                # if total_processed % 5 == 0:
                #     progress_percent = (total_processed / total_communities) * 100
                #     print(f"    Progress: {total_processed}/{total_communities} communities processed ({progress_percent:.1f}%), {total_shapefiles_found} shapefiles found")
                
                # Rate limiting - wait between requests
                time.sleep(0.1)
            
            # County completion summary
            county_communities_processed = total_processed - county_start_processed
            print(f"  ✓ Completed county: {county_name} - {county_communities_processed} communities processed")
    
    # Get final statistics
    stats = get_statistics(conn)
    
    # Generate summary
    print("\n" + "=" * 60)
    print("FLOOD RISK SHAPEFILE DATA COLLECTION COMPLETE")
    print("=" * 60)
    print(f"Total states processed: {stats['total_states']}")
    print(f"Total counties processed: {stats['total_counties']}")
    print(f"Total communities processed: {stats['total_communities']}")
    print(f"Total shapefiles found: {stats['total_shapefiles']}")
    print(f"Successful requests: {stats['successful_requests']}")
    print(f"Failed requests: {stats['failed_requests']}")
    
    print(f"\nDatabase saved to: {db_path}")
    
    # Display top results
    if stats['top_states']:
        print(f"\nTop 5 states by shapefile count:")
        for i, (state_name, state_code, count) in enumerate(stats['top_states'][:5], 1):
            print(f"{i}. {state_name} ({state_code}): {count} shapefiles")
    
    if stats['top_counties']:
        print(f"\nTop 5 counties by shapefile count:")
        for i, (county_name, county_code, state_name, count) in enumerate(stats['top_counties'][:5], 1):
            print(f"{i}. {county_name} ({county_code}), {state_name}: {count} shapefiles")
    
    if stats['top_communities']:
        print(f"\nTop 5 communities by shapefile count:")
        for i, (community_name, community_code, county_name, state_name, count) in enumerate(stats['top_communities'][:5], 1):
            print(f"{i}. {community_name} ({community_code}), {county_name}, {state_name}: {count} shapefiles")
    
    # Close database connection
    conn.close()
    
    print(f"\nYou can now query the database using SQL:")
    print(f"sqlite3 {db_path}")
    print(f"\nExample queries:")
    print(f"  SELECT * FROM shapefiles LIMIT 10;")
    print(f"  SELECT state_name, COUNT(*) FROM shapefiles sf JOIN states s ON sf.state_code = s.state_code GROUP BY state_name;")
    print(f"  SELECT * FROM shapefiles WHERE product_file_size LIKE '%MB' ORDER BY CAST(REPLACE(product_file_size, 'MB', '') AS INTEGER) DESC;")

if __name__ == "__main__":
    main()