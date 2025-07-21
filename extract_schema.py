"""
Script to extract schema information from SQLite database.
"""
import sqlite3
import os

def get_table_schema(db_path):
    """
    Extract schema information from SQLite database.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Dictionary with table names as keys and schema information as values
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} does not exist.")
        return {}
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        schemas = {}
        for table in tables:
            table_name = table[0]
            
            # Get CREATE TABLE statement
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            create_stmt = cursor.fetchone()[0]
            
            # Get indices for this table
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='{table_name}';")
            indices = cursor.fetchall()
            index_stmts = [idx[0] for idx in indices if idx[0] is not None]
            
            # Get sample data (first few rows)
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3;")
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                sample_data = []
                if rows:
                    sample_data.append("-- Sample data:")
                    sample_data.append(f"-- |{'|'.join(columns)}|")
                    sample_data.append(f"-- |{'--|' * len(columns)}")
                    for row in rows:
                        formatted_row = [str(cell) if cell is not None else "NULL" for cell in row]
                        sample_data.append(f"-- |{'|'.join(formatted_row)}|")
            except sqlite3.Error as e:
                sample_data = [f"-- Error getting sample data: {str(e)}"]
            
            schemas[table_name] = {
                'create_stmt': create_stmt,
                'indices': index_stmts,
                'sample_data': sample_data
            }
        
        return schemas
    
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return {}
    
    finally:
        if conn:
            conn.close()

def write_schema_files(schemas, output_dir):
    """
    Write schema information to SQL files.
    
    Args:
        schemas: Dictionary with table names as keys and schema information as values
        output_dir: Directory to write SQL files to
    """
    os.makedirs(output_dir, exist_ok=True)
    
    for table_name, schema_info in schemas.items():
        file_path = os.path.join(output_dir, f"{table_name}.sql")
        
        with open(file_path, 'w') as f:
            f.write(f"-- {table_name} definition\n\n")
            f.write(f"{schema_info['create_stmt']};\n\n")
            
            for index_stmt in schema_info['indices']:
                f.write(f"{index_stmt};\n")
            
            if schema_info['indices']:
                f.write("\n\n")
            
            for data_line in schema_info['sample_data']:
                f.write(f"{data_line}\n")
        
        print(f"Created schema file: {file_path}")

if __name__ == "__main__":
    db_path = "meta_results/flood_risk_shapefiles.db"
    output_dir = "DATA_SCHEMA"
    
    print(f"Extracting schema from {db_path}...")
    schemas = get_table_schema(db_path)
    
    if schemas:
        print(f"Found {len(schemas)} tables.")
        write_schema_files(schemas, output_dir)
        print("Schema extraction complete.")
    else:
        print("No tables found or error occurred.")