
import os
import json
import mysql.connector
import logging
from db_config import DB_CONFIG

SCHEMA_FILE = "mysql_schema.json"
logging.basicConfig(level=logging.INFO)

def load_schema():
    """Loads the schema JSON file if it exists."""
    if os.path.exists(SCHEMA_FILE):
        try:
            with open(SCHEMA_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.error("  Schema file is corrupted! Rebuilding...")
            return {}
    return {}

def save_schema(schema):
    """Saves the updated schema dictionary to JSON."""
    with open(SCHEMA_FILE, "w") as f:
        json.dump(schema, f, indent=4)
        
def get_table_columns(table_name):
    schema = load_schema()
    return schema.get(table_name, {})


def store_all_table_structures(force_update=False):
    """Fetches table structures including Primary Keys & Foreign Keys, stores in JSON."""
    if os.path.exists(SCHEMA_FILE) and not force_update:
        return  

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]
        schema_data = {}

        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            describe_results = cursor.fetchall()  #   Fetch all results before reusing cursor
            
            table_structure = {}

            #   Fetch Primary Keys
            cursor.execute(f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'")
            primary_keys = {row[4] for row in cursor.fetchall()}  

            #   Fetch Foreign Keys
            cursor.execute(f"""
                SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = '{table}' AND REFERENCED_TABLE_NAME IS NOT NULL;
            """)
            foreign_keys = {row[0]: f"{row[1]}({row[2]})" for row in cursor.fetchall()}  

            #   Process Column Info
            for row in describe_results:
                col_name, col_type = row[0], row[1]
                table_structure[col_name] = {
                    "type": col_type,
                    "primary_key": col_name in primary_keys,
                    "foreign_key": foreign_keys.get(col_name, None)
                }

            schema_data[table] = table_structure

        save_schema(schema_data)
        logging.info("Json Updated")

    except mysql.connector.Error as err:
        logging.error(f"  SQL Error: {err}")

    finally:
        if cursor.with_rows:
            cursor.fetchall()  #   Drain any unread results
        cursor.close()
        conn.close()