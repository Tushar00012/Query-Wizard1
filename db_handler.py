import mysql.connector
import streamlit as st
import logging
from query_parser import fix_insert_query
from db_config import DB_CONFIG
from schema_handler import store_all_table_structures
import re

logging.basicConfig(level=logging.INFO)

def extract_table_name(query):
    """
    Extracts the table name from a SQL query.
    Works for SELECT, INSERT, UPDATE, DELETE, and CREATE statements.
    """
    match = re.search(r"(from|into|update|table)\s+`?(\w+)`?", query, re.IGNORECASE)
    return match.group(2) if match else None  # Extracted table name or None

def execute_query(query):
    """
    Executes multiple SQL queries separated by semicolons (;).
    Ensures that all results are fetched before closing the cursor to prevent unread results errors.
    """
    conn = mysql.connector.connect(**DB_CONFIG)  # Use centralized config
    cursor = conn.cursor()

    queries = query.strip().split(";")  #   Split multiple queries
    queries = [q.strip() for q in queries if q.strip()]  # Remove empty queries

    if not queries:
        st.warning(" No valid SQL query found.")
        return

    try:
        for q in queries:
            table_name = extract_table_name(q)

            if not table_name and q.lower().startswith("select"):
                table_name = "Unknown Table"  # Handle generic SELECT queries

            if q.lower().startswith("insert"):
                corrected_query, values_list = fix_insert_query(q, table_name)
                if not corrected_query:
                    st.error(values_list)  # Display error message
                    return

                cursor.executemany(corrected_query, values_list)
                conn.commit()
                st.success(f"  Insert query executed successfully for `{table_name}`!")

            elif q.lower().startswith(("select", "show", "describe")):
                cursor.execute(q)
                results = cursor.fetchall()  # Read all results
                column_names = [desc[0] for desc in cursor.description] if cursor.description else []

                if results:
                    st.write(f"**Query Results for `{table_name}`:**")
                    st.dataframe({col: [row[i] for row in results] for i, col in enumerate(column_names)})  # Display data

            else:
                cursor.execute(q)
                conn.commit()
                st.success(f"  Query executed successfully for `{table_name}`!")

    except mysql.connector.Error as err:
        st.error(f"❌ SQL Execution Error: {err}")
        logging.error(f"SQL Execution Error: {err}")

    finally:
        try:
            while cursor.nextset():  #   Handle multiple result sets
                pass
        except mysql.connector.InterfaceError:
            pass  # Ignore errors when no unread results exist

        cursor.close()
        conn.close()
