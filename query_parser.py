import re
import logging
from schema_handler import get_table_columns

logging.basicConfig(level=logging.INFO)

def fix_insert_query(query, table_name):
    """Fixes an INSERT query by ensuring correct column names and formatting values properly."""
    column_names = get_table_columns(table_name)

    if not column_names:
        return None, f"  Table `{table_name}` not found in schema."

    values_match = re.search(r"VALUES\s*(.*)", query, re.IGNORECASE | re.DOTALL)
    if not values_match:
        return None, "  No values found in the INSERT statement."

    values_str = values_match.group(1).strip().rstrip(";")
    raw_values_list = re.findall(r"\((.*?)\)", values_str, re.DOTALL)

    if not raw_values_list:
        return None, "  No values found in the INSERT statement."

    values_list = []
    for raw_values in raw_values_list:
        values = [val.strip().strip("'") for val in raw_values.split(",")]
        values_list.append(tuple(values))

    if values_list[0] == tuple(column_names):
        values_list.pop(0)

    if not values_list or len(values_list[0]) != len(column_names):
        return None, f"  Column Mismatch: `{table_name}` expects {len(column_names)} columns."

    corrected_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})"

    return corrected_query, values_list
