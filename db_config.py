
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure critical env variables exist
required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
for var in required_vars:
    if not os.getenv(var):
        logging.warning(f"Missing environment variable: {var}")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "@Alpino890"),
    "database": os.getenv("DB_NAME", "school"),
}

