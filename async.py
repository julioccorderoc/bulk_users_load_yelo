"""
Here we will execute the bulk upload stragy:
    1. Create users
    2. Create addresses
    3. Create custom fields
    4. Log all errors and details in a log file, and output in the terminal a proper summary
"""

import asyncio
import sys
import os
from pathlib import Path

from pydantic import ValidationError
from dotenv import load_dotenv


from src.utils import logger
from src.models import CleanUserData
from src.load_data import load_users_from_json
from src.upload_data import run_bulk_upload


# --- Environment Variables ---
load_dotenv()
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR")
CLEAN_DATA_FILE_NAME = os.getenv("CLEAN_DATA_FILE_NAME")


# --- Configuration ---
CURRENT_DIR = Path(".")
TARGET_DIR = CURRENT_DIR / CLEAN_DATA_DIR
JSON_PATH = TARGET_DIR / CLEAN_DATA_FILE_NAME


# --- Main Processing ---
async def main():
    """
    Main async function to load data and trigger the bulk upload.
    """
    all_users_data: list[CleanUserData] = []
    try:
        all_users_data = load_users_from_json(JSON_PATH, CleanUserData)
    except (FileNotFoundError, ValueError, ValidationError) as e:
        logger.error(f"Failed to prepare user data: {e}")
        logger.error("Aborting upload process.")
        sys.exit(1)
    except Exception as e:
        logger.exception(
            f"An unexpected error occurred during data preparation. Error {e}"
        )
        sys.exit(1)

    if not all_users_data:
        logger.warning("No user data found or loaded. Nothing to upload.")
        return

    logger.info(
        f"Data loaded successfully. Starting bulk upload for {len(all_users_data)} users..."
    )

    try:
        await run_bulk_upload(all_users_data)
        logger.info("Bulk upload process finished.")
    except Exception as e:
        logger.exception(
            f"An unexpected error occurred during the run_bulk_upload process. Error {e}"
        )
        sys.exit(1)


# --- Entry Point ---
if __name__ == "__main__":
    asyncio.run(main())
