"""
This script will handle the bulk upload of user data to the Yelo API (asyncronously and concurrently).

Here we will:
    1. Load the clean data from the JSON file.
    2. Upload the data to the API in bulk.
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
YELO_API_BASE_URL = os.getenv("YELO_API_BASE_URL", "default_api_base_url")
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR", "default_clean_data_dir")
CLEAN_DATA_FILE_NAME = os.getenv("CLEAN_DATA_FILE_NAME", "default_clean_data.json")
RESULTS_DIR = os.getenv("RESULTS_DIR", "default_results_dir")
RESULTS_FILE_NAME = os.getenv("RESULTS_FILE_NAME", "default_results.json")

# --- Configuration ---
CURRENT_DIR = Path(".")
DATA_SOURCE_DIR = CURRENT_DIR / CLEAN_DATA_DIR
OUTPUT_DIR = CURRENT_DIR / RESULTS_DIR
JSON_PATH = DATA_SOURCE_DIR / CLEAN_DATA_FILE_NAME
RESULTS_PATH = OUTPUT_DIR / RESULTS_FILE_NAME


# --- Main Processing ---
async def main():
    """
    Main async function to load data and trigger the bulk upload.
    """
    all_users_data: list[CleanUserData] = []
    try:
        all_users_data = load_users_from_json(JSON_PATH, CleanUserData)  # type: ignore
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
        await run_bulk_upload(
            base_url=YELO_API_BASE_URL,
            users_data=all_users_data,
            results_file_path=RESULTS_PATH,
        )
        logger.info("Bulk upload process finished.")
    except Exception as e:
        logger.exception(
            f"An unexpected error occurred during the run_bulk_upload process. Error {e}"
        )
        sys.exit(1)


# --- Entry Point ---
if __name__ == "__main__":
    asyncio.run(main())
