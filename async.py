"""
Here we will execute the bulk upload stragy:
    1. Create users
    2. Create addresses
    3. Create custom fields
    4. Log all errors and details in a log file, and output in the terminal a proper summary
"""

import asyncio
import json
import sys
import os
from pathlib import Path

from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv

from src.upload import run_bulk_upload
from src.utils import logger
from src.models import CleanUserData


# --- Environment Variables ---
load_dotenv()
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR")


# --- Configuration ---
current_dir = Path(".")
parent_dir = current_dir.parent
target_dir = parent_dir / CLEAN_DATA_DIR
DEFAULT_JSON_FILE_PATH = target_dir / "users_phone_email.json"


def load_users_from_json(
    file_path: Path,
    validation_model: BaseModel,
) -> list[BaseModel]:
    """
    Loads user data from a JSON file and validates it against the model.

    Args:
        file_path: The path to the JSON file.

    Returns:
        A list of validated BaseModel objects.

    Raises:
        FileNotFoundError: If the JSON file does not exist.
        ValueError: If JSON is invalid or doesn't contain a list.
        ValidationError: If the data doesn't match the schema.
    """
    logger.info(f"Attempting to load user data from: {file_path}")

    if not file_path.is_file():
        logger.error(f"JSON data file not found at: {file_path}")
        raise FileNotFoundError(f"Data file not found: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if not isinstance(raw_data, list):
            raise ValueError(
                "Invalid JSON format: Root element must be a list (array) of user objects."
            )

        validated_users = [
            validation_model.model_validate(user_dict) for user_dict in raw_data
        ]

        logger.info(
            f"Successfully loaded and validated {len(validated_users)} user records from {file_path}."
        )
        return validated_users

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON file: {file_path}. Error: {e}")
        raise ValueError(f"Invalid JSON content in {file_path}") from e
    except ValidationError as e:
        logger.error(
            f"Data validation failed for records in {file_path}. See details below."
        )
        logger.error(e)
        raise ValidationError(
            "JSON data does not conform to UserUploadData schema.",
            model=validation_model,
        ) from e
    except Exception as e:
        logger.exception(
            f"An unexpected error occurred while loading data from {file_path}. Error: {e}"
        )
        raise


async def main():
    """
    Main async function to load data and trigger the bulk upload.
    """
    all_users_data: list[CleanUserData] = []
    try:
        # Load and validate data first
        all_users_data = load_users_from_json(DEFAULT_JSON_FILE_PATH, CleanUserData)
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


if __name__ == "__main__":
    asyncio.run(main())
