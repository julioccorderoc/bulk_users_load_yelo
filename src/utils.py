import logging
import json
from pathlib import Path

from pydantic import BaseModel

yelo_headers: dict[str, str] = {"Content-Type": "application/json"}

generic_headers: dict[str, str] = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# --- Logger Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s",
)
logger = logging.getLogger(__name__)


# --- Save json file ---
def save_to_json(data_to_save: list[BaseModel], file_path: Path) -> None:
    """Saves the list of user data objects (with final status) to a JSON file."""

    logger.info(f"Attempting to save final results to: {file_path}")
    try:
        results_to_save = [user.model_dump(mode="json") for user in data_to_save]

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(results_to_save, f, indent=4, ensure_ascii=False)

        logger.info(
            f"Successfully saved final results for {len(data_to_save)} users to {file_path}."
        )

    except TypeError as e:
        logger.error(
            f"Failed to serialize results to JSON. Ensure all data is JSON-serializable. Error: {e}."
        )
    except IOError as e:
        logger.error(
            f"Failed to write results file to {file_path}. Check permissions and path. Error: {e}."
        )
    except Exception as e:
        logger.exception(
            f"An unexpected error occurred while saving results to {file_path}. Error: {e}."
        )
