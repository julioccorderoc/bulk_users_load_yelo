import requests
import json
import os
import sys
from pathlib import Path

from pydantic import ValidationError, BaseModel
from dotenv import load_dotenv

from src.utils import logger
from src.models import PostUserYelo, ResponsePostUserYelo, ResponsePostAddressYelo


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


def add_customers(
    customers: list[PostUserYelo],
    api_url: str = "https://beta-api.yelo.red/open/admin/customer/add",
) -> list[ResponsePostUserYelo]:
    """
    Adds a list of customers to the Yelo platform.

    Args:
        customers: A list of Customer Pydantic objects.
        api_url: The URL for the add customer API endpoint.

    Returns:
        A list of responses from the API for each customer.
    """
    headers = {"Content-Type": "application/json"}
    responses: list[ResponsePostUserYelo] = []
    for index, customer in enumerate(customers):
        payload = customer.model_dump_json()
        try:
            response = requests.post(api_url, headers=headers, data=payload)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            responses.append(ResponsePostUserYelo.model_validate(response.json()))
            logger.info(responses[index].data.customer_id)
        except requests.exceptions.RequestException as e:
            print(
                f"Error adding customer {customer.first_name} {customer.last_name}: {e}"
            )
            responses.append({"error": str(e)})  # Or handle the error as needed
    return responses


def main():
    """
    Main async function to load data and trigger the bulk upload.
    """
    all_users_data: list[PostUserYelo] = []
    try:
        # Load and validate data first
        all_users_data = load_users_from_json(
            file_path=DEFAULT_JSON_FILE_PATH, validation_model=PostUserYelo
        )
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
        api_responses = add_customers(all_users_data)
        for response in api_responses:
            print(response.data.customer_id)

        logger.info("Bulk upload process finished.")
    except Exception as e:
        logger.exception(
            f"An unexpected error occurred during the testing process. Error {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()


# url = "https://beta-api.yelo.red/open/admin/customer/address/add"

# # payload = """{
# #     "api_key": "7e415d1f0e4726c863f7d22621652103",
# #     "customer_id": 7554656,
# #     "address": "Avenida Libertador con calle 42, Barquisimeto, Iribarren, Lara, Venezuela",
# #     "postal_code": "3001",
# #     "house_no": "114696",
# #     "email": "julio.cordero+givemedata@jungleworks.com",
# #     "phone_no": "+58 4169876543",
# #     "latitude": 10.080138,
# #     "longitude": -69.334186,
# #     "name": "Natalia Anais",
# #     "loc_type": 0,
# #     "landmark": "114696"
# # }"""

# payload = """{
#     "api_key": "7e415d1f0e4726c863f7d22621652103",
#     "customer_id": 7585551,
#     "address": "Avenida Pedro León Torres con calle 54, Barquisimeto, Iribarren, Lara, Venezuela",
#     "house_no": "114454",
#     "phone_no": "+58 4166768654",
#     "latitude": 10.064805,
#     "longitude": -69.345002,
#     "name": "Natalia Anais",
#     "loc_type": 1
# }"""
# headers = {"Content-Type": "application/json"}

# response = requests.request("POST", url, headers=headers, data=payload)
# print(response.text)
# response = ResponsePostAddressYelo.model_validate(response.json())
# logger.info(response.message)
# logger.info(response.data.id)


# url = "https://beta-api.yelo.red/open/admin/customer/add"

# payload = """{
#     "api_key"    : "7e415d1f0e4726c863f7d22621652103",
#     "first_name" : "Isabel" ,
#     "last_name"  : "Cordero" ,
#     "email"      : "julio.cordero+isabela@jungleworks.com" ,
#     "phone_no"   : "+58 4166768654" ,
#     "password"   : "Demo@123"
# }"""
# headers = {"Content-Type": "application/json"}

# response = requests.request("POST", url, headers=headers, data=payload)
# response = ResponsePostUserYelo.model_validate(response.json())
# logger.info(response.message)
# logger.info(response.data.customer_id)
