import asyncio
import os

# from pydantic import BaseModel
from dotenv import load_dotenv

from src.api_client import ApiClient
from src.utils import logger
from src.custom_exceptions import ApiHttpError, ApiClientError
from src.models import (
    CleanUserData,
    ResponsePostUserYelo,
    PostUserYelo,
    PostUserAddressYelo,
    ResponsePostAddressYelo,
)


load_dotenv()
YELO_API_BASE_URL = os.getenv("YELO_API_BASE_URL")
YELO_API_KEY = os.getenv("YELO_API_KEY")
POST_USER_ENDPOINT = os.getenv("POST_USER_ENDPOINT")
POST_ADDRESS_ENDPOINT = os.getenv("POST_ADDRESS_ENDPOINT")
POST_CUSTOM_FIELD_ENDPOINT = os.getenv("POST_CUSTOM_FIELD_ENDPOINT")


async def upload_single_user(user_data: CleanUserData, client: ApiClient):
    """
    Attempts to upload one user with their addresses and custom fields using the provided API client.
    Updates the status fields on the user_data object directly.
    """
    user_payload: PostUserYelo
    created_user_response: ResponsePostUserYelo
    address_payload: PostUserAddressYelo
    created_address_response: ResponsePostAddressYelo
    user_log_id: str = user_data.password + " - " + user_data.email

    logger.info(f"Processing user: {user_log_id}")
    user_data.upload_status = "processing"

    try:
        # --- 1. Create User ---
        user_payload = PostUserYelo(
            api_key=YELO_API_KEY,
            first_name=user_data.first_name,
            last_name=user_data.last_name,
            email=user_data.email,
            phone_no=user_data.phone_no,
            password=user_data.password,
        )

        created_user_response = await client.post(
            endpoint=POST_USER_ENDPOINT,
            payload=user_payload,
            expected_status=200,
            response_model=ResponsePostUserYelo,
        )
        user_data.customer_id = created_user_response.data.customer_id

        logger.info(f"User {user_log_id} created. Yelo ID: {user_data.customer_id}")

        # --- If user creation succeeds, proceed with addresses/fields ---
        user_failed: bool = False  # Flag to track if any sub-step fails

        # --- 2. Create Addresses ---
        if user_data.addresses:
            logger.info(
                f"Uploading {len(user_data.addresses)} addresses for user {user_log_id}..."
            )
            # You *could* run these concurrently too with another asyncio.gather,
            # but let's keep it sequential per user for simplicity first.
            # Be mindful of potential API rate limits if running address uploads concurrently.
            for index, address in enumerate(user_data.addresses):
                try:
                    address_payload = PostUserAddressYelo(
                        api_key=YELO_API_KEY,
                        name=user_data.first_name,
                        loc_type=address.loc_type,
                        customer_id=user_data.customer_id,
                        email=user_data.email,
                        phone_no=user_data.phone_no,
                        address=address.address,
                        house_no=address.house_no,
                        latitude=address.latitude,
                        longitude=address.longitude,
                    )
                    # # This is to handle multiple locations
                    # if index <= 2:
                    #     address_payload.loc_type = index
                    # else:
                    #     address_payload.loc_type = 2

                    created_address_response = await client.post(
                        endpoint=POST_ADDRESS_ENDPOINT,
                        payload=address_payload,
                        expected_status=200,
                        response_model=ResponsePostAddressYelo,
                    )
                    address.id = created_address_response.data.id
                    address.upload_status = "success"
                    logger.debug(f"Address created for user {user_log_id}")

                except (ApiHttpError, ApiClientError) as e:
                    logger.error(
                        f"Failed to create address for user {user_log_id}. Data: {address.model_dump_json()}. Error: {e}"
                    )
                    address.upload_status = "failed"
                    user_failed = True
                except Exception as e:  # Catch unexpected errors
                    logger.exception(
                        f"Unexpected error creating address for user {user_log_id}. Data: {address.model_dump_json()}. Error: {e}"
                    )
                    address.upload_status = "failed"
                    user_failed = True

        # # --- 3. Create Custom Fields ---
        # if user_data.custom_fields:
        #     logger.info(
        #         f"Uploading {len(user_data.custom_fields)} custom fields for user {user_data.yelo_user_id}..."
        #     )
        #     # Similar loop and error handling as addresses
        #     for field in user_data.custom_fields:
        #         try:
        #             field_payload = {
        #                 "key": field.field_key,
        #                 "value": field.field_value,
        #                 # ...
        #             }
        #             await client.post(
        #                 endpoint=f"users/{user_data.yelo_user_id}/custom_fields",  # Example endpoint
        #                 payload=field_payload,
        #                 expected_status=201,
        #             )
        #             field.status = "success"
        #             logger.debug(
        #                 f"Custom field '{field.field_key}' created for user {user_data.yelo_user_id}"
        #             )
        #         except (ApiHttpError, ApiClientError) as e:
        #             logger.error(
        #                 f"Failed to create custom field '{field.field_key}' for user {user_data.yelo_user_id}. Error: {e}"
        #             )
        #             field.status = "failed"
        #             user_failed = True
        #         except Exception as e:
        #             logger.exception(
        #                 f"Unexpected error creating custom field '{field.field_key}' for user {user_data.yelo_user_id}. Error: {e}"
        #             )
        #             field.status = "failed"
        #             user_failed = True

        # --- Finalize User Status ---
        if user_failed:
            # Check if *all* addresses/fields failed along with user? Or just some?
            # You might want more granular statuses like "partial_success"
            user_data.upload_status = (
                "partial"
                if any(a.upload_status == "success" for a in user_data.addresses)
                # or any(f.upload_status == "success" for f in user_data.custom_fields)
                else "failed"
            )
            logger.warning(
                f"Partial or failed upload for user {user_log_id}. See address/field statuses."
            )
        else:
            user_data.upload_status = "success"
            logger.info(f"Successfully processed user {user_log_id}.")

    except (ApiHttpError, ApiClientError) as e:
        # Handle errors during the initial user creation POST
        logger.error(f"Failed to create user {user_log_id}. Error: {e}")
        user_data.upload_status = "failed"
    except (
        Exception
    ) as e:  # Catch any other unexpected errors during user creation phase
        logger.exception(f"Unexpected error processing user {user_log_id}. Error: {e}")
        user_data.upload_status = "failed"

    # No return value needed, status is updated on the input object


# --- Main Orchestration Function ---
async def run_bulk_upload(
    users_data: list[CleanUserData], base_url: str = YELO_API_BASE_URL
):
    """
    Runs the bulk upload process concurrently for all users.
    """
    success_count = 0
    failed_count = 0
    partial_count = 0

    async with ApiClient(base_url=base_url) as client:
        # Create a list of tasks to run concurrently
        tasks = []
        for user_data in users_data:
            # Pass the SAME client instance to each task
            task = asyncio.create_task(upload_single_user(user_data, client))
            tasks.append(task)

        logger.info(f"Starting concurrent upload for {len(tasks)} users...")

        # --- Run tasks concurrently and wait for completion ---
        # `asyncio.gather` runs them concurrently.
        # `return_exceptions=True` means if one task fails with an exception,
        # gather won't immediately stop; it will return the exception object
        # in the results list for that task. This is useful here because
        # an unexpected error in `upload_single_user` shouldn't halt others.
        results = await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("Concurrent uploads finished. Processing results...")

        # --- Process Results (optional, as status is on user_data objects) ---
        for index, result in enumerate(results):
            user_data = users_data[index]  # Get corresponding user data
            user_log_id: str = user_data.password + " - " + user_data.email
            if isinstance(result, Exception):
                # An unexpected exception occurred *outside* the try/except blocks
                # within upload_single_user (less likely with good handling inside).
                # Or gather itself had an issue.
                logger.error(
                    f"Task for user {user_log_id} failed unexpectedly: {result}"
                )
                # Ensure status reflects this unexpected failure if not already set
                if user_data.upload_status not in ["failed", "partial"]:
                    user_data.upload_status = "failed"

            # Tally results based on the status set within upload_single_user
            if user_data.upload_status == "success":
                success_count += 1
            elif user_data.upload_status == "partial":
                partial_count += 1
            else:  # None, "processing", "failed", or unexpected error case
                failed_count += 1
                # Ensure failed status if processing was interrupted (e.g., status is still None or processing)
                if user_data.upload_status not in ["failed", "partial"]:
                    user_data.upload_status = "failed"
                    if not user_data.error_message:
                        user_data.error_message = (
                            "Processing did not complete successfully."
                        )

    logger.info("--- Bulk Upload Summary ---")
    logger.info(f"Total users processed: {len(users_data)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Partial : {partial_count}")
    logger.info(f"Failed: {failed_count}")

    # --- Optional: Save results/status back to a file/database ---
    # for user in users_data:
    #     print(f"User: {user.client_identifier}, Status: {user.upload_status}, Yelo ID: {user.yelo_user_id}, Error: {user.error_message}")
    #     # You could write user.model_dump_json() to a results file
