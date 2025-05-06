import asyncio
from pathlib import Path

from src.api_client import ApiClient
from src.utils import logger, save_to_json
from src.custom_exceptions import (
    ApiHttpError,
    ApiClientError,
    ApiResponseValidationError,
)
from src.models import (
    CleanUserData,
    ResponsePostUserYelo,
    PostUserYelo,
    PostUserAddressYelo,
    ResponsePostAddressYelo,
)


# --- Constants ---
POST_USER_ENDPOINT = "/open/admin/customer/add"
POST_ADDRESS_ENDPOINT = "/open/admin/customer/address/add"
POST_CUSTOM_FIELD_ENDPOINT = "/open/admin/customer/custom/add"


async def _post_user(user_data: CleanUserData, client: ApiClient) -> str | None:
    """
    Attempts to create a single user.

    Args:
        user_data: The data for the user to create.
        client: ApiClient instance.

    Returns:
        The customer_id (str) if creation is successful, None otherwise.
    """

    logger.debug(f"Attempting to create user: {user_data.email}.")
    user_payload: PostUserYelo
    created_user_response: ResponsePostUserYelo

    try:
        user_payload = PostUserYelo(
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

        # Validate if response structure is as expected even with 200 OK
        if (
            not created_user_response
            or not created_user_response.data
            or not created_user_response.data.customer_id
        ):
            logger.error(
                f"User creation API call succeeded (200 OK) but response format is invalid for {user_data.email}. Response: {created_user_response}"
            )
            raise ApiResponseValidationError(
                "User creation response invalid format.",
                status_code=200,
                response_body=created_user_response,
            )

        customer_id = created_user_response.data.customer_id
        logger.info(
            f"Successfully created user {user_data.email}. Yelo ID: {customer_id}"
        )
        return customer_id

    except (ApiHttpError, ApiClientError, ApiResponseValidationError) as e:
        logger.error(f"Failed to create user {user_data.email}. Error: {e}.")
        user_data.error_message = f"User creation failed: {e}."
        return None
    except Exception as e:
        logger.exception(
            f"Unexpected error during user creation for {user_data.email}. Error: {e}."
        )
        user_data.error_message = f"Unexpected user creation error: {e}"
        return None


async def _post_addresses(
    user_data: CleanUserData, customer_id: str, client: ApiClient
) -> bool:
    """
    Attempts to create addresses for a given user ID.
    Updates status on individual address objects within user_data.

    Args:
        user_data: The user data object containing the list of addresses.
        customer_id: The customer ID obtained after user creation.
        client: The initialized ApiClient instance.

    Returns:
        True if all address creation attempts were successful (or if no addresses),
        False if any address creation attempt failed.
    """
    if not user_data.addresses:
        logger.debug(f"No addresses to upload for user {user_data.email}.")
        return True

    logger.info(
        f"Uploading {len(user_data.addresses)} addresses for user {user_data.email}."
    )
    address_payload: PostUserAddressYelo
    created_address_response: ResponsePostAddressYelo
    any_address_failed: bool = False

    for index, address_data in enumerate(user_data.addresses):
        try:
            if address_data.id is not None:
                logger.debug(
                    f"Address {index + 1}/{len(user_data.addresses)} already created for user {user_data.email}."
                )
                continue

            address_data.upload_status = "processing"
            full_name: str = f"{user_data.first_name} {user_data.last_name}"
            address_payload = PostUserAddressYelo(
                name=full_name,
                customer_id=customer_id,
                email=user_data.email,
                phone_no=user_data.phone_no,
                loc_type=address_data.loc_type,
                address=address_data.address,
                house_no=address_data.house_no,
                latitude=address_data.latitude,
                longitude=address_data.longitude,
            )

            created_address_response = await client.post(
                endpoint=POST_ADDRESS_ENDPOINT,
                payload=address_payload,
                expected_status=200,
                response_model=ResponsePostAddressYelo,
            )

            if (
                not created_address_response
                or not created_address_response.data
                or not created_address_response.data.id
            ):
                logger.error(
                    f"Address creation API call succeeded (200 OK) but response format is invalid for user {customer_id}, address index {index}. Response: {created_address_response}"
                )
                raise ApiResponseValidationError(
                    "Address creation response invalid format.",
                    status_code=200,
                    response_body=created_address_response,
                )

            address_data.id = created_address_response.data.id
            address_data.upload_status = "success"
            logger.debug(
                f"Address {index + 1}/{len(user_data.addresses)} created successfully for user {customer_id}. Yelo Address ID: {address_data.id}."
            )

        except (ApiHttpError, ApiClientError, ApiResponseValidationError) as e:
            logger.error(
                f"Failed to create address index {index} for user {customer_id}. Data: {address_data.model_dump_json(exclude={'upload_status', 'id'})}. Error: {e}."
            )
            address_data.upload_status = "failed"
            address_data.error_message = str(e)
            any_address_failed = True
        except Exception as e:
            logger.exception(
                f"Unexpected error creating address index {index} for user {customer_id}. Data: {address_data.model_dump_json(exclude={'upload_status', 'id'})}. Error: {e}."
            )
            address_data.upload_status = "failed"
            address_data.error_message = f"Unexpected error: {e}."
            any_address_failed = True

    if any_address_failed:
        logger.warning(
            f"One or more addresses failed to upload for user {customer_id}."
        )
        return False
    else:
        if user_data.addresses:
            logger.info(
                f"All {len(user_data.addresses)} addresses processed successfully for user {customer_id}."
            )
        return True


async def _post_custom_fields(
    user_data: CleanUserData, customer_id: str, client: ApiClient
) -> bool:
    """
    Placeholder for creating custom fields.

    Args:
        user_data: User data containing custom fields.
        customer_id: The Yelo customer ID.
        client: The ApiClient instance.

    Returns:
        True (currently always true as it's not implemented).
    """
    # if not user_data.custom_fields:
    #     logger.debug(f"No custom fields to upload for user {customer_id}.")
    #     return True
    #
    # logger.info(f"Uploading {len(user_data.custom_fields)} custom fields for user {customer_id}...")
    # any_field_failed = False
    #
    # # --- Loop through custom fields ---
    # # for field_data in user_data.custom_fields:
    # #    try:
    # #        # Reset status
    # #        field_data.upload_status = "processing"
    # #        # Create payload (e.g., PostCustomFieldYelo)
    # #        field_payload = ...
    # #        # Make API call
    # #        await client.post(
    # #             endpoint=POST_CUSTOM_FIELD_ENDPOINT, # Or specific endpoint like f"users/{customer_id}/custom_fields"
    # #             payload=field_payload,
    # #             expected_status=201 # Or 200
    # #             # response_model=ResponseCustomFieldYelo # If applicable
    # #        )
    # #        # Update field status/id
    # #        field_data.upload_status = "success"
    # #        # field_data.id = response.data.id # If applicable
    # #        logger.debug(f"Custom field '{field_data.field_key}' created for user {customer_id}")
    # #    except (ApiHttpError, ApiClientError, ApiResponseValidationError) as e:
    # #        logger.error(f"Failed to create custom field '{field_data.field_key}' for user {customer_id}. Error: {e}")
    # #        field_data.upload_status = "failed"
    # #        any_field_failed = True
    # #    except Exception as e:
    # #        logger.exception(f"Unexpected error creating custom field '{field_data.field_key}' for user {customer_id}. Error: {e}")
    # #        field_data.upload_status = "failed"
    # #        any_field_failed = True
    #
    # if any_field_failed:
    #     logger.warning(f"One or more custom fields failed to upload for user {customer_id}.")
    #     return False
    # else:
    #     # logger.info(f"All {len(user_data.custom_fields)} custom fields uploaded successfully for user {customer_id}.")
    #     return True # Return True if section is commented out

    # Remove this line when implementing custom fields
    logger.debug(
        f"Custom field upload skipped for user {customer_id} (not implemented)."
    )
    return True


async def upload_user(user_data: CleanUserData, client: ApiClient):
    """
    Orchestrates the upload of one user with their addresses and custom fields.
    Updates the overall status field on the user_data object.
    """

    logger.info(f"----- Processing User: {user_data.email} -----")
    user_data.upload_status = "processing"

    # --- Step 1: Create User ---
    customer_id = await _post_user(user_data, client)

    if customer_id is None:
        user_data.upload_status = "failed"
        logger.error(
            f"User creation failed for {user_data.email}. Skipping addresses/fields."
        )
        return

    user_data.customer_id = customer_id

    # --- Step 2: Create Addresses ---
    all_addresses_succeeded = await _post_addresses(user_data, customer_id, client)

    # --- Step 3: Create Custom Fields ---
    all_fields_succeeded = await _post_custom_fields(user_data, customer_id, client)

    # --- Step 4: Determine Final User Status ---
    if all_addresses_succeeded and all_fields_succeeded:
        user_data.upload_status = "success"
        logger.info(
            f"Successfully processed user {user_data.email}. All sub-tasks successful."
        )
    else:
        # Check if *anything* succeeded after user creation (at least one address or field)
        any_sub_task_success = any(
            a.upload_status == "success" for a in user_data.addresses
        )
        # or any(f.upload_status == "success" for f in user_data.custom_fields) # Add when fields implemented

        if any_sub_task_success:
            user_data.upload_status = "partial"
            user_data.error_message = (
                "User created, but one or more addresses/fields failed."
            )
            logger.warning(
                f"Partially processed user {user_data.email}. See sub-task statuses."
            )
        else:
            # User created, but *all* subsequent steps failed
            user_data.upload_status = "failed"
            user_data.error_message = "User created, but all addresses/fields failed."
            logger.error(
                f"Failed to process user {user_data.email} after user creation. All sub-tasks failed."
            )


# --- Main Orchestration Function ---
async def run_bulk_upload(
    base_url: str,
    users_data: list[CleanUserData],
    results_file_path: Path,
):
    """
    Runs the bulk upload process concurrently for all users.
    """
    success_count: int = 0
    failed_count: int = 0
    partial_count: int = 0

    async with ApiClient(base_url=base_url) as client:
        tasks_to_run = []
        users_to_process_indices = []
        for index, user_data_item in enumerate(users_data):
            if user_data_item.customer_id is None:
                users_to_process_indices.append(index)
                task = asyncio.create_task(upload_user(user_data_item, client))
                tasks_to_run.append(task)
            else:
                logger.info(f"User {user_data_item.email} already processed. Skipping.")

        if not tasks_to_run:
            logger.warning("No users to process. Nothing to upload.")
            return

        logger.info(f"Starting concurrent upload for {len(tasks_to_run)} users...")

        # --- Run tasks concurrently and wait for completion ---
        # `return_exceptions=True` means if one task fails with an exception,
        # gather won't immediately stop; it will return the exception object
        # in the results list for that task. An unexpected error in
        # `upload_user` shouldn't halt others.
        results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

        logger.info("Concurrent uploads finished. Processing results...")

        # --- Process Results ---
        for index, result in enumerate(results):
            original_index: int = users_to_process_indices[index]
            user_data_result = users_data[original_index]
            final_status = user_data_result.upload_status
            if isinstance(result, Exception):
                # An unexpected exception occurred *outside* the try/except blocks
                # within upload_user (less likely with good handling inside).
                # Or gather itself had an issue.
                logger.error(
                    f"Task for user {user_data_result.email} failed unexpectedly: {result}"
                )
                # Ensure status reflects this unexpected failure if not already set
                if final_status not in ["failed", "partial"]:
                    final_status = "failed"
                    if not user_data_result.error_message:
                        user_data_result.error_message = (
                            f"Unexpected task failure: {result}"
                        )

            # Tally results based on the status set within upload_user
            if final_status not in ["success", "partial", "failed"]:
                logger.warning(
                    f"User {user_data_result.email} ended with unexpected status. Counting as failed."
                )
                final_status = "failed"
            if not user_data_result.error_message:
                user_data_result.error_message = (
                    "Processing did not complete or ended in unexpected state."
                )

            if final_status == "success":
                success_count += 1
            elif final_status == "partial":
                partial_count += 1
            else:  # "failed"
                failed_count += 1
                if user_data_result.error_message:
                    logger.debug(
                        f"Final failure reason for {user_data_result.email}: {user_data_result.error_message}"
                    )

    save_to_json(users_data, results_file_path)
    logger.info("--- Bulk Upload Summary ---")
    logger.info(f"Total users processed: {len(users_data)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Partial : {partial_count}")
    logger.info(f"Failed: {failed_count}")
