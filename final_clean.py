import json
import os
from collections import Counter

from dotenv import load_dotenv

load_dotenv()
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR", "default_clean_data_dir")
CLEAN_DATA_FILE_NAME = os.getenv("CLEAN_DATA_FILE_NAME", "default_clean_data.json")

# --- Configuration ---
INPUT_JSON_FILE = os.path.join(CLEAN_DATA_DIR, CLEAN_DATA_FILE_NAME)
OUTPUT_DIR = "final_clean"  # New output directory name
SINGLE_ADDRESS_USERS_FILE = os.path.join(OUTPUT_DIR, "calida_users_single_address.json")
MULTI_ADDRESS_USERS_FILE = os.path.join(OUTPUT_DIR, "calidda_users_multi_address.json")
DROPPED_CONTACTS_SUMMARY_FILE = os.path.join(OUTPUT_DIR, "dropped_summary.txt")


# --- Main Processing ---
def process_users_from_json(
    input_file, output_single_addr_file, output_multi_addr_file, summary_file_path
):
    # --- Create output directory first ---
    # This ensures the directory exists before trying to create files in it.
    # os.path.dirname(summary_file_path) will give OUTPUT_DIR
    # os.makedirs(os.path.dirname(summary_file_path), exist_ok=True) # This is also fine
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    summary_fh = None  # Initialize to None
    try:
        summary_fh = open(summary_file_path, "w")

        def dual_print(message):
            print(message)
            if summary_fh:  # Check if file handle is valid
                summary_fh.write(message + "\n")

        dual_print(f"Starting processing for: {input_file}")
        dual_print(f"{'=' * 40}")

        try:
            with open(input_file, "r") as f:
                all_users_loaded = json.load(f)
        except FileNotFoundError:
            dual_print(f"ERROR: Input file '{input_file}' not found. Exiting.")
            return  # Exit the function
        except json.JSONDecodeError:
            dual_print(f"ERROR: Could not decode JSON from '{input_file}'. Exiting.")
            return  # Exit the function
        except Exception as e:
            dual_print(f"ERROR: Failed to load users: {e}. Exiting.")
            return  # Exit the function

        if not all_users_loaded:
            dual_print("No users found in the input file.")
            return  # Exit the function

        initial_user_count = len(all_users_loaded)
        dual_print(f"\nInitial users loaded: {initial_user_count}")
        dual_print("--- Dropping Users with Non-Unique Contacts ---")

        # 1. Identify non-unique emails (case-insensitive)
        email_counts = Counter()
        if initial_user_count > 0:  # Only proceed if there are users
            for user in all_users_loaded:
                email = user.get("email")
                if email:  # Consider only non-empty emails
                    email_counts[email.lower()] += 1

        shared_emails = {email for email, count in email_counts.items() if count > 1}
        dual_print(
            f"- Found {len(shared_emails)} email addresses that are shared by multiple users."
        )
        if (
            len(shared_emails) > 0 and initial_user_count > 0
        ):  # Log example shared emails if any
            dual_print(f"  (Examples of shared emails: {list(shared_emails)[:3]})")

        # Filter out users who have one of these shared emails
        users_after_email_filter = []
        # dropped_by_email_details = {} # Optional: To store which email caused drop for which user

        if initial_user_count > 0:
            for user in all_users_loaded:
                email = user.get("email")
                if email and email.lower() in shared_emails:
                    # Optional detailed logging:
                    # dual_print(f"  - Dropping user {user_id_for_log} due to shared email: {email.lower()}")
                    # if email.lower() not in dropped_by_email_details:
                    #     dropped_by_email_details[email.lower()] = []
                    # dropped_by_email_details[email.lower()].append(user_id_for_log)
                    pass  # User is dropped, so not added to next list
                else:
                    users_after_email_filter.append(user)

        count_after_email_filter = len(users_after_email_filter)
        dual_print(
            f"- Users remaining after shared email filter: {count_after_email_filter} (dropped {initial_user_count - count_after_email_filter})"
        )

        # 2. From the remaining users, identify non-unique phone numbers
        phone_counts = Counter()
        if count_after_email_filter > 0:
            for user in users_after_email_filter:
                phone = user.get("phone_no")
                if phone:
                    phone_counts[phone] += 1

        shared_phones = {phone for phone, count in phone_counts.items() if count > 1}
        dual_print(
            f"- Found {len(shared_phones)} phone numbers shared by multiple users (among remaining users)."
        )
        if len(shared_phones) > 0 and count_after_email_filter > 0:
            dual_print(f"  (Examples of shared phones: {list(shared_phones)[:3]})")

        # Filter out users who have one of these shared phones
        final_users_survived = []
        # dropped_by_phone_details = {} # Optional

        if count_after_email_filter > 0:
            for user in users_after_email_filter:
                phone = user.get("phone_no")
                if phone and phone in shared_phones:
                    # Optional detailed logging:
                    # dual_print(f"  - Dropping user {user_id_for_log} due to shared phone: {phone}")
                    # if phone not in dropped_by_phone_details:
                    #     dropped_by_phone_details[phone] = []
                    # dropped_by_phone_details[phone].append(user_id_for_log)
                    pass  # User is dropped
                else:
                    final_users_survived.append(user)

        count_after_phone_filter = len(final_users_survived)
        dual_print(
            f"- Users remaining after shared phone filter: {count_after_phone_filter} (dropped {count_after_email_filter - count_after_phone_filter} from this step)"
        )
        dual_print(
            f"Total users dropped due to shared contacts: {initial_user_count - count_after_phone_filter}"
        )

        dual_print("\n--- Address Segmentation Phase ---")
        # 3. Segment users by address count
        single_address_users = []
        multi_address_users = []

        if not final_users_survived:  # Check if list is empty
            dual_print(
                "No users remaining after contact filtering for address segmentation."
            )
        else:
            for user in final_users_survived:
                addresses = user.get("addresses", [])
                if len(addresses) == 1:
                    single_address_users.append(user)
                else:  # Includes users with 0 addresses or >1 address
                    multi_address_users.append(user)

            dual_print(f"- Users with exactly one address: {len(single_address_users)}")
            dual_print(
                f"- Users with multiple (or zero) addresses: {len(multi_address_users)}"
            )

        # 4. Save output files
        dual_print("\n--- Saving Output ---")
        try:
            with open(output_single_addr_file, "w") as f:
                json.dump(single_address_users, f, indent=4)
            dual_print(
                f"- Saved {len(single_address_users)} single-address users to: {output_single_addr_file}"
            )
        except Exception as e:
            dual_print(f"ERROR: Could not save single-address users file: {e}")

        try:
            with open(output_multi_addr_file, "w") as f:
                json.dump(multi_address_users, f, indent=4)
            dual_print(
                f"- Saved {len(multi_address_users)} multi-address users to: {output_multi_addr_file}"
            )
        except Exception as e:
            dual_print(f"ERROR: Could not save multi-address users file: {e}")

        dual_print("\nProcessing finished.")
        dual_print("--- Final Summary of User Counts ---")
        dual_print(f"- Initial users loaded: {initial_user_count}")
        dual_print(
            f"- Users remaining after all filters (these are segmented): {len(final_users_survived)}"
        )
        dual_print(f"  -> Users with one address (saved): {len(single_address_users)}")
        dual_print(
            f"  -> Users with multiple/zero addresses (saved): {len(multi_address_users)}"
        )

    finally:  # Ensure summary file is closed even if errors occur
        if summary_fh:
            summary_fh.close()


if __name__ == "__main__":
    process_users_from_json(
        INPUT_JSON_FILE,
        SINGLE_ADDRESS_USERS_FILE,
        MULTI_ADDRESS_USERS_FILE,
        DROPPED_CONTACTS_SUMMARY_FILE,
    )
