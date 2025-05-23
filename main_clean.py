"""
This script will clean and transform raw user data from CSV to JSON format.

Here we will:
    1. Load, clean and transform the data.
    2. Group the data by unique users.
    3. Segment users based on phone/email availability.
    4. Save segmented data to separate JSON files.
"""

import pandas as pd
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from src.utils import logger
from src.cleaning import (
    split_name,
    format_phone,
    aggregate_user_data,
    is_valid_email_format,
)


# --- Environment Variables ---
load_dotenv()
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "default_raw_data_dir")
RAW_DATA_FILE_NAME = os.getenv("RAW_DATA_FILE_NAME", "default_raw_data")
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR", "default_clean_data_dir")
CLEAN_DATA_FILE_NAME = os.getenv("CLEAN_DATA_FILE_NAME", "default_clean_data.json")


# --- Constants ---
CURRENT_DIR = Path(".")
TARGET_DIR = CURRENT_DIR / RAW_DATA_DIR
FILE_DIR = TARGET_DIR / RAW_DATA_FILE_NAME

CHECKPOINT_FILE = os.path.join(CLEAN_DATA_DIR, "raw_data_standardized.json")
OUTPUT_FILES = {
    "both": os.path.join(CLEAN_DATA_DIR, CLEAN_DATA_FILE_NAME),
    "phone_only": os.path.join(CLEAN_DATA_DIR, "usable_data.json"),
    "email_only": os.path.join(CLEAN_DATA_DIR, "only_email.json"),
    "neither": os.path.join(CLEAN_DATA_DIR, "unusable_data.json"),
}
initial_row_count: int = 0
initial_unique_users: int = 0
rows_before_cc_drop: int = 0
rows_after_cc_drop: int = 0
unique_users_after_cc_drop: int = 0


# --- Main Processing ---
logger.info(f"Starting data processing for: {FILE_DIR}...")
os.makedirs(CLEAN_DATA_DIR, exist_ok=True)


# ---------------------------
# --- 1. RENAME ORIGINAL FILE
# ---------------------------

# This is done in the excel file (if needed)
# change the original names of the columns to the ones used in the code
# This are the correct names: INTERLOCUTOR;NUM_IDENT;CTA_CONTR;CATEGORIA_CTA;NOMBRE;SALDO_DISPONIBLE;CELULAR;CELULAR_FINAL;CORREO;CTA_CONTR2;DIREC2;DISTRITO;NSE;CORD_X;CORD_Y

# ---------------------------
# --- 2. LOAD ---------------
# ---------------------------

column_dtypes = {
    "INTERLOCUTOR": str,
    "NUM_IDENT": str,
    "NOMBRE": str,
    "CATEGORIA_CTA": str,
    "SALDO_DISPONIBLE": str,
    "CELULAR": str,
    "CELULAR_FINAL": str,
    "CORREO": str,
    "CTA_CONTR": str,
    "CTA_CONTR2": str,
    "DIREC2": str,
    "DISTRITO": str,
    "NSE": str,
    "CORD_Y": str,
    "CORD_X": str,
}


try:
    df: pd.DataFrame = pd.read_csv(
        FILE_DIR, delimiter=";", dtype=column_dtypes, engine="c"
    )
    logger.info(f"Shape: {df.shape}")
except FileNotFoundError:
    logger.info(f"ERROR: Input file '{FILE_DIR}' not found. Exiting.")
    exit()
except Exception as e:
    logger.info(f"ERROR: Failed to load CSV: {e}. Exiting.")
    exit()

initial_row_count = len(df)
logger.info(f"Loaded {initial_row_count} rows.")

initial_unique_users = df["NUM_IDENT"].nunique()
logger.info(f"Unique NUM_IDENT values: {initial_unique_users}")

# ---------------------------
# --- 3. CLEAN --------------
# ---------------------------

# Remove unused columns
unused_columns = [
    "SALDO_DISPONIBLE",
    "fijo",
    "NSE",
    "CATEGORIA_CTA",
    "CTA_CONTR2",
]
df.drop(columns=unused_columns, inplace=True, errors="ignore")

# Remove NA values
essential_columns = ["CTA_CONTR", "NUM_IDENT", "NOMBRE"]
df.dropna(subset=essential_columns, inplace=True)

rows_dropped_for_errors = initial_row_count - len(df)
if rows_dropped_for_errors > 0:
    logger.info(
        f"Removed {rows_dropped_for_errors} rows missing essential data in: {', '.join(essential_columns)}"
    )

####

logger.info("Cleaning and validating coordinate data (CORD_Y, CORD_X)...")
coordinate_columns_to_process = ["CORD_Y", "CORD_X"]

for col_name in coordinate_columns_to_process:
    if col_name in df.columns:
        # Count NaNs before coercion (if any already exist from CSV interpretation as empty strings)
        nans_before_coercion = df[col_name].isna().sum()

        # Coerce to numeric. Errors become NaN.
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

        # Count NaNs after coercion.
        nans_after_coercion = df[col_name].isna().sum()

        newly_coerced_to_nan = nans_after_coercion - nans_before_coercion
        if newly_coerced_to_nan > 0:
            logger.info(
                f"Column '{col_name}': {newly_coerced_to_nan} non-numeric values were converted to NaN."
            )
        if nans_after_coercion > 0:
            logger.info(
                f"Column '{col_name}': Total NaNs after coercion (including pre-existing): {nans_after_coercion}."
            )
    else:
        logger.warning(f"Coordinate column '{col_name}' not found for processing.")

# Drop rows where CORD_Y or CORD_X is NaN
rows_before_coord_nan_drop = len(df)
# Create a mask for rows where *either* CORD_Y or CORD_X is NaN
# This assumes both columns should ideally exist. If one might be missing entirely,
# you might want to adjust the subset list based on df.columns.
columns_to_check_for_nan_in_coords = [
    col for col in coordinate_columns_to_process if col in df.columns
]

if (
    columns_to_check_for_nan_in_coords
):  # Proceed only if at least one coordinate column exists
    df.dropna(subset=columns_to_check_for_nan_in_coords, how="any", inplace=True)
    rows_dropped_due_to_coord_nan = rows_before_coord_nan_drop - len(df)
    if rows_dropped_due_to_coord_nan > 0:
        logger.info(
            f"Removed {rows_dropped_due_to_coord_nan} rows because CORD_Y or CORD_X was NaN (invalid or missing)."
        )
    else:
        logger.info("No rows removed due to NaN coordinates.")
else:
    logger.warning("No coordinate columns found to check for NaN values to drop rows.")

logger.info(f"Rows remaining after coordinate cleaning: {len(df)}")

####

# Keep only unique "CTA_CONTR" values
rows_before_cc_drop = len(df)
df.drop_duplicates(subset=["CTA_CONTR"], keep="first", inplace=True)
rows_after_cc_drop = len(df)
logger.info(
    f"Removed {rows_before_cc_drop - rows_after_cc_drop} rows with duplicate 'CTA_CONTR'."
)
# Recalculate unique users *after* this filtering step, before grouping
unique_users_after_cc_drop = df["NUM_IDENT"].nunique()


# ---------------------------
# --- 4. TRANSFORM ----------
# ---------------------------

# Adress Transformation
# Fill NaN with empty strings before joining
df["DIREC2"] = df["DIREC2"].fillna("")
df["DISTRITO"] = df["DISTRITO"].fillna("")

# Join address parts including ", Peru"
df["full_address"] = df[["DIREC2", "DISTRITO"]].agg(
    lambda x: ", ".join(filter(None, x)).strip(", "), axis=1
)
df["full_address"] = df["full_address"].apply(lambda x: f"{x}, Peru" if x else "Peru")

# Drop original address columns
df.drop(columns=["DIREC2", "DISTRITO"], inplace=True)

logger.info("Address transformation complete.")

# Name Transformation
df[["last_name", "first_name"]] = df["NOMBRE"].apply(
    lambda full_name: pd.Series(split_name(full_name))
)
df.drop(columns=["NOMBRE"], inplace=True)

logger.info("Name transformation complete.")

# Email Validation and Cleaning
logger.info("Validating and cleaning email addresses...")
# Ensure CORREO column exists, fill NaNs with empty string for .str accessor, then None if needed
if "CORREO" in df.columns:
    df["CORREO"] = df["CORREO"].fillna("").astype(str)  # Ensure string type for apply
    original_email_count = df[df["CORREO"] != ""].shape[0]  # Count non-empty strings

    # Apply validation, set invalid or empty emails to None
    df["CORREO"] = df["CORREO"].apply(
        lambda x: x if x and is_valid_email_format(x) else None
    )
    valid_email_count = df["CORREO"].count()  # .count() excludes NaNs (None)
    invalid_emails_set_to_none = original_email_count - valid_email_count
    if invalid_emails_set_to_none > 0:
        logger.info(
            f"{invalid_emails_set_to_none} email addresses were invalid or empty and set to None."
        )
else:
    logger.warning("Column 'CORREO' not found for email validation.")
    df["CORREO"] = (
        None  # Create the column as None if it doesn't exist to prevent key errors later
    )


# Initialize a Series to store the final, validated phone numbers
# Start with None, and fill it as we find valid phones
df["final_phone_processed"] = pd.Series([None] * len(df), dtype=object, index=df.index)

processed_count_log = {"attempted_cf": 0, "valid_cf": 0, "attempted_c": 0, "valid_c": 0}

# Attempt 1: Use CELULAR_FINAL if it exists and is valid
if "CELULAR_FINAL" in df.columns:
    logger.info("Processing 'CELULAR_FINAL' column...")
    # Iterate only over rows where final_phone_processed is still None
    # and CELULAR_FINAL is not NaN (to avoid processing NaNs unnecessarily with format_phone)
    mask_try_celular_final = (
        df["final_phone_processed"].isna() & df["CELULAR_FINAL"].notna()
    )
    processed_count_log["attempted_cf"] = mask_try_celular_final.sum()

    # Apply format_phone. It returns None if invalid.
    validated_celular_final = df.loc[mask_try_celular_final, "CELULAR_FINAL"].apply(
        format_phone
    )

    # Assign valid phones from CELULAR_FINAL to our target column
    df.loc[
        mask_try_celular_final & validated_celular_final.notna(),
        "final_phone_processed",
    ] = validated_celular_final[validated_celular_final.notna()]
    processed_count_log["valid_cf"] = validated_celular_final.notna().sum()
    logger.info(
        f"  From 'CELULAR_FINAL': Attempted on {processed_count_log['attempted_cf']} non-empty values, found {processed_count_log['valid_cf']} valid phones."
    )

else:
    logger.warning("Column 'CELULAR_FINAL' not found.")

# Attempt 2: Use CELULAR for rows where CELULAR_FINAL was not found or was invalid
if "CELULAR" in df.columns:
    logger.info("Processing 'CELULAR' column as fallback...")
    # Iterate only over rows where final_phone_processed is still None (meaning CELULAR_FINAL didn't yield a valid phone)
    # and CELULAR is not NaN
    mask_try_celular = df["final_phone_processed"].isna() & df["CELULAR"].notna()
    processed_count_log["attempted_c"] = mask_try_celular.sum()

    if mask_try_celular.any():  # Only apply if there are rows to process
        validated_celular = df.loc[mask_try_celular, "CELULAR"].apply(format_phone)

        # Assign valid phones from CELULAR to our target column
        df.loc[
            mask_try_celular & validated_celular.notna(), "final_phone_processed"
        ] = validated_celular[validated_celular.notna()]
        processed_count_log["valid_c"] = validated_celular.notna().sum()
        logger.info(
            f"  From 'CELULAR' (fallback): Attempted on {processed_count_log['attempted_c']} non-empty values, found {processed_count_log['valid_c']} valid phones."
        )
    else:
        logger.info(
            "  No rows needed fallback processing for 'CELULAR' or 'CELULAR' column is all NaN where fallback was needed."
        )
else:
    logger.warning("Column 'CELULAR' not found for fallback.")

# Now, 'final_phone_processed' contains the result.
# We need to update the column name used by the rest of the script (e.g., aggregate_user_data expects 'CELULAR_FINAL')
# So, we can drop the original 'CELULAR_FINAL' (if it exists) and 'CELULAR' (if it exists and you want to),
# and rename 'final_phone_processed' to 'CELULAR_FINAL'.

if "CELULAR_FINAL" in df.columns:
    df.drop(columns=["CELULAR_FINAL"], inplace=True, errors="ignore")
# Decide if you want to drop the original "CELULAR" column as well
if "CELULAR" in df.columns:
    df.drop(columns=["CELULAR"], inplace=True, errors="ignore")

df.rename(columns={"final_phone_processed": "CELULAR_FINAL"}, inplace=True)

total_valid_phones = df[
    "CELULAR_FINAL"
].count()  # Count non-NaN in the final result column
logger.info(
    f"Total valid phone numbers after processing both fields: {total_valid_phones}."
)

# Reorder columns
intermediate_order = [
    "INTERLOCUTOR",
    "NUM_IDENT",
    "first_name",
    "last_name",
    "CELULAR_FINAL",
    "CORREO",
    "CTA_CONTR",
    "full_address",
    "CORD_Y",
    "CORD_X",
]
# Ensure all columns are included, even if not in the specific order list
current_cols = [col for col in intermediate_order if col in df.columns]
other_cols = [col for col in df.columns if col not in current_cols]
df = df[current_cols + other_cols]


logger.info("\nSample of data before grouping:\n\n")
print(df.head(3))
print("\n\n")

# # Intermediate Checkpoint
# try:
#     df.to_json(CHECKPOINT_FILE, orient="records", indent=4)
# except Exception as e:
#     logger.warning(f"Warning: Failed to save checkpoint file: {e}.\n")


# ---------------------------
# --- 5. GROUP --------------
# ---------------------------

logger.info("Grouping data by 'NUM_IDENT'...")

# Apply grouping and aggregation
grouped_data = (
    df.groupby("NUM_IDENT")
    .apply(aggregate_user_data, include_groups=False)
    .reset_index()
)
unique_user_count = len(grouped_data)
rows_dropped_count = initial_row_count - unique_user_count
logger.info(f"Grouping complete. {len(grouped_data)} unique users found.")


# ---------------------------
# --- 6.Target Schema -------
# ---------------------------

logger.info("Transforming grouped data to target JSON structure...")
transformed_users = []
for _, user in grouped_data.iterrows():
    # Format addresses according to CleanAddress model structure
    clean_addresses = []
    for index, addr in enumerate(user["addresses_raw"]):
        loc_type_value = index if index <= 2 else 2  # loc type can only be 0, 1, or 2
        clean_address = {
            "address": addr["address"],
            "latitude": addr["latitude"],
            "longitude": addr["longitude"],
            "house_no": str(addr["house_no"]),
            "postal_code": str(addr["postal_code"]),
            "loc_type": loc_type_value,
            "id": None,
            "upload_status": None,
        }
        clean_addresses.append(clean_address)

    user_data = {
        "password": str(user["INTERLOCUTOR"]),
        "first_name": user["first_name"],
        "last_name": user["last_name"],
        "email": user["email"],
        "phone_no": user["phone_no"],
        "addresses": clean_addresses,
        "custom_fields": None,  # TODO: No source data for custom fields
        "upload_status": None,
        "customer_id": None,
        "error_message": None,
    }
    transformed_users.append(user_data)


# ---------------------------
# --- 7. SEGMENT ------------
# ---------------------------

logger.info("Segmenting users based on phone/email availability...")
segmented_data = {
    "both": [],
    "phone_only": [],
    "email_only": [],
    "neither": [],
}

for user in transformed_users:
    has_email = user.get("email") is not None and user["email"] != ""
    has_phone = user.get("phone_no") is not None and user["phone_no"] != ""

    if has_email and has_phone:
        segmented_data["both"].append(user)
    elif has_phone:
        segmented_data["phone_only"].append(user)
    elif has_email:
        segmented_data["email_only"].append(user)
    else:
        segmented_data["neither"].append(user)

total_segmented = 0
for key, users in segmented_data.items():
    count = len(users)
    print(f" - {key.replace('_', ' ').title()}: {count} users")
    total_segmented += count

if total_segmented != unique_user_count:
    logger.warning(
        f"Warning: Mismatch between grouped users ({unique_user_count}) and segmented users ({total_segmented}). Check segmentation logic."
    )
else:
    logger.info(f"Total users segmented: {total_segmented}")

# ---------------------------
# --- 8. OUTPUT -------------
# ---------------------------

logger.info("--- Processing Summary ---")

print(f"Initial unique users (NUM_IDENT): {initial_unique_users}.")
print(f"Unique users after 'CTA_CONTR' filter: {unique_users_after_cc_drop}.")
print(f"Final unique users processed (after grouping): {unique_user_count}.")

if unique_user_count < initial_unique_users:
    logger.warning(
        f"-> Note: {initial_unique_users - unique_user_count} unique users were dropped, potentially due to 'CTA_CONTR' filtering removing all their records."
    )
elif unique_users_after_cc_drop < initial_unique_users:
    logger.warning(
        "-> Note: Some users might have lost records due to 'CTA_CONTR' filtering, but all initial unique users are still present."
    )
else:
    logger.info("All initial unique users remain after processing.")

logger.info("Saving segmented data to output files...")
for key, users in segmented_data.items():
    output_filename = OUTPUT_FILES[key]
    num_users = len(users)

    if num_users == 0:
        print(f" - No users for '{key}', skipping file.")
        continue

    print(f" - Saving {num_users} users for '{key}' to '{output_filename}'")
    try:
        with open(output_filename, "w") as f:
            json.dump(users, f, indent=4)
    except Exception as e:
        print(f"ERROR: Failed to save file '{output_filename}': {e}")

logger.info("Processing finished.")
