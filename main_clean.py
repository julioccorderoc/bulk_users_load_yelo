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
from src.cleaning import split_name, format_phone, aggregate_user_data


# --- Environment Variables ---
load_dotenv()
RAW_DATA_FILE_NAME = os.getenv("RAW_DATA_FILE_NAME")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR")
CLEAN_DATA_FILE_NAME = os.getenv("CLEAN_DATA_FILE_NAME")


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
logger.info("Starting data processing...")
os.makedirs(CLEAN_DATA_DIR, exist_ok=True)


# ---------------------------
# --- 2. LOAD ---------------
# ---------------------------

try:
    df = pd.read_csv(FILE_DIR)
    logger.info(f"Shape: {df.shape}")
except FileNotFoundError:
    logger.info(f"ERROR: Input file '{FILE_DIR}' not found. Exiting.")
    exit()
except Exception as e:
    logger.info(f"ERROR: Failed to load CSV: {e}. Exiting.")
    exit()

initial_row_count = len(df)
initial_unique_users = df["num_document"].nunique()

# ---------------------------
# --- 2. CLEAN --------------
# ---------------------------

# Remove unused columns
columns_to_drop = ["num_interlocutor", "saldo_disponible", "fijo", "NSE"]
df.drop(columns=columns_to_drop, inplace=True, errors="ignore")

# Remove NA values


# Keep only unique "cuenta_contrato" values
rows_before_cc_drop = len(df)
df.drop_duplicates(subset=["cuenta_contrato"], keep="first", inplace=True)
rows_after_cc_drop = len(df)
logger.info(
    f"Removed {rows_before_cc_drop - rows_after_cc_drop} rows with duplicate 'cuenta_contrato'."
)
# Recalculate unique users *after* this filtering step, before grouping
unique_users_after_cc_drop = df["num_document"].nunique()


# ---------------------------
# --- 3. TRANSFORM ----------
# ---------------------------

# Adress Transformation
# Fill NaN with empty strings before joining
df["direccion"] = df["direccion"].fillna("")
df["distrito"] = df["distrito"].fillna("")

# Join address parts including ", Peru"
df["full_address"] = df[["direccion", "distrito"]].agg(
    lambda x: ", ".join(filter(None, x)).strip(", "), axis=1
)
df["full_address"] = df["full_address"].apply(lambda x: f"{x}, Peru" if x else "Peru")

# Drop original address columns
df.drop(columns=["direccion", "distrito"], inplace=True)

# Name Transformation
df[["first_name", "last_name"]] = df["apellidos_nombres"].apply(
    lambda full_name: pd.Series(split_name(full_name))
)
df.drop(columns=["apellidos_nombres"], inplace=True)

# Phone Numbers transformation
df["celular"] = df["celular"].apply(format_phone)

# Reorder columns
intermediate_order = [
    "num_document",
    "first_name",
    "last_name",
    "celular",
    "correo",
    "cuenta_contrato",
    "full_address",
    "latitud",
    "longitud",
]
# Ensure all columns are included, even if not in the specific order list
current_cols = [col for col in intermediate_order if col in df.columns]
other_cols = [col for col in df.columns if col not in current_cols]
df = df[current_cols + other_cols]


logger.info("\nSample of data before grouping:\n\n")
print(df.head(3))
print("\n\n")

# 7. Intermediate Checkpoint
try:
    df.to_json(CHECKPOINT_FILE, orient="records", indent=4)
except Exception as e:
    logger.warning(f"Warning: Failed to save checkpoint file: {e}.\n")


# ---------------------------
# --- 4. GROUP --------------
# ---------------------------

logger.info("Grouping data by 'num_document'...")

# Apply grouping and aggregation
grouped_data = df.groupby("num_document").apply(aggregate_user_data).reset_index()
unique_user_count = len(grouped_data)
rows_dropped_count = initial_row_count - unique_user_count
logger.info(f"Grouping complete. {len(grouped_data)} unique users found.")


# ---------------------------
# --- 5.Target Schema -------
# ---------------------------

logger.info("Transforming grouped data to target JSON structure...")
transformed_users = []
for _, user in grouped_data.iterrows():
    # Format addresses according to CleanAddress model structure
    clean_addresses = []
    for idx, addr in enumerate(user["addresses_raw"]):
        clean_address = {
            "address": addr["address"],
            "latitude": addr["latitude"],
            "longitude": addr["longitude"],
            "house_no": str(addr["house_no"]),
            "loc_type": idx,
            "id": None,
            "upload_status": None,
        }
        clean_addresses.append(clean_address)

    user_data = {
        "password": str(user["num_document"]),
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
# --- 6. SEGMENT ------------
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
# --- 7. OUTPUT -------------
# ---------------------------

logger.info("--- Processing Summary ---")

print(f"Initial unique users (num_document): {initial_unique_users}.")
print(f"Unique users after 'cuenta_contrato' filter: {unique_users_after_cc_drop}.")
print(f"Final unique users processed (after grouping): {unique_user_count}.")

if unique_user_count < initial_unique_users:
    logger.warning(
        f"-> Note: {initial_unique_users - unique_user_count} unique users were dropped, potentially due to 'cuenta_contrato' filtering removing all their records."
    )
elif unique_users_after_cc_drop < initial_unique_users:
    logger.warning(
        "-> Note: Some users might have lost records due to 'cuenta_contrato' filtering, but all initial unique users are still present."
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
