import pandas as pd
import json
import os

# --- Configuration ---
INPUT_CSV_FILE = "C:/Users/Equipo/Documents/bulk_load_yelo_calidda/data/test_data.csv"
OUTPUT_DIR = "output"
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "intermediate_checkpoint.json")
OUTPUT_FILES = {
    "both": os.path.join(OUTPUT_DIR, "users_phone_email.json"),
    "phone_only": os.path.join(OUTPUT_DIR, "users_only_phone.json"),
    "email_only": os.path.join(OUTPUT_DIR, "users_only_email.json"),
    "neither": os.path.join(OUTPUT_DIR, "users_neither.json"),
}
initial_row_count: int = 0
initial_unique_users: int = 0
rows_before_cc_drop: int = 0
rows_after_cc_drop: int = 0
unique_users_after_cc_drop: int = 0

# --- Helper Functions ---


def split_name(full_name):
    """Splits a full name based on word count."""
    if pd.isna(full_name):
        return pd.NA, pd.NA
    parts = str(full_name).split()
    if len(parts) == 0:
        return "", ""
    elif len(parts) == 1:
        return parts[0], parts[0]
    elif len(parts) == 2:
        return parts[0], parts[1]
    elif len(parts) == 3:
        return parts[0], " ".join(parts[1:])
    else:  # 4 or more
        return " ".join(parts[:2]), " ".join(parts[2:])


def format_phone(phone):
    if pd.isna(phone):
        return None
    try:
        # Convert to int first to remove potential ".0", then format
        phone_str = str(int(phone))
        # Add prefix only if it doesn't already start with it (optional safety check)
        if not phone_str.startswith("+51 "):
            return f"+51 {phone_str}"
        return phone_str  # Already formatted? Return as is.
    except (ValueError, TypeError):
        # Handle cases where phone is not a valid number
        print(
            f"  - Warning: Could not format non-numeric phone '{phone}'. Skipping format."
        )
        return str(phone)  # Return original string representation if conversion fails


def aggregate_user_data(group):
    """Aggregates data for a single user group."""
    # Get first non-null value for single fields
    first_name = (
        group["first_name"].dropna().iloc[0]
        if not group["first_name"].dropna().empty
        else None
    )
    last_name = (
        group["last_name"].dropna().iloc[0]
        if not group["last_name"].dropna().empty
        else None
    )

    # Get unique non-null emails and phones
    emails = group["correo"].dropna().unique()
    phones = group["celular"].dropna().unique()

    # Select the first unique email found
    selected_email = emails[0] if len(emails) > 0 else None
    # Select the first unique formatted phone found
    selected_phone = phones[0] if len(phones) > 0 else None

    # Create list of addresses (simple dict structure for now)
    addresses = []
    # Drop duplicates based on all address fields to avoid identical entries
    for _, row in (
        group[["full_address", "latitud", "longitud", "cuenta_contrato"]]
        .drop_duplicates()
        .iterrows()
    ):
        # Skip if essential address info is missing
        if (
            pd.isna(row["full_address"])
            and pd.isna(row["latitud"])
            and pd.isna(row["longitud"])
        ):
            continue
        addresses.append(
            {
                "address": row["full_address"],
                "latitude": row["latitud"] if pd.notna(row["latitud"]) else None,
                "longitude": row["longitud"] if pd.notna(row["longitud"]) else None,
                "house_no": row["cuenta_contrato"],
            }
        )

    return pd.Series(
        {
            "first_name": first_name,
            "last_name": last_name,
            "email": selected_email,
            "phone_no": selected_phone,
            "addresses_raw": addresses,
        }
    )


# --- Main Processing ---

print("Starting data processing...")

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Output directory '{OUTPUT_DIR}' ensured.")


# ---------------------------
# --- 2. LOAD ---------------
# ---------------------------

try:
    df = pd.read_csv(INPUT_CSV_FILE)
    print(f"Loaded data from '{INPUT_CSV_FILE}'. Shape: {df.shape}")
    print("\nSample of raw data:")
    print(df.head().to_markdown(index=False))
except FileNotFoundError:
    print(f"ERROR: Input file '{INPUT_CSV_FILE}' not found. Exiting.")
    exit()
except Exception as e:
    print(f"ERROR: Failed to load CSV: {e}. Exiting.")
    exit()

initial_row_count = len(df)
initial_unique_users = df["num_document"].nunique()

# ---------------------------
# --- 2. CLEAN --------------
# ---------------------------

# Remove unused columns
columns_to_drop = ["num_interlocutor", "saldo_disponible", "fijo", "NSE"]
df.drop(columns=columns_to_drop, inplace=True, errors="ignore")
print(f"\nDropped columns: {columns_to_drop}")

# Remove NA values


# Keep only unique "cuenta_contrato" values


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
print("\nCombined address columns into 'full_address' and added ', Peru'.")

# Name Transformation
df[["first_name", "last_name"]] = df["apellidos_nombres"].apply(
    lambda full_name: pd.Series(split_name(full_name))
)
df.drop(columns=["apellidos_nombres"], inplace=True)
print("\nSplit 'apellidos_nombres' into 'first_name' and 'last_name'.")

# Phone Numbers transformation
print("\nFormatting phone numbers...")
df["celular"] = df["celular"].apply(format_phone)
print("Phone number formatting applied ('+51 ' prefix).")


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
print("\nReordered columns.")

# 7. Drop rows with duplicate 'cuenta_contrato', keeping the first occurrence
print("\nFiltering based on 'cuenta_contrato'...")
rows_before_cc_drop = len(df)
df.drop_duplicates(subset=["cuenta_contrato"], keep="first", inplace=True)
rows_after_cc_drop = len(df)
print(
    f"Removed {rows_before_cc_drop - rows_after_cc_drop} rows with duplicate 'cuenta_contrato'."
)
# Recalculate unique users *after* this filtering step, before grouping
unique_users_after_cc_drop = df["num_document"].nunique()

print("\nSample of data before grouping:")
print(df.head().to_markdown(index=False))


# 7. Intermediate Checkpoint
try:
    df.to_json(CHECKPOINT_FILE, orient="records", indent=4)
    print(f"\nSaved intermediate checkpoint to '{CHECKPOINT_FILE}'")
except Exception as e:
    print(f"Warning: Failed to save checkpoint file: {e}")


# 8. Group by User (num_document)
print("\nGrouping data by 'num_document'...")

# Apply grouping and aggregation
grouped_data = df.groupby("num_document").apply(aggregate_user_data).reset_index()

unique_user_count = len(grouped_data)
rows_dropped_count = initial_row_count - unique_user_count

print(f"Grouping complete. {len(grouped_data)} unique users found.")
print("\nSample of grouped data:")
print(grouped_data.head().to_markdown(index=False))


# 9. Transform to Target Schema (`CleanUserData` structure)
print("\nTransforming grouped data to target JSON structure...")
transformed_users = []
for _, user in grouped_data.iterrows():
    # Format addresses according to CleanAddress model structure
    clean_addresses = []
    for idx, addr in enumerate(user["addresses_raw"]):
        clean_addresses.append(
            {
                "address": addr["address"],
                "latitude": addr["latitude"],
                "longitude": addr["longitude"],
                "house_no": str(addr["house_no"]),
                "id": None,  # Placeholder
                "upload_status": None,  # Placeholder
            }
        )

    user_data = {
        "password": str(user["num_document"]),  # Ensure password is a string
        "first_name": user["first_name"],
        "last_name": user["last_name"],
        "email": user["email"],
        "phone_no": user["phone_no"],
        "addresses": clean_addresses,
        "custom_fields": None,  # No source data for custom fields
        "upload_status": None,  # Placeholder
        "customer_id": None,  # Placeholder
        "error_message": None,  # Placeholder
    }
    transformed_users.append(user_data)

print(f"Transformation complete. {len(transformed_users)} user records created.")
if transformed_users:
    print("\nSample of first transformed user:")
    print(json.dumps(transformed_users[0], indent=2))


# 10. Segment Data
print("\nSegmenting users based on phone/email availability...")
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

print("Segmentation complete:")
for key, users in segmented_data.items():
    print(f" - {key.replace('_', ' ').title()}: {len(users)} users")


# 11. Final Report and Output Files
print("\n--- Processing Summary ---")

final_unique_users = unique_user_count  # Already calculated after grouping

print(f"Initial unique users (num_document): {initial_unique_users}")
print(f"Unique users after 'cuenta_contrato' filter: {unique_users_after_cc_drop}")
print(f"Final unique users processed (after grouping): {final_unique_users}")

if final_unique_users < initial_unique_users:
    print(
        f"-> Note: {initial_unique_users - final_unique_users} unique users were dropped, potentially due to 'cuenta_contrato' filtering removing all their records."
    )
elif unique_users_after_cc_drop < initial_unique_users:
    print(
        "-> Note: Some users might have lost records due to 'cuenta_contrato' filtering, but all initial unique users are still present."
    )
else:
    print("-> All initial unique users remain after processing.")

print("\nUser Segmentation Results:")
total_segmented = 0
for key, users in segmented_data.items():
    count = len(users)
    print(f" - {key.replace('_', ' ').title()}: {count} users")
    total_segmented += count

if total_segmented != unique_user_count:
    print(
        f"Warning: Mismatch between grouped users ({unique_user_count}) and segmented users ({total_segmented}). Check segmentation logic."
    )
else:
    print(f"Total users segmented: {total_segmented}")

print("\nSaving segmented data to output files...")
for key, users in segmented_data.items():
    output_filename = OUTPUT_FILES[key]
    num_users = len(users)

    if num_users == 0:
        print(f" - No users for '{key}', skipping file.")
        continue

    print(f" - Saving {num_users} users for '{key}' to '{output_filename}'")
    try:
        with open(output_filename, "w") as f:
            # Dump the entire list of users for this category into one file
            json.dump(users, f, indent=4)
    except Exception as e:
        print(f"ERROR: Failed to save file '{output_filename}': {e}")

print("\nProcessing finished.")
