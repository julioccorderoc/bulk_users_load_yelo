import re

import pandas as pd


def is_valid_peruvian_mobile_format(phone_number_str: str) -> bool:
    """
    Validates if a string represents a Peruvian mobile number format.
    - Must be 9 digits.
    - Must start with '9'.
    """
    if phone_number_str is None:
        return False
    # Check if it's all digits, 9 characters long, and starts with '9'
    return (
        phone_number_str.isdigit()
        and len(phone_number_str) == 9
        and phone_number_str.startswith("9")
    )


def is_valid_email_format(email: str) -> bool:
    """
    Validates email format with specific length constraints:
    - Local part (before @): min 5 characters.
    - Domain part (immediately before TLD): min 3 characters.
    - Top-level domain (after last .): min 2 characters.
    """
    if email is None:
        return False
    # Regex breakdown:
    # ^[a-zA-Z0-9._%+-]{5,}      # Local part: 5+ chars
    # @                           # Separator
    # (?:[a-zA-Z0-9-]+\.)*       # Optional subdomains (e.g., sub.domain.) - non-capturing group
    # ([a-zA-Z0-9-]{3,})         # Domain name part (before TLD): 3+ chars
    # \.                          # Dot before TLD
    # [a-zA-Z]{2,}                # TLD: 2+ letters
    # $                           # End of string
    pattern = (
        r"^[a-zA-Z0-9._%+-]{5,}@(?:[a-zA-Z0-9-]+\.)*([a-zA-Z0-9-]{3,})\.[a-zA-Z]{2,}$"
    )
    return bool(re.match(pattern, email))


# --- Modify existing format_phone function ---


def format_phone(phone):  # Keep existing signature
    if pd.isna(phone):
        return None

    raw_phone_str = ""
    try:
        # Attempt to convert to int then str to handle potential floats like "987654321.0"
        raw_phone_str = str(int(float(phone)))
    except (ValueError, TypeError):
        # If it's not a number that can be cleanly converted to int (e.g., already has non-digits)
        raw_phone_str = str(phone).strip()  # Use it as is, stripped

    if not is_valid_peruvian_mobile_format(raw_phone_str):
        return None

    # If valid, format with prefix
    # No need to check startswith("+51 ") here if validation ensures it's just the 9 digits
    return f"+51 {raw_phone_str}"


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


def aggregate_user_data(group) -> pd.Series:
    """
    Aggregates data for a single user group. Optimized to avoid .iterrows().
    """
    # 1. Get User Identifier
    user_num_ident = str(
        group.name
    )  # group.name holds the value of 'NUM_IDENT' for this group

    # 2. Extract Single Fields (keeping original logic for exact behavior match)
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

    emails = group["CORREO"].dropna().unique()
    selected_email = emails[0] if len(emails) > 0 else None

    phones = group["CELULAR_FINAL"].dropna().unique()
    selected_phone = phones[0] if len(phones) > 0 else None

    # 3. Create addresses_raw List (Optimized)
    address_defining_columns = ["full_address", "CORD_Y", "CORD_X", "CTA_CONTR"]

    # Get unique address definitions for this user
    # Using .copy() to avoid potential SettingWithCopyWarning if any modifications were planned on unique_addresses_df,
    # though not strictly necessary if only reading from it for to_dict.
    unique_addresses_df = group[address_defining_columns].drop_duplicates().copy()

    addresses = []
    if not unique_addresses_df.empty:
        # Convert the DataFrame of unique addresses directly to a list of Python dictionaries
        address_records = unique_addresses_df.to_dict(orient="records")

        # Use a list comprehension to transform these records into the desired final structure
        addresses = [
            {
                "address": record.get(
                    "full_address"
                ),  # .get() is safer if column might be missing, though unlikely here
                "latitude": record.get("CORD_Y")
                if pd.notna(record.get("CORD_Y"))
                else None,
                "longitude": record.get("CORD_X")
                if pd.notna(record.get("CORD_X"))
                else None,
                "house_no": str(record.get("CTA_CONTR"))
                if pd.notna(record.get("CTA_CONTR"))
                else None,
                "postal_code": user_num_ident,  # NUM_IDENT of the user for all their addresses
            }
            for record in address_records
        ]

    return pd.Series(
        {
            "INTERLOCUTOR": group["INTERLOCUTOR"].iloc[0],
            "first_name": first_name,
            "last_name": last_name,
            "email": selected_email,
            "phone_no": selected_phone,
            "addresses_raw": addresses,
        }
    )
