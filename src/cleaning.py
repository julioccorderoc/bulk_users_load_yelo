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
    emails = group["CORREO"].dropna().unique()
    phones = group["CELULAR_FINAL"].dropna().unique()

    # Select the first unique email found
    selected_email = emails[0] if len(emails) > 0 else None
    # Select the first unique formatted phone found
    selected_phone = phones[0] if len(phones) > 0 else None

    # Get the NUM_IDENT value for the user
    user_num_ident = str(group.name)

    # Create list of addresses (simple dict structure for now)
    addresses = []
    # Drop duplicates based on all address fields to avoid identical entries
    for _, row in (
        group[["full_address", "CORD_Y", "CORD_X", "CTA_CONTR"]]
        .drop_duplicates()
        .iterrows()
    ):
        # Skip if essential address info is missing
        if (
            pd.isna(row["full_address"])
            and pd.isna(row["CORD_Y"])
            and pd.isna(row["CORD_X"])
        ):
            continue
        addresses.append(
            {
                "address": row["full_address"],
                "latitude": row["CORD_Y"] if pd.notna(row["CORD_Y"]) else None,
                "longitude": row["CORD_X"] if pd.notna(row["CORD_X"]) else None,
                "house_no": row["CTA_CONTR"],
                "postal_code": user_num_ident,
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
