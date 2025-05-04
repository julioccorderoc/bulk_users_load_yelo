import pandas as pd

from src.utils import logger


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
        phone_str = str(int(phone))
        if not phone_str.startswith("+51 "):
            return f"+51 {phone_str}"
        return phone_str
    except (ValueError, TypeError):
        logger.warning(
            f"  - Warning: Could not format non-numeric phone '{phone}'. Skipping format."
        )
        return str(phone)


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
