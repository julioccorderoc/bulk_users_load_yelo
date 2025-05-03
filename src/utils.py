import logging

yelo_headers: dict[str, str] = {"Content-Type": "application/json"}

generic_headers: dict[str, str] = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# --- Logger Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s",
)
logger = logging.getLogger(__name__)
