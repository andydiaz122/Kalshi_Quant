"""
Authentication utilities for Kalshi API.
"""
from pathlib import Path


def load_private_key_pem(key_file_path: Path) -> str:
    """
    Load RSA private key from PEM file and return as string.
    
    Args:
        key_file_path: Path to the RSA private key file in PEM format
        
    Returns:
        Private key content as string
        
    Raises:
        FileNotFoundError: If the key file doesn't exist
        ValueError: If the key file cannot be parsed
    """
    if not key_file_path.exists():
        raise FileNotFoundError(
            f"❌ ERROR: Could not find key file '{key_file_path}' in the current directory.\n"
            f"   -> Please ensure 'kalshi.key' is in the project root directory."
        )
    
    try:
        with open(key_file_path, "r", encoding="utf-8") as key_file:
            private_key_pem = key_file.read()
        return private_key_pem
    except Exception as e:
        raise ValueError(
            f"❌ ERROR: Failed to read key file '{key_file_path}': {e}"
        ) from e

