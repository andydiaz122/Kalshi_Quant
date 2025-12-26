"""
QETE Authentication Utilities

Centralized authentication logic for Kalshi API access.
Provides functions for loading credentials and creating authenticated clients.
"""

from pathlib import Path
from typing import Union

from kalshi_python import ApiClient, Configuration


def load_private_key_pem(key_file_path: Union[str, Path]) -> str:
    """
    Load RSA private key from PEM file and return as string.
    
    Args:
        key_file_path: Path to the RSA private key file in PEM format.
                       Can be string or Path object.
        
    Returns:
        Private key content as string.
        
    Raises:
        FileNotFoundError: If the key file doesn't exist.
        ValueError: If the key file cannot be read or parsed.
    
    Example:
        >>> private_key = load_private_key_pem("my_api_key.key")
        >>> print(private_key[:27])
        '-----BEGIN RSA PRIVATE KEY'
    """
    key_path = Path(key_file_path) if isinstance(key_file_path, str) else key_file_path
    
    if not key_path.exists():
        raise FileNotFoundError(
            f"Could not find key file '{key_path}'.\n"
            f"Please ensure the key file exists at the specified path."
        )
    
    try:
        with open(key_path, "r", encoding="utf-8") as key_file:
            private_key_pem = key_file.read()
        
        # Basic validation - ensure it looks like a PEM key
        if "-----BEGIN" not in private_key_pem:
            raise ValueError(
                f"Key file '{key_path}' does not appear to be a valid PEM format."
            )
        
        return private_key_pem
        
    except IOError as e:
        raise ValueError(
            f"Failed to read key file '{key_path}': {e}"
        ) from e


def create_authenticated_client(
    key_id: str,
    key_file_path: Union[str, Path]
) -> ApiClient:
    """
    Create and return an authenticated Kalshi API client.
    
    This function handles loading the private key and initializing
    the API client with proper authentication.
    
    Args:
        key_id: Kalshi API Key ID (UUID format).
        key_file_path: Path to the RSA private key file in PEM format.
        
    Returns:
        Authenticated ApiClient ready for API calls.
        
    Raises:
        FileNotFoundError: If the key file doesn't exist.
        ValueError: If credentials are invalid.
        
    Example:
        >>> from kalshi_qete.config import KEY_ID, KEY_FILE_PATH
        >>> client = create_authenticated_client(KEY_ID, KEY_FILE_PATH)
    """
    # Validate key file exists
    key_path = Path(key_file_path) if isinstance(key_file_path, str) else key_file_path
    if not key_path.exists():
        raise FileNotFoundError(f"Key file not found: {key_path}")
    
    # Create client with authentication
    config = Configuration()
    client = ApiClient(configuration=config)
    
    # SDK expects file path for authentication
    client.set_kalshi_auth(key_id, str(key_path))
    
    return client


def validate_credentials(key_id: str, key_file_path: Union[str, Path]) -> bool:
    """
    Validate that API credentials are properly configured.
    
    Args:
        key_id: Kalshi API Key ID to validate.
        key_file_path: Path to the key file to validate.
        
    Returns:
        True if credentials appear valid, False otherwise.
    """
    # Check key_id format (should be UUID-like)
    if not key_id or len(key_id) < 10:
        return False
    
    # Check key file exists and is readable
    key_path = Path(key_file_path) if isinstance(key_file_path, str) else key_file_path
    if not key_path.exists():
        return False
    
    try:
        private_key = load_private_key_pem(key_path)
        return "-----BEGIN" in private_key
    except (FileNotFoundError, ValueError):
        return False

