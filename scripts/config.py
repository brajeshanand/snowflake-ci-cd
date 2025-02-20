import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

# Load Snowflake connection details from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "CI_CD_USER")  # Default user if env var is not set
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "rw41886.eu-west-1")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "SANDBOX_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DEV_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "CI_CD_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "DEV_ROLE")
PRIVATE_KEY_PASSPHRASE = os.getenv("PRIVATE_KEY_PASSPHRASE")  # Optional passphrase

# Set the default path to the private key for local development
DEFAULT_PRIVATE_KEY_PATH = "/Users/brajeshanand/my_venv01/venv/snowflake_rsa_key.pem"

# Function to load private key from a file
def load_private_key_from_file(key_path, passphrase=None):
    """
    Load the private key from a file.
    :param key_path: Path to the private key file.
    :param passphrase: Passphrase for the private key (if applicable).
    :return: Private key bytes in DER format.
    """
    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return private_key_bytes

# Function to load private key from an environment variable
def load_private_key_from_env(private_key_env_var, passphrase=None):
    """
    Load the private key from an environment variable.
    :param private_key_env_var: Environment variable containing the private key in PEM format.
    :param passphrase: Passphrase for the private key (if applicable).
    :return: Private key bytes in DER format.
    """
    private_key_pem = os.environ.get(private_key_env_var)
    if not private_key_pem:
        raise FileNotFoundError(f"Environment variable {private_key_env_var} not found.")
    
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return private_key_bytes

# Determine the source of the private key
PRIVATE_KEY_SOURCE = os.getenv("PRIVATE_KEY_SOURCE", "LOCAL")  # Defaults to "LOCAL"

if PRIVATE_KEY_SOURCE == "GITHUB":  # Use GitHub Actions Secrets
    PRIVATE_KEY_ENV_VAR = "SNOWFLAKE_PRIVATE_KEY"
    PRIVATE_KEY_BYTES = load_private_key_from_env(
        private_key_env_var=PRIVATE_KEY_ENV_VAR,
        passphrase=PRIVATE_KEY_PASSPHRASE
    )
else:  # Use local file for development
    PRIVATE_KEY_BYTES = load_private_key_from_file(
        key_path=DEFAULT_PRIVATE_KEY_PATH,
        passphrase=PRIVATE_KEY_PASSPHRASE
    )

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    "user": SNOWFLAKE_USER,
    "account": SNOWFLAKE_ACCOUNT,
    "private_key": PRIVATE_KEY_BYTES,
    "warehouse": SNOWFLAKE_WAREHOUSE,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA,
    "role": SNOWFLAKE_ROLE
}

def test_connection():
    """
    Test the Snowflake connection and print the current timestamp.
    """
    try:
        print("Testing Snowflake connection...")
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_TIMESTAMP;")
        print(f"Connection successful! Current timestamp: {cur.fetchone()[0]}")
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# Uncomment the below line to test the connection directly
# test_connection()

