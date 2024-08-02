import os
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

def get_env_var(var_name):
    """Get an environment variable and raise an error if not set."""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable {var_name} is not set.")
    return value

def initialize_databricks_client(team_name):
    """Initialize Databricks client for a given team."""
    if team_name == 'ds':
        host = get_env_var("DATABRICKS_HOST_WORKSPACE_DS")
    elif team_name == 'mle':
        host = get_env_var("DATABRICKS_HOST_WORKSPACE_MLE")
    else:
        raise ValueError(f"Invalid team name: {team_name}. Expected 'ds' or 'mle'.")

    client_id = get_env_var("DATABRICKS_CLIENT_ID")
    client_secret = get_env_var("DATABRICKS_CLIENT_SECRET")

    config = Config(
        host=host,
        client_id=client_id,
        client_secret=client_secret
    )

    # Log configuration details
    logger.info("Host: %s", config.host)
    #logger.info("Client ID: %s", config.client_id)

    return WorkspaceClient(config=config)

# Optionally, log or handle other initialization specifics
logger.info("Databricks client initialized successfully.")
