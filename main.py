import os
import json
import logging
import argparse
from clusters import create_cluster_with_tags, list_clusters, terminate_cluster
from databricks_client_config import initialize_databricks_client
from databricks.sdk.config import Config
from databricks.sdk import WorkspaceClient

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def initialize_databricks_client(team_name):
    """Initialize Databricks client for a given team."""
    if team_name == 'ds':
        host = os.getenv("DATABRICKS_HOST_WORKSPACE_DS")
    elif team_name == 'mle':
        host = os.getenv("DATABRICKS_HOST_WORKSPACE_MLE")
    else:
        raise ValueError(f"Invalid team name: {team_name}. Expected 'ds' or 'mle'.")

    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    account_id = os.getenv("DATABRICKS_ACCOUNT_ID")

    config = Config(
        host=host,
        client_id=client_id,
        client_secret=client_secret,
        account_id=account_id
    )

    # Log configuration details
    logger.info("Host: %s", config.host)
    #logger.info("Client ID: %s", config.client_id)

    return WorkspaceClient(config=config)

def load_json_from_file(file_path):
    """Load JSON configuration from a file."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.error("The JSON file could not be found: %s. Please ensure the file exists and try again.", file_path)
        raise
    except json.JSONDecodeError as e:
        logger.error("Failed to decode JSON file %s: %s", file_path, e)
        raise
    except Exception as e:
        logger.error("An error occurred while loading JSON from file %s: %s", file_path, e)
        raise

def create_environment(w, team_name, multi_node_enabled):
    """Create Databricks environment."""
    config_dir = f'json_files/{team_name}'
    config_path_for_cluster_config = os.path.join(config_dir, 'multi_node_cluster_config.json' if multi_node_enabled else 'single_node_cluster_config.json')
    config_path_for_policy_config = os.path.join(config_dir, 'policy_definition.json')
    try:
        config_for_cluster = load_json_from_file(config_path_for_cluster_config)
        config_for_policy = load_json_from_file(config_path_for_policy_config)
        create_cluster_with_tags(w, config_for_cluster, config_for_policy, multi_node_enabled)
        logger.info("%s environment created successfully.", team_name.capitalize().replace("_", " "))
    except Exception as e:
        logger.error("An error occurred while creating the %s environment: %s", team_name.capitalize().replace("_", " "), e)
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Databricks environment.")
    parser.add_argument('team_name', type=str, help='Team name (e.g., ds or mle)')
    parser.add_argument('--multi_node_enabled', action='store_true', help='Enable multi-node cluster configuration')

    args = parser.parse_args()

    try:
        w = initialize_databricks_client(args.team_name)
        create_environment(w, args.team_name, args.multi_node_enabled)

        # List all clusters
        # list_clusters()

    except Exception as e:
        logger.error("An error occurred in the main execution: %s", e)
        raise