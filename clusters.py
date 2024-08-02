import os
import time
import logging
import json


# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def create_or_get_policy(w,policy_name, config_policy):
    """Create or retrieve a cluster policy."""
    try:
        
        # Check if policy already exists
        policies = w.cluster_policies.list()
        logger.info("policies=%s", policies)
        for policy in policies:
            logger.info("policy.name =%s policy_name=%s", policy.name, policy_name)
            if policy.name == policy_name:
                logger.info("Policy already exists with name: %s and id: %s", policy.name, policy.policy_id)
                return policy.policy_id

        # Convert the policy definition to a JSON string
        policy_definition_json = json.dumps(config_policy)

        # Create new policy
        created_policy = w.cluster_policies.create(
            name=policy_name,
            definition=policy_definition_json
        ).result()
        logger.info("Policy created with name: %s and id: %s", policy_name, created_policy.policy_id)
        return created_policy.policy_id
    except Exception as e:
        logger.error("An error occurred while creating or retrieving the policy: %s", e)
        raise
    
def load_json_config(file_path):
    """Load JSON configuration from a file."""
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        logger.error("The JSON configuration file could not be found: %s. Please ensure the file exists and try again.", file_path)
        raise
    except Exception as e:
        logger.error("An error occurred while loading the JSON configuration from file %s: %s", file_path, e)
        raise

def install_library(cluster_id, package_name):
    """Install a library on a specified cluster."""
    try:
        # Define library configuration in the correct format
        library_config = {
            'cluster_id': cluster_id,
            'libraries': [
                {
                    'pypi': {
                        'package': package_name
                    }
                }
            ]
        }

        # Ensure the format is correct
        response = w.libraries.install(
            cluster_id=library_config['cluster_id'],
            libraries=library_config['libraries']
        ).result()

        logger.info("Library '%s' installed on cluster '%s'", package_name, cluster_id)
    except Exception as e:
        logger.error("An error occurred while installing the library '%s' on cluster '%s': %s", package_name, cluster_id, e)
        raise


def create_cluster_with_tags(w,cluster_config, config_policy, multi_node_enabled):
    """Create a Databricks cluster based on the configuration file."""
    try:
        # Define cluster name
        cluster_name = cluster_config.get("cluster_name", f'sdk-{time.time_ns()}')
        logger.debug("Cluster name determined: %s", cluster_name)

        # Prepare cluster configuration
        spark_version = cluster_config["spark_version"]
        node_type_id = cluster_config["node_type_id"]
        autotermination_minutes = cluster_config["autotermination_minutes"]
        autoscale = cluster_config.get("autoscale",{})
        enable_elastic_disk = cluster_config["enable_elastic_disk"]
        num_workers = cluster_config["num_workers"]
        spark_conf = cluster_config.get("spark_conf", {})
        spark_env_vars = cluster_config.get("spark_env_vars", {})
        custom_tags = cluster_config.get("custom_tags", {})
        aws_attributes = cluster_config.get("aws_attributes", {})
        policy_name = f'{cluster_name}-policy'

        # Create or get policy
        policy_id = create_or_get_policy(w,policy_name, config_policy)

        logger.debug("Cluster configuration prepared: %s", {
            "spark_version": spark_version,
            "node_type_id": node_type_id,
            "num_workers": num_workers,
            "spark_conf": spark_conf,
            "custom_tags": custom_tags,
            "aws_attributes": aws_attributes,
            "policy_id": policy_id
        })

        # Create cluster with specified configuration and enforce policy
        if multi_node_enabled:
            response = w.clusters.create(
                cluster_name=cluster_name,
                autoscale=autoscale,
                spark_version=spark_version,
                node_type_id=node_type_id,
                autotermination_minutes=autotermination_minutes,
                enable_elastic_disk=enable_elastic_disk,
                num_workers=num_workers,
                spark_conf=spark_conf,
                spark_env_vars=spark_env_vars,
                custom_tags=custom_tags,
                aws_attributes=aws_attributes,
                policy_id=policy_id
            ).result()
        else:
            response = w.clusters.create(
                cluster_name=cluster_name,
                spark_version=spark_version,
                node_type_id=node_type_id,
                autotermination_minutes=autotermination_minutes,
                enable_elastic_disk=enable_elastic_disk,
                num_workers=num_workers,
                spark_conf=spark_conf,
                spark_env_vars=spark_env_vars,
                custom_tags=custom_tags,
                policy_id=policy_id
            ).result()

        cluster_id = response.cluster_id
        logger.debug("Cluster creation response: %s", response)

        # Install libraries on the newly created cluster
        # Example package
        # install_library(cluster_id, 'requests')

    except KeyError as e:
        logger.error("A KeyError occurred while processing the cluster configuration: %s", e)
        raise
    except ValueError as e:
        logger.error("A ValueError occurred: %s", e)
        raise
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)
        raise


def list_clusters():
    """List all Databricks clusters."""
    try:
        clusters = w.clusters.list()  # Fetch the list of clusters
        for cluster in clusters:
            # Access attributes directly, assuming cluster is of type ClusterDetails
            cluster_id = cluster.cluster_id
            cluster_name = cluster.cluster_name
            logger.info("Cluster ID: %s, Name: %s", cluster_id, cluster_name)
        return cluster_id
    except AttributeError as e:
        logger.error("AttributeError occurred while processing cluster details: %s", e)
        raise
    except Exception as e:
        logger.error("An error occurred while listing clusters: %s", e)
        raise


def terminate_cluster(cluster_id):
    """Terminate a specified Databricks cluster."""
    try:
        w.clusters.delete(cluster_id=cluster_id)
        logger.info("Cluster %s terminated successfully", cluster_id)
    except Exception as e:
        logger.error("An error occurred while terminating the cluster %s: %s", cluster_id, e)
        raise

