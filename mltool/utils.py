import argparse
import yaml
import os

import logging

logger = logging.getLogger(__name__)

def parse_command_line_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Execute steps based on configurations and handle reruns.")
    parser.add_argument('pipeline_config', type=str, help="Path to YAML file containing step configurations.")
    parser.add_argument('--rerun', action='store_true', help="Flag to indicate rerun mode.")
    parser.add_argument('--run-id', type=str, default=None, help="ID for rerun mode to use existing outputs.")
    args =  parser.parse_args()

    return args.pipeline_config, args.rerun, args.run_id

def parse_yaml_config(config_path):
    
    """Parse Yaml configuration files"""
    with open(config_path) as input_stream:
        try:
            config = yaml.safe_load(input_stream)
        except yaml.YAMLError as exc:
            raise exc

    return config

def create_workspace_folder(workspace_path, delete_if_exist = False):

    if os.path.exists(workspace_path):
        logger.info(f"the workspace directory already exist: {workspace_path}")
        if delete_if_exist:
            logger.info("Deleting and creating agian")
            os.rmdir(workspace_path)
            os.mkdir(workspace_path)  
    else:
        logger.info(f"creating {workspace_path}")
        os.mkdir(workspace_path)        
    
    return True

  
def write_file(file_path, content):
    with open(file_path, 'w') as file:
            file.write(content)