import argparse
import yaml
import os


def get_argsparser():
    parser = argparse.ArgumentParser()

    parser.add_argument("pipeline_config", help="name of the pipleline configuration file")

    parser.add_argument("--debug", help = "turn on debugging", action="store_true")

    return parser

def parse_config(config_path):
    with open(config_path) as input_stream:
        try:
            config = yaml.safe_load(input_stream)
        except yaml.YAMLError as exc:
            raise exc

    return config or None


def create_workspace_folder(workspace_path, delete = False):

    if os.path.exists(workspace_path):
        print("the workspace directory already exist.")
        if delete:
            print("Deleting and creating agian")
            os.rmdir(workspace_path)
            os.mkdir(workspace_path)  
    else:
        print(f"creating {workspace_path}")
        os.mkdir(workspace_path)        
    
    return True

  
