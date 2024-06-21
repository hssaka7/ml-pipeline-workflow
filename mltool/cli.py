import logging
import logging.config
import os
import sys

from dotenv import load_dotenv

from mltool.pipeline import Pipeline
from mltool.utils import parse_command_line_args,parse_yaml_config, create_workspace_folder

#TODO Generic steps: read from dropzone, write to dropzone, read / write to db tables/ S3
#TODO parallel processing
#TODO add a rerun capabilities
#TODO Mlflow integration in traning session
#TODO add capability to import steps and execute a pipeline
#TODO Add metadata to workspace, make workspace to read all the inputs in the filestate, rather than returning
##### all the returns are saved as a file in file space and recorded to metadata as returns.


# CICID
#TODO seperate pipeline completely. Pipeline should not be inside the package module
#TODO docker file
#TODO add test cases
#TODO convert this to  full fleged cli tool
#TODO upload to PYPI: complete packaging, add author name, description and readme and docs
##### add github worflow to load it to pypi

# loading environment variable
load_dotenv()
WORKSPACE = os.getenv("WORKSPACE")

# sys.path.append(os.getcwd())

# loggging
LOGGER_CONFIG_PATH = os.path.join(os.getcwd(),  "logger_config.yaml")
logging_config = parse_yaml_config(LOGGER_CONFIG_PATH)
logging.config.dictConfig(logging_config)


logger = logging.getLogger('main')



def execute_step(step_func,config):

    step_name = config['name']
    is_class = bool(config.get('class_name', None))
   
    try:
        if not is_class: 
            result = step_func(**config)
        else:
            obj = step_func(**config)
            result = obj.run()
        
        return result
    except Exception as e:
        logger.error(f"Error executing step '{step_name}': {e}")
        return dict()
    

def start():

    pipeline_config_path, is_rerun, run_id, *_ = parse_command_line_args()

    print(pipeline_config_path)
    print(is_rerun)
    print(run_id)
    

    # TODO manage rerun here, call pipeline class with rerun argumnent
    
    # process the pipeline config and get the execution order and steps.
    pipeline_config=parse_yaml_config(pipeline_config_path)
    pipeline = Pipeline(pipeline_config)
    steps_to_execute = pipeline.get_steps_to_execute()


    results = dict()
    step_references = steps_to_execute['step_reference']
    
    # TODO take the parallel order instead of linear oreder
    for step_name in steps_to_execute['linear_order']:
           
            # worspace for each steps
            step_workspace = os.path.join(pipeline.run_workspace, step_name)
            create_workspace_folder(step_workspace, delete_if_exist = True)
            
            step_ref = step_references[step_name]

            step_ref['workspace'] = step_workspace
            step_ref['inputs'] = [results[_sn] for _sn in step_ref['depends']] 
            
            # TODO not to pass step_moudule inside the config
            step_func = step_ref['step_module']

            results[step_name] =  execute_step(step_func, step_ref)


    logger.info("Ending run")
        
    
if __name__ == '__main__':
     start()

