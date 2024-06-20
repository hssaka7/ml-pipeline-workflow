import logging
import logging.config
import os
import sys

from dotenv import load_dotenv

from pipeline import Pipeline
from utils import get_argsparser,parse_config, create_workspace_folder


#TODO Generic steps
#TODO Mlflow integration in traning session

#TODO docker file

# env variables:
load_dotenv()
WORKSPACE = os.getenv("WORKSPACE")

sys.path.append(os.getcwd())

# loggging
LOGGER_CONFIG_PATH = os.path.join(os.getcwd(),  "logger_config.yaml")
logging_config = parse_config(LOGGER_CONFIG_PATH)
logging.config.dictConfig(logging_config)

logger = logging.getLogger(__name__)



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

    args = vars(get_argsparser().parse_args())
    pipeline_config= parse_config(f"{args['pipeline_config']}")
 
    # creates a folder with the pipeline name in workspace location
    pipeline_name = pipeline_config['pipeline_name']
    logger.info(f"Running Pipeline : {pipeline_name}")
    
    workspace_path = os.path.join(WORKSPACE, pipeline_name)
    create_workspace_folder(workspace_path, delete_if_exist=False)
    
    # process the pipeline config and get the execution order and steps.
    pipeline = Pipeline(pipeline_config['steps'],workspace_path)
    steps_to_execute = pipeline.get_steps_to_execute()


    results = dict()
    step_references = steps_to_execute['step_reference']
    
    for step_name in steps_to_execute['linear_order']:
           
            step_workspace = os.path.join(pipeline.workspace, step_name)
            create_workspace_folder(step_workspace, delete_if_exist = True)
            
            step_ref = step_references[step_name]

            step_ref['workspace'] = step_workspace
            step_ref['inputs'] = [results.get(_sn, dict()) for _sn in step_ref['depends']] 
            
            # TODO not to pass step_moudule inside the config
            step_func = step_ref['step_module']

            results[step_name] =  execute_step(step_func, step_ref)


    logger.info("Ending run")
        
    

if __name__ == "__main__":
    start()

