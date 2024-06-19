import logging
import logging.config
import os
import sys
import uuid

from importlib import import_module
from utils import get_argsparser,parse_config, create_workspace_folder


from step import step_function

    
# TODO set up complete project
#TODO Generic steps

#TODO Mlflow integration in traning session
#TODO checkout model to model registry
#TODO create FAST API to pull latest model from model registry and make predictions
#TODO docker file

# env variables:

WORKSPACE = '/Users/aakashbasnet/development/python/workspace/ml_pipelines'
MLFLOW_TRACKING_SERVER = ''

sys.path.append(os.getcwd())

# loggging
LOGGER_CONFIG_PATH = os.path.join(os.getcwd(),  "logger_config.yaml")
logging_config = parse_config(LOGGER_CONFIG_PATH)
logging.config.dictConfig(logging_config)

logger = logging.getLogger(__name__)

# manager run 
class Manager:
    def __init__(self, step_config, workspace_path):
        
        self.id = uuid.uuid4()
        logger.info(f"Starting Manager with id {self.id}")
        self.workspace = f"{workspace_path}/{self.id}"
        
        # creates folders with uniqiue id for each run inside the pipeline folder
        create_workspace_folder(self.workspace, delete_if_exist=False)
        
        self.step_config = step_config
        self.steps = self.get_steps()


    def run(self):
        results = dict()
        for count, step in enumerate(self.steps):

            if count == 0 and step.depends != []:
                raise Exception("First step should not have any dependency")
            
            # creates folder with the step name inside the unique id folder 
            step_workspace = f"{self.workspace}/{step.name}"
            create_workspace_folder(step_workspace, delete_if_exist = True)
            step.workspace = step_workspace

            if step.depends != []:
                step.inputs = [results[x] for x in step.depends]
            results[step.name] =  step.run()
            # save the returned results
            # set the input for the next object


    def get_steps(self):
        
        # TODO create dependency graph
        step_objs = []
        for step in self.step_config:
             
            
            
            
            if step.get('class_name', None):
                _project, _folder, _file, _step = step['class_name'].split('.')
                mod = import_module(f"pipelines.{_project}.{_folder}.{_file}") 
                mod = getattr(mod,_step)   
                obj = mod(**step)

            else:
                _project, _folder, _file, _step = step['function_name'].split('.')
                mod = import_module(f"pipelines.{_project}.{_folder}.{_file}") 
                mod = getattr(mod,_step)
                obj = step_function(mod)

            
            
            step_objs.append(obj)  
        return step_objs

def start():
    # TODO setup loggers

    args = vars(get_argsparser().parse_args())
    pipeline_config= parse_config(f"{args['pipeline_config']}")
 
    # creates a folder with the pipeline name in workspace location
    pipeline_name = pipeline_config['pipeline_name']
    logger.info(f"Pipeline name: {pipeline_name}")
    workspace_path = f"{WORKSPACE}/{pipeline_name}"
    create_workspace_folder(workspace_path, delete_if_exist=False)
    
    manager = Manager(pipeline_config['steps'] , workspace_path)
    manager.run()
        
    

if __name__ == "__main__":
    start()

