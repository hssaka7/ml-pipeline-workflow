import logging
import os

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import wraps

from mltool.utils import write_file, parse_yaml_config, create_workspace_folder
from mltool.yaml_handler import YamlCRUD


# step abstractions with decorators for functional programming 
def step_function(func):
    @wraps(func)
    def wrapper( **kwargs):
        name = kwargs['name']
        # depends = kwargs['depends']
        inputs = kwargs.get('inputs', [])

        workspace = kwargs['workspace']
        
        return func(kwargs)
    return wrapper

# step abstractions with Class for object oriented programming
class Step(ABC):
    def __init__(self, *args, **kwargs):
        
        self.workspace = kwargs['workspace']
        self.name = kwargs['name']
        self.depends = kwargs['depends']
        self.is_fresh_run = kwargs.get('fresh_run', True)
        self.config = kwargs

        # TODO maybe on reruns delete the metadata and let the code run,
        # which should run like the rerun when the metadata is not there. 
        # when the metadata is there. it can skip the fresh run during the rerun.
        
        self.inputs_workspace = kwargs.get('inputs_workspace', dict())
        
        # current step metadata placeholder
        self.metadata = dict()

        # placeholder for the outputs and metadata form steps on depends
        self.inputs = kwargs.get('inputs', [])
        self.inputs_metadata = []

        self.outputs = []

   
    def _load_inputs ( self):
        """Loads the inputs for the curtent step based on """
 
        if self.depends:
            
            for _prev_step in self.depends:

                _prev_step_workspace = self.inputs_workspace[_prev_step]
                
                # load metadata
                _prev_step_metadata_path = os.path.join(_prev_step_workspace, "_metadata.yaml")
                _prev_step_metadata = parse_yaml_config(_prev_step_metadata_path)
                self.inputs_metadata.append(_prev_step_metadata)

                # load inputs
                #  TODO attach inputs for  here based on the value in metadata
                _prev_step_output = [FileState(file_path=os.path.join(_prev_step_workspace, f)) 
                                     for f in os.listdir(_prev_step_workspace)
                                     if os.path.isfile(os.path.join(_prev_step_workspace, f))
                                     and f!='_metadata.yaml'
                                     ]
                
                self.inputs.append(_prev_step_output)

        
    def save_metadata(self):
        yaml_obj = YamlCRUD(os.path.join(self.workspace,'_metadata.yaml'))
        yaml_obj.create_data(self.metadata)


    def execute(self):
        
        # TODO if rerun then, delete if any thing exists in the workfolder. 
        # if rerun is False then dont execute the step
        
        # TODO nned to do this outside the step, on pipeline executor. 
        # if it does not need fresh run then the step should not be initialize
        if not self.is_fresh_run:
            # do not execute.
            return None,None

        try:
            self._load_inputs()
            create_workspace_folder(self.workspace, delete_if_exist=False)
            result = self.run()
            self.metadata["success"] = True
            self.save_metadata()

            return result, self.metadata
        
        except Exception as e:
            self.metadata["success"] = False
            self.metadata["error_msg"] = str(e)
            self.save_metadata()
            raise e
        

    @abstractmethod
    def run(self):
        pass

@dataclass
class FileIO:
    workspce: str
    filename: str
    content: str
    save_func: callable


    def open(self):
        return open(self.file_path, 'r')


# Input and output abstraction for Files
# Each file will be a file state
class FileState():
    
    def __init__(self,
                 workspace = None,
                 filename = None,
                 content = None ,
                 file_path = None,
                 save_function = None,
                 metadata = None):

        self.workspace = workspace
        self.filename = filename
        self.content = content
        self.file_path = file_path
        self.save_function = save_function

        if self.file_path:
            self.file_path = self.file_path
        
        elif self.workspace and self.filename and self.content:
            _temp_file_path = os.path.join(workspace,filename)
            self.file_path = self._create_file_path(_temp_file_path, content,save_function)

        else:
            raise("Either need filepath or (workspace, filename and content) for file creation")
    
    def _create_file_path(self, file_path, content, save_function):
        
        # check if content is there
        if not bool(content):
            raise Exception(" Must provide (string I/O file content) or (object content and save function)")
        
        # TODO need to work more on the save function
        if  bool(save_function):
            save_function(content, file_path)
        
        else:
             write_file(file_path, content)
             
        return file_path


    def open (self):
        return  open(self.file_path, 'r') 