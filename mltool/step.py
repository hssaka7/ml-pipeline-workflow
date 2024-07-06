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

        
        
    def set_inputs ( self):
 
        if self.depends:
            
            for _prev_step in self.depends:
                
                _prev_step_metadata_path = os.path.join(self.inputs_workspace[_prev_step], "_metadata.yaml")
                _prev_step_metadata = parse_yaml_config(_prev_step_metadata_path)
                
                self.inputs_metadata.append(_prev_step_metadata)

                
                # TODO attach inputs for  here based on the value in metadata

            # all the files saved in the folder will be used as a input?? 

    def set_workspace(self):
        create_workspace_folder(self.workspace, delete_if_exist=False)
        
    def save_metadata(self):
        yaml_obj = YamlCRUD(os.path.join(self.workspace,'_metadata.yaml'))
        yaml_obj.create_data(self.metadata)


    def execute(self):
        
        # if rerun then, delete if any thing exists in the workfolder. 
        # if rerun is False then dont execute the step
        
        if not self.is_fresh_run:
            # do not exececute.
            return None

        try:
            self.set_inputs()
            self.set_workspace()
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
                 workspace,
                 filename,
                 content , file_path = None, save_function = None, metadata = None):

        
        if not bool(file_path):
            _temp_file_path = os.path.join(workspace,filename)
            self.file_path = self._create_file_path(_temp_file_path, content,save_function)
 
        # TODO check file path must be a file inside the workspace
        # TODO file path must exist
        else:
            self.file_path = file_path
    
    def _create_file_path(self, file_path, content, save_function):
        
        # check if content is there
        if not bool(content):
            raise Exception(" Must provide (string I/O file content) or (object content and save function)")
        
        if  bool(save_function):
            save_function(content)
        
        else:
             write_file(file_path, content)
             
        return file_path


    def open (self):
        return  open(self.file_path, 'r') 