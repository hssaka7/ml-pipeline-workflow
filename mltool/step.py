import json
import logging 
import os

from abc import ABC, abstractmethod
from dataclasses import dataclass
from mltool.utils import write_file

from functools import wraps

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
        self.config = kwargs
        self.inputs = kwargs['inputs']

        self.metadata = dict()
        self.rerun = kwargs.get('rerun', False)


    def set_inputs ( self, inputs = []):
        # step input workspace
        # get the input from metadata, and the files in the filestate

        
        self.inputs = inputs

    def execute(self):
        
        # if rerun then, delete if any thing exists in the workfolder. 
        # if rerun is False then dont execute the step
        _metadata = dict()
        
        try:
            result = self.run()
            _metadata["success"] = True
            return result
        
        except Exception as e:
            _metadata["success"] = False
            raise 
        

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
    
    def __init__(self,workspace, filename, content , file_path = None, save_function = None, metadata = None):

        
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