import json
import logging 

from abc import ABC, abstractmethod
from dataclasses import dataclass
from core.utils import write_file

from functools import wraps


# step abstractions with decorators for functional programming 
def step_function(func):
    func.__dependencies__ = set()
    
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

    def set_inputs ( self, inputs = []):
        self.inputs = inputs

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
    def __init__(self,workspace, filename, content, metadata = None):
        
        self.file_path = f'{workspace}/{filename}'
        
        self.meatdata_path = f'{workspace}/_metadata.json'
        metadata = metadata if bool(metadata) else dict()
        

        write_file(self.file_path, content)
        write_file(self.meatdata_path, json.dumps(metadata))
       
    def open (self):
        return open(self.file_path, 'r')
            

