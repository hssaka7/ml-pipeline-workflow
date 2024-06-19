import json
import logging 
from abc import ABC, abstractmethod
from utils import write_file

from functools import wraps
# TODO implement file state
# TODO duty shoud be able to import the feed class 


def step_function(func):
    func.__dependencies__ = set()
    
    @wraps(func)
    def wrapper(inputs, *args, **kwargs):
        return func(inputs, *args, **kwargs)
    return wrapper

class Step(ABC):
    def __init__(self, *args, **kwargs):
        
        self.name = kwargs['name']
        self.depends = kwargs['depends']
        self.config = kwargs
        
        self.logger = None
        self.inputs = []

    def set_inputs ( self, inputs = []):
        self.inputs = inputs

    @abstractmethod
    def run(self):
        pass


class FileState():
    def __init__(self,workspace, filename, content, metadata = None):
        
        self.file_path = f'{workspace}/{filename}'
        
        self.meatdata_path = f'{workspace}/_metadata.json'
        metadata = metadata if bool(metadata) else dict()
        

        write_file(self.file_path, content)
        write_file(self.meatdata_path, json.dumps(metadata))
       
    def open (self):
        return open(self.file_path, 'r')
            

