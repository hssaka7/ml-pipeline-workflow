import json

from abc import ABC, abstractmethod
from utils import write_file


# TODO implement file state
# TODO duty shoud be able to import the feed class 

class Step(ABC):
    def __init__(self, *args, **kwargs):
        
        self.name = kwargs['name']
        self.depends = kwargs['depends']
        
        self.config = kwargs

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
            

