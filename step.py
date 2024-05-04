from abc import ABC, abstractmethod


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
    def __init__(self,file_path=None, content=None, metadata = None):
        
        if  not (file_path and content):
            # how to create empty file
            print("Empty file")
        
        self.file_path = file_path
        self.metadata = metadata if bool(metadata) else dict()
        # need workspace folder

        self.save(content)

    
    def save(self, content):
        with open(self.file_path, 'w') as file:
            file.write(content)

    
    def open (self, mode = 'r'):
        return open(self.file_path, mode)
            

