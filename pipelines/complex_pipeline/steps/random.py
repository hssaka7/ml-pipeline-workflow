import json
import logging
import random

from mltool.step import Step, FileState


class GetRandomNumbers(Step):
    def run(self):
        self.logger = logging.getLogger(self.name)
        self.logger.info("Generating random numbers")
        random_numbers = [int (random.random() * 10 ) for x in  range(5)]
        self.logger.info(len(random_numbers))

        return FileState(self.workspace,
                         'random_number.json',
                         content = json.dumps(random_numbers))
    
class AddOne(Step):
    def run(self):
        self.logger = logging.getLogger(self.name) 
        one_added = [rn+1 for f in self.inputs for rn in json.load(f.open())]
        self.logger.info(one_added)
        self.logger.info(len(one_added))

        return FileState(self.workspace,
                         'one_added.json',
                         content = json.dumps(one_added))
        
