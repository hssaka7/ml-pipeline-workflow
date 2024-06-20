import logging
import os
import uuid
import yaml


from dependency import DependencyManager

from utils import create_workspace_folder


class Pipeline:
    def __init__(self, config_file, workspace_path):
        
        self.logger = logging.getLogger(__name__)
        self.id = uuid.uuid4()
        self.logger.info(f"\n Starting pipeline with id {self.id}")

        self.workspace = os.path.join(workspace_path, str(self.id))
        create_workspace_folder(self.workspace, delete_if_exist=False)

        self.steps_list = config_file
        
        self.ordered_steps_config = dict()
        self._create_steps_execution_order()

    # create the linear and parallel execution and attach the step module 
    def _create_steps_execution_order(self):
        
        dm = DependencyManager(self.steps_list)
        steps_reference, parallel_order, linear_order = dm.get_execution_order()
        
        execution_order = {
            "step_reference": steps_reference,
            "parallel_order": parallel_order,
            "linear_order": linear_order,
        }
        
        self.ordered_steps_config = execution_order


    def get_steps_to_execute(self):
        return self.ordered_steps_config
