import logging
import os
import uuid



from mltool.dependency import DependencyManager

from mltool.utils import create_workspace_folder


class Pipeline:
    def __init__(self, config_file):
        
        self.pipeline_name = config_file['pipeline_name']
        self.run_id = uuid.uuid4()

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"\n Starting pipeline with id {self.run_id}")

        self.steps_list = config_file['steps']

        self.run_workspace = None
        self.ordered_steps_config = dict()

        self._create_worspace()
        self._create_steps_execution_order()

    def _create_worspace(self):
         # folder with pipeline name inside root_workspace
        root_workspace = os.getenv('WORKSPACE')
        pipeline_workspace_path = os.path.join(root_workspace, self.pipeline_name)
        create_workspace_folder(pipeline_workspace_path, delete_if_exist=False)
        
        # folder with run_id inside the pipleline name
        self.run_workspace = os.path.join(pipeline_workspace_path, str(self.run_id))
        create_workspace_folder(self.run_workspace, delete_if_exist=False)


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
