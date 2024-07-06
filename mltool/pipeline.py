import logging
import os
import uuid



from mltool.dependency import DependencyManager

from mltool.utils import create_workspace_folder


class Pipeline:
    
    def __init__(self, config_file, is_rerun=False, run_id = None):

        self.logger = logging.getLogger(__name__)
        self.pipeline_name = config_file['pipeline_name']
        self.steps_list = config_file['steps']
        
        self.is_rerun = is_rerun
        
        if self.is_rerun:
            self.run_id = run_id
            self.logger.info(f" Rerunning pipleline: {self.pipeline_name} with id : {self.run_id}")
            # add the rerun variables to the steps
            # manage the workspace accordingly
        
        else:   
            self.run_id = uuid.uuid4() 
            self.logger.info(f"\n Creating pipeline: {self.pipeline_name}  with id {self.run_id}")
        
        self.run_workspace = self._create_worspace()
        
        
        self.ordered_steps_config = dict()
        self._create_steps_execution_order()

        self.logger.info(f"Pipeline: {self.pipeline_name} created successfully with run id: {self.run_id}")

    def _create_worspace(self):
         # folder with pipeline name inside root_workspace

        self.logger.info(f"Setting up workspace for : {self.pipeline_name} and run id: {self.run_id}")
        
        root_workspace = os.getenv('WORKSPACE')
        pipeline_workspace_path = os.path.join(root_workspace, self.pipeline_name)
        create_workspace_folder(pipeline_workspace_path, delete_if_exist=False)
        
        # folder with run_id inside the pipleline name
        run_workspace = os.path.join(pipeline_workspace_path, str(self.run_id))
        create_workspace_folder(run_workspace, delete_if_exist=False)

        return run_workspace




    # create the linear and parallel execution and attach the step module 
    def _create_steps_execution_order(self):

        self.logger.info(f"Creating execution order for steps in {self.pipeline_name}")
        
        dm = DependencyManager(self.steps_list, self.run_workspace)
        steps_reference, parallel_order, linear_order = dm.get_execution_order()
        
        execution_order = {
            "step_reference": steps_reference,
            "parallel_order": parallel_order,
            "linear_order": linear_order,
        }
        
        self.ordered_steps_config = execution_order


    def get_steps_to_execute(self):
        return self.ordered_steps_config
