
# core/pipeline_executor.py

import yaml
import importlib
from step import step_function, FileState

import logging

logger = logging.getLogger(__name__)

class PipelineExecutor:
    def __init__(self, config_file):
        self.config_file = config_file
        self.steps_config = self._load_steps_config()

    def _load_steps_config(self):
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config.get('steps', [])

    def get_steps_to_execute(self):
        steps_to_execute = {}
        for step_config in self.steps_config:
            step_name = step_config['name']
            class_name = step_config['class_name']
            depends_on = step_config.get('depends_on', [])

            # Dynamically import step function/class
            module_name, func_name = class_name.rsplit('.', 1)
            module = importlib.import_module(module_name)
            step_func = getattr(module, func_name)

            # Wrap step function with step_function decorator
            wrapped_step_func = step_function(step_func)

            

            # Store the wrapped step function in steps_to_execute dictionary
            steps_to_execute[step_name] = wrapped_step_func

        return self._resolve_dependencies(steps_to_execute)

    def _resolve_dependencies(self, steps_to_execute):
        # Convert steps_to_execute dictionary to a list of tuples
        steps_list = list(steps_to_execute.items())

        # Topologically sort steps based on dependencies using Kahn's algorithm
        ordered_steps = []
        while steps_list:
            removable_steps = [step for step in steps_list if all(dep in ordered_steps for dep in step[1].__dependencies__)]
            if not removable_steps:
                raise ValueError("Circular dependency detected or dependency not satisfied")
            ordered_steps.extend(removable_steps)
            steps_list = [step for step in steps_list if step not in removable_steps]

        # Return ordered steps as a dictionary
        return dict(ordered_steps)
