import uuid

from utils import get_argsparser,parse_config, create_workspace_folder



    


# env variables:

WORKSPACE = '/Users/aakashbasnet/development/python/workspace/ml_pipelines'

# manager run 
class Manager:
    def __init__(self, steps, workspace_path):
        id = uuid.uuid4()
        self.steps = steps
        self.workspace = f"{workspace_path}/{id}"
        create_workspace_folder(self.workspace)
    
    def run(self):
        results = dict()
        for step in self.steps:
            step_workspace = f"{self.workspace}/{step.name}"
            create_workspace_folder(step_workspace, delete = True)
            print(step.name)
            step.run()
            # save the returned results
            # set the input for the next object


def get_steps(p_config):
    
    # TODO create dependency graph
    step_objs = []
    for step in p_config:

        _folder, _file, _step = step['class_name'].split('.')
        mod = __import__(f"core.{_folder}.{_file}")
        mod = getattr(mod, _folder)
        mod = getattr(mod, _file)
        mod = getattr(mod, _step)
        obj = mod(**step)
        step_objs.append(obj)
    return step_objs

def start():
    # TODO setup loggers

    args = vars(get_argsparser().parse_args())
    pipeline_config= parse_config(f"core/{args['pipeline_config']}")
 
    
    # create pipeline folder in workspace
    pipeline_name = pipeline_config['pipeline_name']
    workspace_path = f"{WORKSPACE}/{pipeline_name}"
    create_workspace_folder(workspace_path)

    steps = get_steps(pipeline_config['steps'])
    
    manager = Manager(steps , workspace_path)
    manager.run()
        
    

if __name__ == "__main__":
    start()

