import logging
import networkx as nx
import matplotlib.pyplot as plt

from collections import defaultdict, deque
from graphviz import Digraph
from importlib import import_module




class DependencyManager():
    
    def __init__(self, step_list):
        self.logger = logging.getLogger(__name__)
        self.steps = step_list

        self.parallel_execution_order = []
        self.linear_execution_order = []

        self.steps_config = dict()
        self.graph = defaultdict(list)
        self.degree = defaultdict(int)
        
        self.logger.info("Creating execution order and attaching step module")
        self._create_dependency_graph()
        self._topological_sort()

        # self._draw_dependency_graph()
        # self._draw_dependency_graph2()
    
    
    def get_execution_order(self):
        
        if self._validate_execution_order():
            return self.steps_config,self.parallel_execution_order, self.linear_execution_order
        else:
            raise ValueError("Cycle detected or invalid dependencies")
        
    
    def _validate_execution_order(self):
        return len(self.linear_execution_order) == len(self.steps)

    
    def _attach_step(self,step):
       
        # Dynamically import step function or STEP class
        if step.get('class_name', None):
            _project, _folder, _file, _step = step['class_name'].split('.')
           
        else:
            _project, _folder, _file, _step = step['function_name'].split('.')
        
        mod = import_module(f"pipelines.{_project}.{_folder}.{_file}") 
        mod = getattr(mod,_step)
                
        step["step_module"] = mod
        return step
        
    def _create_dependency_graph(self):

        for step in self.steps:
            
            name = step['name']
            self.steps_config[name] = self._attach_step(step)

            # attach a function here
            
            for dependency in step['depends']:

                self.graph[dependency].append(name)
                self.degree[name] += 1


    # Implementaion of Topological Sorting (Kahn's Algorithm)        
    def _topological_sort(self):
        queue = deque([step for step in self.steps_config if self.degree[step] == 0])

        while queue:
            current_level = list(queue)
            self.parallel_execution_order.append(current_level)
            
            for step in current_level:
                queue.popleft()
                self.linear_execution_order.append(step)
                
                for dependent in self.graph[step]:
                    self.degree[dependent] -= 1
                    if self.degree[dependent] == 0:
                        queue.append(dependent)


    def _draw_dependency_graph(self):
        G = nx.DiGraph()
        
        for step in self.steps:
            for dependency in step['depends']:
                G.add_edge(dependency, step['name'])
        
        pos = nx.spring_layout(G)
        plt.figure(figsize=(12, 8))
        nx.draw(G, pos, with_labels=True, node_color='skyblue', node_size=3000, 
                edge_color='gray', linewidths=1, font_size=15, font_weight='bold', arrowsize=20)
        plt.title("Pipeline Dependency Graph", size=20)
        plt.show()

    
    def _draw_dependency_graph2(self, output_file="dependency_graph"):
        dot = Digraph(comment='Pipeline Dependency Graph')
        
        # Add nodes in the order of execution
        for step_name in self.linear_execution_order:
            step = next(step for step in self.steps if step['name'] == step_name)
            dot.node(step_name, step_name)
        
        # Add edges
        for step in self.steps:
            for dependency in step['depends']:
                dot.edge(dependency, step['name'])
        
        # Render and save the graph
        dot.format = 'png'
        dot.render(output_file, view=True)

   
