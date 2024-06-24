import logging
import networkx as nx
import matplotlib.pyplot as plt
import os
import sys

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

        self._create_execution_order()

        
    def _create_execution_order(self):
        
        self.logger.info("Managing dependencies and creating execution order .. ")
        
        self._create_dependency_graph()
        
        self._topological_sort()
        
        # self._draw_dependency_graph()
        # self._draw_dependency_graph2()

        self.logger.info("execution order created successfully")

        
    
        
    
    def get_execution_order(self):
        
        if self._validate_execution_order():
            return self.steps_config,self.parallel_execution_order, self.linear_execution_order
        else:
            raise ValueError("Cycle detected or invalid dependencies")
        
    
    def _validate_execution_order(self):
        return len(self.linear_execution_order) == len(self.steps)

    
    def _attach_step(self,step: dict):

        # Dynamically import step function or STEP class

        step_module = step['class_name']  if step.get('class_name', None)  else step['function_name']
        self.logger.info(f"Attaching class/function for step_name: {step['name']} with  step_module: {step_module} ")

        _project, _folder, _file, _step = step_module.split('.')
        
        
        ## TODO Find a way around this
        PATH_TO_PIPELINES = '/Users/aakashbasnet/development/python/projects/ml-pipeline-workflow/pipelines/'
        sys.path.insert(0, f"{os.path.join(PATH_TO_PIPELINES, _project)}")

        mod = import_module(f"{_folder}.{_file}") 
        mod = getattr(mod,_step)
                
        step["step_module"] = mod
        return step
        
    def _create_dependency_graph(self):

        for step in self.steps:
            
            name = step['name']

            #attaching a step module
            self.steps_config[name] = self._attach_step(step)
            
            for dependency in step['depends']:

                self.graph[dependency].append(name)
                self.degree[name] += 1


    # Implementaion of Topological Sorting (Kahn's Algorithm)        
    def _topological_sort(self):
        self.logger.info("Resolving dependencies ... ")
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

   
