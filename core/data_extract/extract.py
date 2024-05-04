
import pandas as pd

from sklearn.datasets import load_diabetes

from step import Step, FileState

class Extract(Step):
    def run(self):
        print("running extract")
        diabetes_dataset = load_diabetes()
        df = pd.DataFrame(data=diabetes_dataset.data,
                          columns=diabetes_dataset.feature_names)
        
        df['target'] = diabetes_dataset.target

        return FileState(workspace= self.workspace,
                         filename= 'diabetes_extract.csv',
                         content = df.to_csv(),
                         metadata = {"message": "success"})
       