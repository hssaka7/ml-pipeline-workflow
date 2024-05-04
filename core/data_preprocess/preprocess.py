import pandas as pd

from step import Step, FileState

class PreprocessData(Step):
    def run(self):
        print("running Preprocess")
        file = self.inputs[0]
      
        df = pd.read_csv(file.open())
        print(df.head())

        return FileState(self.workspace,
                         filename = 'data_preprocess.csv',
                         content = df.to_csv())
            
