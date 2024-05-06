import pandas as pd

from step import Step, FileState

class PreprocessData(Step):
    def run(self):
        self.logger.info("running Preprocess")
        file = self.inputs[0]
      
        df = pd.read_csv(file.open())
        self.logger.info(df.shape)

        return FileState(self.workspace,
                         filename = 'data_preprocess.csv',
                         content = df.to_csv())
            
