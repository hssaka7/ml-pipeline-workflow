import pandas as pd
import logging 

from core.step import Step, FileState

logger = logging.getLogger(__name__)
class PreprocessData(Step):
    def run(self):
        logger.info("running Preprocess")
        file = self.inputs[0]
      
        df = pd.read_csv(file.open())
        logger.info(df.shape)

        return FileState(self.workspace,
                         filename = 'data_preprocess.csv',
                         content = df.to_csv(index=False))
            
