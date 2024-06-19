
import pandas as pd
import logging

from sklearn.datasets import load_diabetes

from core.step import Step, FileState
from ..helpers.functions import add

logger = logging.getLogger(__name__)

class Extract(Step):
    def run(self):
        logger.info("running extract")
        diabetes_dataset = load_diabetes()
        df = pd.DataFrame(data=diabetes_dataset.data,
                          columns=diabetes_dataset.feature_names)
        
        df['target'] = diabetes_dataset.target
        logger.info(f"ADDING ..... {add(1,2)}")

        return FileState(workspace= self.workspace,
                         filename= 'diabetes_extract.csv',
                         content = df.to_csv(index=False),
                         metadata = {"message": "success"})





