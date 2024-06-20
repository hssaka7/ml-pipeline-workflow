import pandas as pd
import logging

from core.step import step_function, FileState

logger = logging.getLogger(__name__)

@step_function
def preprocess_data(config):
    
    logger.info("running Preprocess")
    file = config['inputs'][0]
    
    df = pd.read_csv(file.open())
    logger.info(df.shape)

    return FileState(config['workspace'],
                        filename = 'data_preprocess.csv',
                        content = df.to_csv(index=False))
        
