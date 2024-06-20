import json
import pandas as pd
import logging

from sklearn.model_selection import train_test_split

from core.step import step_function, FileState

logger = logging.getLogger(__name__)

@step_function
def feature_engineering(config):
    logger.info("Running extract features ...")

    data = config['inputs'][0]
    df = pd.read_csv(data.open())
    print(df.columns)
    feature_column = ['age', 'sex', 'bmi']
    target_column = ['target']
    X_train, X_test, y_train, y_test = train_test_split(df[feature_column].values.tolist(),
                                                        df[target_column].values.tolist())
    
    features = {
        'x_train': X_train,
        'x_test': X_test,
        'y_train': y_train,
        'y_test': y_test
    }
    
    return FileState(config['workspace'], 
                        filename='features.json',
                        content = json.dumps(features))
