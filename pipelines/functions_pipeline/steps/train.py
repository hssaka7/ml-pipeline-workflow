import json
import logging

from sklearn.ensemble import RandomForestRegressor

from core.step import step_function

logger = logging.getLogger(__name__)

@step_function
def train_randomforest(config):
    
    logger.info("Running Training ...")
    input = config['inputs'][0]
    data = json.load(input.open())

    x_train = data['x_train']
    y_train = [y for x in data['y_train'] for y in x]


    random_forest = RandomForestRegressor(n_estimators=100, max_depth=6, max_features=3)

    
    random_forest.fit(x_train, y_train)

    print("here")
