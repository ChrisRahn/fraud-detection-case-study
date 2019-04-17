'''
Builds the model pickle with the preprocess_data.py pipeline
'''

import pandas as pd
import numpy as np
import pickle
from preprocess_data import pipeline
#from sklearn.linear_model import LogisticRegression
#from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import GradientBoostingClassifier
#from sklearn.ensemble import RandomForestClassifier as RFC

#Read in the training dataset
raw_data = pd.read_json( 'data/data.json' )

#Transform it using all of preprocess_data.py's functions
clean_data = pipeline( raw_data )

#Split out labels
y = np.array( clean_data.pop('fraudulent') )
X = np.array( clean_data )

#Define a particular model and fit it
model = GradientBoostingClassifier(n_estimators=5000)
model.fit(X, y)

def model_score(model):
    print( model.score(X, y) )
    
    #y_predict = model.predict(X)
    #y_true = y

model_score(model)

#Pickle the model for later use as model.pkl
pickle.dump( model, open('model.pkl', 'wb') )