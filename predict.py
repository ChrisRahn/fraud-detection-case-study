#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Get unprocessed files out of the MongoDB and update them with a prediction for each one
'''
from sys import argv
from configparser import ConfigParser
from datetime import datetime
from time import sleep
import pickle
import pandas as pd
import numpy as np
from pymongo import MongoClient
from preprocess_data import blind_pipeline

#Set the API variables and MongoDB configurations from the INI file
config = ConfigParser()
config.read('config.ini')

url = config['LiveAPI']['url']

client = MongoClient(host=config['MongoDB']['host'],
                     port=int(config['MongoDB']['port'])
                     )

db = client[ config['MongoDB']['database'] ]
coll = db[ config['MongoDB']['collection'] ]

version = config['MongoDB']['version']

#If an update mode was passed,set it; otherwise use the config settings
try: update_mode = argv[1]
except: update_mode = config['MongoDB']['update_mode']
print(f'\nOkay, starting with update mode {update_mode}!')

#Load in the pickled model
model = pickle.load( open('model.pkl', 'rb') )

#Define function to process the set of new records
def record_transform( record ):
    '''
    Reads in JSON of the record and returns the cleaned-up DataFrame using the same transformations as the trained model
    '''
    #Cast the new records to a DataFrame
    rec_dirty = pd.DataFrame( dict([ (k, pd.Series(v)) for k,v in record.items()]) )
    
    #Apply the same pre-processing pipeline to the new data as the training data
    rec_clean = blind_pipeline( rec_dirty )
    
    #Cast the cleaned DataFrame back to a JSON
    #return rec_clean.to_json( orient='records')
    return rec_clean

####MAIN####
    
if(__name__ == '__main__'):
    #Define cursor based on update_mode
    if (update_mode == 'new_only'):
        #Pull all the records from the MongoDB without a prediction
        cursor = coll.find( {'prediction':{'$exists':False}} )
    if (update_mode == 'all'):
        #Pull ALL the records
        cursor = coll.find( {} )
    
    print(f'\nFound {cursor.count()} documents! Watch me go...!')
        
    #For each record in the cursor...
    try:
        while cursor.alive:
            record = cursor.next()
            
            #Pop the record id, transform & predict using the model
            rec_id = record.pop('_id', -1)
            try:
                rec_clean = record_transform( record )
                print('Cleaned record OK')
                rec_arr = np.array(rec_clean)
                rec_arr = np.nan_to_num( rec_arr )
    
                pred_class = int( model.predict( rec_arr )[0] )
                print('Hard predict OK')
                prob = float( model.predict_proba( rec_arr )[0, 1] )
                print('Soft predict OK')
            
            except:
                print('\nSomething went wrong with prediction')
                continue
        
            #Update the same record with the predicted value and the current model version
            try:
                coll.update_one( {'_id':rec_id}, {'$set':{'prediction':pred_class, 'probability':prob, 'version':version}} , upsert=True)
            
                #Print the object_id just processed
                print('\nUpdated prediction for object_id ', record['object_id'], f' (Mongo id: {rec_id}).')
            
            except:
                print( '\nFailed to update a record - it was object_id: ', record['object_id'] )
                #raise SystemExit( f'\nFatal error occured at {datetime.now().time()}! OwO\nTry restarting me!\n' )

            sleep(0.05)
    
    except KeyboardInterrupt:
        print('\nOK, quitting job then...')