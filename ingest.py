#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Ingest data from the feed of new records and store them in the MongoDB server
'''
from configparser import ConfigParser
from time import sleep
from datetime import datetime
import requests
from pymongo import MongoClient

#Set the API variables and MongoDB configurations from the INI file
config = ConfigParser()
config.read('config.ini')

url = config['LiveAPI']['url']

client = MongoClient(host=config['MongoDB']['host'],
                     port=int(config['MongoDB']['port'])
                     )

db = client[ config['MongoDB']['database'] ]
coll = db[ config['MongoDB']['collection'] ]

#Define function to fetch new records from the live API
def api_pull(url):
    '''
    The response is returned as a JSON with these fields:
    '''
    response = requests.get( url )
    return response.json()

#Define function to insert records into the MongoDB server
def mongo_push( rec, coll ):
    '''
    Push the JSON of transformed records into the MongoDB
    '''
    coll.insert_one( rec )
    #print('\n INSERTED NEW RECORD:\n', rec)

#Create and push an example record to test
def test_mongo( coll ):
    example_record = {'kind':'test', 'test_field':True}
    coll.insert_one(example_record)
    print(coll.find({'kind':'test'}))
    print(db.list_collection_names())

########MAIN#########

if(__name__ == '__main__'):
    try:
        #Tracker vars
        cnt = 0
        ids = []
        while True:
            #If the counter is mod 15, re-sync the saved list of ids
            if (cnt % 15 == 0):
                ids = coll.find({}).distinct('object_id')
            
            #Fetch the latest live records
            try:
                new_rec = api_pull(url)
                print( '\nI just found this!\n\n', 'object id: {}, name: {}'.format(new_rec['object_id'], new_rec['name']) )
            except:
                print( '\nFailed to fetch new records!' )
                raise SystemExit( f'\nFatal error occured at {datetime.now().time()}! OwO\nTry restarting me!\n' )
            
            #Push into the MongoDB if id not there already, then append to list of ids
            if new_rec['object_id'] not in ids:
                try:
                    mongo_push( new_rec, coll )
                    print ( '\nInserted into MongoDB successfully!' )
                    ids.append( new_rec['object_id'] )
                except:
                    print( '\nFailed to put anything in the MongoDB!' )
                    raise SystemExit( f'\nFatal error occured at {datetime.now().time()}! OwO\nTry restarting me!\n' )
            
            #Increment counter
            cnt += 1
            
            #Wait ten secs before looping again
            sleep(10)
            
    except KeyboardInterrupt:
        print(f'\nOK, I\'ll stop!\n')