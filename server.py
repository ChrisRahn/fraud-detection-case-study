#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The server for our web app
"""

from flask import Flask, render_template, request, jsonify, Response
import pandas as pd
from pymongo import MongoClient
from configparser import ConfigParser


config = ConfigParser()
config.read('config.ini')

url = config['LiveAPI']['url']

client = MongoClient(host=config['MongoDB']['host'],
                     port=int(config['MongoDB']['port'])
                     )

db = client[ config['MongoDB']['database'] ]
coll = db[ config['MongoDB']['collection'] ]

version = config['MongoDB']['version']

#Create the app object that will route our calls
app = Flask(__name__)

@app.route( '/', methods=['GET'] )
def home():
    return render_template('home.html')

@app.route( '/dashboard', methods=['GET'] )
def dashboard():
    return render_template('dashboard.html')

@app.route('/fetch_reports', methods=['GET'])
def fetch_reports():
    
    #Fetch the latest 20 documents
    cursor = coll.find( {'name':{'$ne':'null'}}, {'name':True, 'prediction':True, 'probability':True}).sort('event_created').limit(20)
    
    name = []
    probability = []
    prediction = []
    for i in cursor:
        name.append( i['name'] )
        probability.append( i['probability'] )
        prediction.append( i['prediction'] )
        print(name)
        print(probability)
        print(prediction)
    data = list(zip(name, probability, prediction))
        
    return jsonify(data)

#     cursor = coll.find( {'prediction':{'$exists':True}}, {'object_id':True}, {'name':True}, {'probability':True}, {'prediction':True})

    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3333, debug=True)
