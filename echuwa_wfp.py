# Script uses the requests and pymongo libraries to fecth data from ONA's Rest API for storing into a Atlas MongoDB database.
# Todo: Improve exception handling (to catch more exceptions, more detailed messaging, logging, etc)
# Author: echuwa@gmail.com

import sys

if sys.version_info[0] < 3:
    raise Exception("Script written for Python 3 or higher.")
    
import requests
import pymongo


from pymongo import MongoClient


# Function to open connection to Mongo DB server; returns the connection object 
# To likely be generalised to accept connection details as inputs
def openDatabaseConnection ():
    #app_conn = MongoClient(
    #    'mongodb+srv://echuwa:3ng3lb3r%2B@cluster0-uzmpx.gcp.mongodb.net/test?retryWrites=true&w=majority'
    #)

    app_conn = MongoClient('mongodb://echuwa:3ng3lb3r%2B@cluster0-shard-00-00-uzmpx.gcp.mongodb.net:27017,cluster0-shard-00-01-uzmpx.gcp.mongodb.net:27017,cluster0-shard-00-02-uzmpx.gcp.mongodb.net:27017/wfptest?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority'
    )
    return app_conn

# Function to fetch data from the ONA API and store is as is to MongoDB. 
# Inputs: connection object and ONA dataset idenfitier
def fetchData(conn, dataset_id):
    try:
        #To throw an exception if connection attempt failed 
        conn.server_info() 
        
        db_conn = conn.wfptest
        
        # To likely use one of the end points with limit operators instead of this for efficiency (parallel requests, incremental synchronisation) 
        data = requests.get('https://api.ona.io/api/v1/data/' + dataset_id)
        
        if data.status_code != 200:
            raise Exception('API not available. Status code: '.format(data.status_code))
        try:
            #May need to revise the line below depending on whether or not the API only exposes completed/final submissions. Should ONA support editing responses, or the API expose partial submissions like one might expect with SMS surveys, I would revert to some implementation of upsert instead. Paging through the received data would also be handled here.        
            res_ids = db_conn.formresults.insert_many(data.json(), ordered=False) 
            
        #Catch more exceptions, more descriptive error messagins, graceful handled    
        except pymongo.errors.BulkWriteError as blk_db_err:
            print ("Bulk Write Error.")
            #print (format(blk_db_err.details))    
        except pymongo.errors.PyMongoError as gen_db_err:
            print ("Gen DB Error: ", format(gen_db_err))
    except:
        print("Error ", format(sys.exc_info()[0]))

# Program main       
app_conn = openDatabaseConnection()
fetchData(app_conn, '185260')
