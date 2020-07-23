#############
#############
#############
###INFO PROJECT NAME:   EMAG INFLUX TO MONGO PARSER
###INFO
###INFO DESCRIPTION:    THIS PARSER PARSES THE INFORMATION SAVED IN INFLUX DB V1.7 TO MONGO DB
###INFO                 CONDITIONS:
###INFO                     1: EACH ENDPOINT IS SAVED IN ONE MEASUREMENT OF THE INFLUX DB
###INFO                         DATABASE example-databse
###INFO                             |-  MEASUREMENTS
###INFO                             | - measurement-endpoint1
###INFO                                 | - timestamp1, value1
###INFO                                 | - timestamp2, value2
###INFO                                 | - timestamp3, value3
###INFO                                 | - ...
###INFO                             | - measurement-endpoint2
###INFO                             | - measurement-endpoint3
###INFO                             | - ...
###INFO                     2: ALL ENDPOINTS OF ALL MEASUREMENTS WILL BE PARSED TO A SINGLE COLLECTION OF THE MONGO DB
###INFO                         DATABASE example-databse
###INFO                             |-  COLLECTIONS
###INFO                             | - machineDataCollection
###INFO                                 | - {document 1 containing timestamp1, value 1 from measurement-endpoint 1}
###INFO                                 | - {document 2 containing timestamp2, value 2 from measurement-endpoint 1}
###INFO                                 | - {document 3 containing timestamp3, value 3 from measurement-endpoint 1}
###INFO                                 | - ...
###INFO                                 | - {document n+1 containing timestamp1, value 1 from measurement-endpoint 2}
###INFO                                 | - ...
###INFO                                 | - {document n+1 containing timestamp1, value 1 from measurement-endpoint 3}
###INFO                                 | - ...
###INFO
###INFO CREATOR: EMIL BAROTHI
###INFO E-MAIL: EBAROTHI@EMAG.COM
#############
#############
#############
#
#########################
###NOTE IMPORTING PACKAGES
##########################
#
from influxdb import InfluxDBClient, DataFrameClient
import json
import pandas as pd
from pymongo import MongoClient
import time
import json
#
##########################
###NOTE STARTING TIMER
##########################
start_time = time.time()
#
##########################
###NOTE DEFINING NECESSARY VARIABLES
##########################
database_name = 'M111682'
machine_serial_number = 11682
machine_type = 'CI400'
#
##########################
###NOTE OBTAINING MEASUREMENTS OF THE INFLUX DATABASE
##########################
client = InfluxDBClient(host='127.0.0.1',
                    port = 8086,
                    username='',
                    password='',
                    database=database_name)
measurements = client.get_list_measurements()
measurements = [elem['name'] for elem in measurements]
#
##########################
###NOTE THIS IS WHERE THE MAGIC HAPPENS
##########################
client = DataFrameClient(host='127.0.0.1',
                        port=8086,
                        username='',
                        password='',
                        database=database_name)
#
#* FOR EACH MEASUREMENT OF THE INFLUX DATABASE PERFORM THE FOLLOWING OPERATIONS:
#*  1: QUERY THE MEASUREMENT AND SAVE THE TIMESTAMP - VALUE PAIRS IN A DATA FRAME
#*  2: EXTEND THE DATAFRAME WITH THE SUPPLEMENTARY INFORMATION NEEDED IN EACH DOCUMENT OF THE MONGO DB
#*  3: TRANSFORM THE DATAFRAME TO A DICTIONARY
#*  4: CONNECT TO THE MONGO DATABASE
#*  5: SAVE THE DICTIONARY TO THE machineDataCollection of the MONGO DATABASE
#
for measurement in measurements:
    print(measurement)
    #
    #*  1: QUERY THE MEASUREMENT AND SAVE THE TIMESTAMP - VALUE PAIRS IN A DATA FRAME
    results = client.query('select * from %s' %(measurement))
    df = results[measurement]
    df.rename(columns={'Value': 'values'}, inplace=True)
    #
    df['createdAt'] = df.index
    df.reset_index(inplace=True, drop=True)
    #
    #*  2: EXTEND THE DATAFRAME WITH THE SUPPLEMENTARY INFORMATION NEEDED IN EACH DOCUMENT OF THE MONGO DB
    df['machineType'] = machine_type
    df['machineId'] = machine_serial_number
    df['valueType'] = measurement
    df['version'] = 0
    #
    df = df[['createdAt', 'machineId', 'machineType', 'valueType', 'values', 'version']]
    #
    #*  3: TRANSFORM THE DATAFRAME TO A DICTIONARY
    documents = df.to_dict('records')
    #
    #*  4: CONNECT TO THE MONGO DATABASE
    mongoDB_client = MongoClient('127.0.0.1', 27017)
    selected_db = mongoDB_client['test_db']
    selected_collection = selected_db['test_collection']
    #
    #*  5: SAVE THE DICTIONARY TO THE machineDataCollection of the MONGO DATABASE
    selected_collection.insert_many(documents)
    #
    print("\n Finished in: \n --- %s sec ---" % round((time.time() - start_time), 2))
