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
from pymongo import MongoClient
#
import json
import pandas as pd
import strict_rfc3339
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

class InfluxToMongoParser():
    def __init__(self,
                influx_host='127.0.0.1',
                influx_port=8086,
                influx_user='',
                influx_password='',
                influx_database=None,
                influx_exclude_measurements=None,
                mongo_host='127.0.0.1',
                mongo_port=27017,
                mongo_user='',
                mongo_password='',
                mongo_database=None,
                mongo_collection='machineDataCollection'):
        #
        self.influx_host = influx_host
        self.influx_port = int(influx_port)
        self.influx_user = influx_user
        self.influx_password = influx_password
        self.influx_database = influx_database
        self.influx_exclude_measurements = influx_exclude_measurements
        self.mongo_host = mongo_host
        self.mongo_port = int(mongo_port)
        self.mongo_user = mongo_user
        self.mongo_password = mongo_password
        self.mongo_database = mongo_database
        self.mongo_collection = mongo_collection
    #
    ##########################
    ###NOTE OBTAINING MEASUREMENTS OF THE INFLUX DATABASE
    ##########################
    def get_influx_measurements(self):
        #
        client = InfluxDBClient(host=self.influx_host, 
                                port=self.influx_port, 
                                username=self.influx_user, 
                                password=self.influx_password, 
                                database=self.influx_database)
        #
        measurements = client.get_list_measurements()
        measurements = [measurement['name'] for measurement in measurements if measurement not in self.influx_exclude_measurements]
        #
        return measurements
    #
    def parse_data_influx_to_mongo(self, 
                                influx_measurements, 
                                machine_type='CI400', 
                                machine_serial_number=11682, 
                                start_timestamp='2000-01-01T00:00:00.000Z'):
        #
        #* Connect to the Influx database with the DataFrameClient
        influx_client = DataFrameClient(host=self.influx_host, 
                                    port=self.influx_port, 
                                    username=self.influx_user, 
                                    password=self.influx_password, 
                                    database=self.influx_database)
        #
        #* Connect to the Mongo database
        #ToDo Here I need to clarify how I can get connected with the mongo database
        #ToDo mongo_client = MongoClient(host=mongo_host, port=mongo_port, username=mongo_user, password=mongo_password,
        #ToDo                         authSource='admin', authMechanism='SCRAM-SHA-1')
        #
        mongo_client = MongoClient(host=self.mongo_host, port=self.mongo_port)
        mongo_database = mongo_client[self.mongo_database]
        mongo_collection = mongo_database[self.mongo_collection]
        #
        #########################
        #########################
        #* For each measurement of the Influx database perform the following operations:
        #*  1: Query the measurement and save the timestamp - value pairs in the data frame
        #*  2: Extend the dataframe with the supplementary information needed in each document of the Mongo db
        #*  3: Convert the dataframe to a dictionary
        #*  4: Save the dictionary to the machineDataCollection of the Mongo database
        #########################
        #########################
        #
        start_timestamp = int(strict_rfc3339.rfc3339_to_timestamp(start_timestamp) * pow(10,9))
        #
        for measurement in influx_measurements[0:3]:
            #*  1: Query the measurement and save the timestamp - value pairs in the data frame
            query_result = influx_client.query('SELECT * from %s WHERE time>%s' %(measurement, start_timestamp))
            df = query_result[measurement]
            df.rename(columns={'Value': 'values'}, inplace=True)
            #
            #* Remove the timestamp from the index of the dataframe and place it in the 'createdAt' column
            df['createdAt'] = df.index
            df.reset_index(inplace=True, drop=True)
            #
            #*  2: Extend the dataframe with the supplementary information needed in each document of the Mongo db
            df['machineType'] = machine_type
            df['machineId'] = machine_serial_number
            df['valueType'] = measurement
            df['version'] = 0
            #
            #* Rearrange the columns of the dataframe to match the order of the document key - value pairs in the collection 'machineDataCollection'
            df = df[['createdAt', 'machineId', 'machineType', 'valueType', 'values', 'version']]
            #
            #*  3: Convert the dataframe to a dictionary
            documents = df.to_dict('records')
            #
            #*  4: Save the dictionary to the machineDataCollection of the Mongo database
            mongo_collection.insert_many(documents)
#
#
if __name__ == "__main__":
    influx_to_mongo_parser = InfluxToMongoParser(influx_host='127.0.0.1', 
                                                influx_port=8086, 
                                                influx_user='', 
                                                influx_password='', 
                                                influx_database='M111682',
                                                influx_exclude_measurements=[], 
                                                mongo_host='127.0.0.1', 
                                                mongo_port=27017, 
                                                mongo_user='', 
                                                mongo_password='',
                                                mongo_database='test_mongo',
                                                mongo_collection='machineDataCollection')
    #
    influx_measurements = influx_to_mongo_parser.get_influx_measurements()
    influx_to_mongo_parser.parse_data_influx_to_mongo(influx_measurements=influx_measurements,
                                                    machine_type='CI400', 
                                                    machine_serial_number=11682,
                                                    start_timestamp='2000-01-01T00:00:00.000Z')
    #
    print("\n Finished in: \n --- %s sec ---" % round((time.time() - start_time), 2))
