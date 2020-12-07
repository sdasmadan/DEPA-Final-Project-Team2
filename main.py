from flask import escape
import pandas as pd # table manipulation
import numpy as np # number manipulation
from datetime import datetime # time metrics
#import re # string manipulation
import usaddress as add # address parsing
import configparser # read in MySQL connection attributes stored in a separate config file
import pymysql # import mysql package to fetch data in Python
from sqlalchemy import create_engine # SQL Package that helps connect to MySQL for DB Writes

def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = 'World'
    return 'Hello {}!'.format(escape(name))
    


def inputData(limit=2000,offset=0,start_date="",condition=">="):
    # Initiate df
    columns = ['inspection_id', 'dba_name', 'aka_name', 'license_', 'facility_type',
           'risk', 'address', 'city', 'state', 'zip', 'inspection_date',
           'inspection_type', 'results', 'violations', 'latitude', 'longitude',
           'location']
    df = pd.DataFrame(columns=columns)
    
    # Input variables
    limit = limit # Able to pull 50k at a time
    offset = offset
    counter = 1
    rows = limit # Only to start while loop

    # Need to subset data by Inspection Date?
    if start_date != "":
        start_filter = "&$where=inspection_date"+condition+"'"+start_date+"'"
    else:
        start_filter = start_date

    start = datetime.now()

    # Conduct while loop to paginate through dataset until all data for query retrieved
    while limit == rows:
        iter_start = datetime.now()
        df_temp = pd.read_json("https://data.cityofchicago.org/resource/4ijn-s7e5.json?$limit="+
                          str(limit)+"&$offset="+str(offset)+"&$order=inspection_date"+start_filter)
        print("Time Between Iteration",counter,"-",datetime.now()-iter_start,"-",datetime.now()-start)
        rows = df_temp.shape[0]
        offset += limit
        df = pd.concat([df,df_temp])
        counter += 1
    print("Finished")
    
    return df