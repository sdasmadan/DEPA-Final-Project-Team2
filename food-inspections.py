#!/usr/bin/env python
# coding: utf-8

# ## Chicago Food Inspections
# #### CGDL Group
# 
# In the modern world, consumers are more empowered than ever. They increasingly use freely- available tools on the internet to make informed decisions while relying less upon traditional trust-building sources such as word-of-mouth.While internet sources such as Yelp can provide valuable information, there remains significant opportunity to enhance the quality of data readily available to consumers.
# 
# To further empower consumers in the City of Chicago, CDGL Group plans to extract, analyze and data collected by the Chicago Department of Public Health Food Protection Program. These results will underpin a set of tools to improve customer access to the following information:
# 
# - Restaurant and other food establishment recommendations 
# - Cleanliness score
# - Locations with clean restaurants

# In[1]:

!pip install configparser
!pip install pymysql
!pip install sqlalchemy
!pip install datetime
!pip install usaddress
!pip install re

import pandas as pd # table manipulation
import numpy as np # number manipulation

from datetime import datetime,timedelta # time metrics
script_start = datetime.now() # Start Script

import re # string manipulation
import usaddress as add # address parsing

import configparser # read in MySQL connection attributes stored in a separate config file
import pymysql # import mysql package to fetch data in Python
from sqlalchemy import create_engine # SQL Package that helps connect to MySQL for DB Writes

# Add encoder of np.float64
pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
pymysql.converters.encoders[np.int64] = pymysql.converters.escape_int
pymysql.converters.conversions = pymysql.converters.encoders.copy()
pymysql.converters.conversions.update(pymysql.converters.decoders)


# In[2]:


# Create function to input Chicago Food Inspections dataset
def inputData(limit=50000,offset=0,start_date="",condition=">="):
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


# In[3]:


# Insert data for last 7 days
week_ago = (datetime.now() - timedelta(days=7)).date()
#df = inputData(start_date=week_ago,condition=">=").reset_index()

# Insert full dataset
df = inputData().reset_index()
df.head()


# Create functions `write_query` and `read_query` to insert and read data from that insert as a sanity check

# In[4]:


# Create function to insert data into MySQL relational instance
def write_query(sql,records,schema='foodinspection',execute='Many'):
    
    try:
    # Pull config information: Connection, Username, Password
        config = configparser.ConfigParser()
        config.read('food-inspection-config.ini')
        conn = config.get('mysql','Connection')
        user = config.get('mysql','Username')
        pwd = config.get('mysql','Password')

        # Connect to Sakila
        db = pymysql.connect(conn,user,pwd,schema)
        
        # Write SQL Query
        cursor = db.cursor()
        
        # Insert from Python or SQL
        if execute == 'Many':
            cursor.executemany(sql,records)
        else:
            cursor.execute(sql)
            
        db.commit()
        print("Successful Insert!")
    except Exception as e:
        print(e)
    finally:
        db.close()


# In[5]:


# Create function to read data into MySQL relational instance
def read_query(sql,schema='foodinspection',head=False):
    
    try:
        # Pull config information: Connection, Username, Password
        config = configparser.ConfigParser()
        config.read('food-inspection-config.ini')
        conn = config.get('mysql','Connection')
        user = config.get('mysql','Username')
        pwd = config.get('mysql','Password')

        # Connect to Sakila
        db = pymysql.connect(conn,user,pwd,schema)

        # Execute dataframe and output either first 5 rows or all depending on user choice
        if head == True:
            df = pd.read_sql(sql, con = db).head()
        else:
            df = pd.read_sql(sql, con = db)

        return df
    except Exception as e:
        print(e)


# ### Generate DML scripts for the `foodinspection` OLTP schema
# 
# Prepare `risk` table insert

# In[6]:


# Create risk list
risks = list(set(df.risk))
risks = [x for x in risks if str(x) != 'nan']

# Create risk_id list
nums = []
for risk in risks:
    if "1" in risk:
        nums.append(1)
    elif "2" in risk:
        nums.append(2)
    elif "3" in risk:
        nums.append(3)
    else:
        nums.append(4)
    
# Create tuple of rows to insert
records = [(nums[i],risks[i]) for i in range(0,len(risks))]

# Insert data
sql = """INSERT IGNORE INTO risk (risk_id,risk)
        VALUES (%s,%s)"""
write_query(sql, records)

# Read test
read_query("SELECT * FROM risk LIMIT 5")


# Prepare `result` table insert

# In[7]:


results_orig = list(set(df.results))

# Create result and conditon_flag columnns
results = []
conditions = []
for i in results_orig:
    if "w/ Condititions" in i:
        conditions.append(True)
    else:
        conditions.append(False)
    
    if "Pass" in i:
        results.append("Pass")
    else:
        results.append(i)
        
# Create tuple of rows to insert
records = list(set([(results[i],conditions[i]) for i in range(0,len(results))]))

# Insert data
sql = """INSERT IGNORE INTO result (result,condition_flag)
        VALUES (%s,%s)"""
write_query(sql, records)

# Read test
read_query("SELECT * FROM result LIMIT 5")


# Prepare `violation` table insert

# In[8]:


# Initiate violation df
viodf = df[["inspection_id","violations"]]
viodf = viodf[viodf.violations.notnull()]

# Split multiple violations frome each inspection id to rows
new_viodf = pd.DataFrame(viodf.violations.str.split("|").tolist(),index=viodf.inspection_id).stack()
new_viodf = new_viodf.reset_index([0,'inspection_id'])
new_viodf.columns = ['inspection_id','violation']

# Get rid of initial violation id
violations = new_viodf.violation.str.split('. ',n=1,expand=True)
violations.columns = ['number','violation']

# Split comments from violation
comments = violations.violation.str.split('- Comments:',n=1,expand=True)
comments.columns = ['violation','comment']

# Create inspection violation df
ins_vio_df = pd.DataFrame({'inspection_id':new_viodf.inspection_id,
                           'violation':comments.violation,
                           'comment':comments.comment})

# Create unique set of violations
records = list(set(ins_vio_df.violation))
records = [tuple([i]) for i in records]

# Insert data
sql = """INSERT IGNORE INTO violation (violation)
        VALUES (%s)"""
write_query(sql, records)

# Read test
read_query("SELECT * FROM violation LIMIT 5")


# Prepare `establishment` table insert

# In[9]:


# Reverse tuple of address qualifiers to place in dictionary
def reverseTuple(x):
    return (x[1],x[0])

# Extract address and create list of address components
def extractAddress(full_address):
    
    # Initialize lists
    try:
        add_temp = (add.parse(full_address))
        add_dict = dict(map(reverseTuple,add_temp))

        # Address number list
        if "AddressNumber" in add_dict:
            add_num = add_dict["AddressNumber"]
        else:
            add_num = ""

        # Address direction list
        if "StreetNamePreDirectional" in add_dict:
            add_dir = add_dict["StreetNamePreDirectional"]
        else:
            add_dir = ""

        # Address street list
        if "StreetName" in add_dict and "StreetNamePostType" in add_dict and "OccupancyIdentifier" in add_dict:
            add_street = add_dict["StreetName"] + " " + add_dict["StreetNamePostType"] + " " + add_dict["OccupancyIdentifier"]
        elif "StreetName" in add_dict and "StreetNamePostType" in add_dict:
            add_street = add_dict["StreetName"] + " " + add_dict["StreetNamePostType"]
        elif "StreetName" in add_dict:
            add_street = add_dict["StreetName"]
        else:
            add_street = ""

        # Address city list
        if "PlaceName" in add_dict:
            add_city = add_dict["PlaceName"]
        else:
            add_city = ""

        # Address state list
        if "StateName" in add_dict:
            add_state = add_dict["StateName"]
        else:
            add_state = ""

        # Address zip list
        if "ZipCode" in add_dict:
            add_zip = add_dict["ZipCode"]
        else:
            add_zip = ""
        
    # Error handling in the case that address is blank
    except:
        add_num = ""
        add_dir = ""
        add_street = ""
        add_city = ""
        add_state = ""
        add_zip = ""
    
    # Replace NaNs as they cannot go into MySQL database
    add_num = add_num.replace('nan','')
    add_dir = add_dir.replace('nan','')
    add_street = add_street.replace('nan','')
    add_city = add_city.replace('nan','')
    add_state = add_state.replace('nan','')
    add_zip = add_zip.replace('nan','')
        
    return add_num, add_dir, add_street, add_city, add_state, add_zip


# In[10]:


# Clean address
df["address"] = df.address.replace(" - ","-").replace(" -","-").replace("- ","-").fillna("")

# Clean city
df["city"] = df.city.fillna("")

# Clean zip code
df["zip"] = df.zip.apply(str)
df["zip"] = df.zip.fillna("")

# Clean state
df["state"] = df.state.fillna("")

# Clean facility type
df["facility_type"] = df.facility_type.fillna("")

# Create full address field for extractAddress function
full_address = df.address+df.city+" "+df.state+" "+df.zip

# Clean dba_name
df["dba_name"] = df.dba_name.fillna("")

# Clean aka_name
df["aka_name"] = df.aka_name.fillna("")

# Clean Latitude and Longitude
latitudes = [i if abs(i) > 0 else None for i in df.latitude]
longitudes = [i if abs(i) > 0 else None for i in df.longitude]

# Parse addresses
address_list = list(map(extractAddress,full_address.fillna("")))

# Create establishment table records with potential duplicates
records = list(set([(df.dba_name[i],df.aka_name[i])+address_list[i]+
                     (latitudes[i],longitudes[i],df.facility_type[i]) for i in range(0,df.shape[0])]))

# Insert data
sql = """INSERT IGNORE INTO establishment (dba_name,aka_name,address_num,address_direction,street,city,state,
                                            zip,latitude,longitude,facility_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
write_query(sql, records)

# Read test
read_query("SELECT * FROM establishment LIMIT 5")


# Prepare `inspection` table insert

# In[11]:


# Convert date string to date format YYYY-mm-dd
def datetrans(x):
    return x[:10]


# In[12]:


# Pull tables to pull the Ids
establishments = read_query("""SELECT * FROM establishment""")
risks = read_query("""SELECT * FROM risk""")
results = read_query("""SELECT * FROM result""")
results["results"] = [result[i]+" w/ Conditions" if results.condition_flag[i] else results.result[i] for i in range(0,len(results))]

# Create dataframe with merged data
df_link = df.merge(establishments, on=["dba_name","aka_name","latitude","longitude"],how="left")
df_link = df_link.merge(risks,on=["risk"],how="left")
df_link = df_link.merge(results,on=["results"],how="left")
df_link = df_link.where(pd.notnull(df_link), None)

# Find inspection table columns
inspection = df_link[['inspection_id','establishment_id','risk_id','inspection_date','inspection_type','result_id']].reset_index()
inspection["inspection_date"] = list(map(datetrans,inspection.inspection_date))
records = list(inspection.drop(columns=['index']).to_records())
records = [tuple(list(i)[1:]) for i in records]

# Insert data
sql = """INSERT IGNORE INTO inspection (inspection_id,establishment_id,risk_id,
            inspection_date,inspection_type,result_id)
        VALUES (%s,%s,%s,%s,%s,%s)"""
write_query(sql, records)

# Read test
read_query("SELECT * FROM inspection LIMIT 5")


# Prepare `inspection_violation` table insert

# In[13]:


# Read violation table to obtain ids
violation_df = read_query("select * from violation")

# Join on violation from previous dataframe
inspection_violation = ins_vio_df.merge(violation_df,on='violation')[['inspection_id','violation_id','comment']]
records = list(inspection_violation.to_records())
records = [tuple(list(i)[1:]) for i in records]

# Insert data
sql = """INSERT IGNORE INTO inspection_violation (inspection_id,violation_id,comment)
        VALUES (%s,%s,%s)"""
write_query(sql, records)

# Read test
read_query("SELECT * FROM inspection_violation LIMIT 5")


# ### Generate DML scripts for the `foodinspectionDW` OLAP schema
# 
# Prepare `dim_risk` table insert

# In[14]:


# Insert data
sql = """INSERT IGNORE INTO dim_risk (risk_id,risk)
        SELECT risk_id, risk from foodinspection.risk"""
write_query(sql, records,schema='foodinspectionDW',execute='One')

# Read test
read_query("SELECT * FROM dim_risk LIMIT 5",schema='foodinspectionDW')


# Prepare `dim_result` table insert

# In[15]:


# Insert data
sql = """INSERT IGNORE INTO dim_result (result_id,result,condition_flag)
        SELECT result_id, result, condition_flag from foodinspection.result"""
write_query(sql, records,schema='foodinspectionDW',execute='One')

# Read test
read_query("SELECT * FROM dim_result LIMIT 5",schema='foodinspectionDW')


# Prepare `dim_establishment` table insert

# In[16]:


# Insert data
sql = """INSERT IGNORE INTO dim_establishment (establishment_id, dba_name, aka_name, address_num,
       address_direction, street, city, state, zip, latitude, longitude, facility_type)
        SELECT establishment_id, dba_name, aka_name, address_num,
       address_direction, street, city, state, zip, latitude, longitude, facility_type 
       from foodinspection.establishment"""
write_query(sql, records,schema='foodinspectionDW',execute='One')

# Read test
read_query("SELECT * FROM dim_establishment LIMIT 5",schema='foodinspectionDW')


# Prepare `fact_inspection` table insert

# In[17]:


# Insert data
sql = """INSERT IGNORE INTO fact_inspection (inspection_id,establishment_id,inspection_date,
    inspection_type,violation,comment,risk_id,high_risk,medium_risk,low_risk,all_risk,result_id,pass,fail)
    SELECT i.inspection_id,e.establishment_id,i.inspection_date,i.inspection_type,
        v.violation,iv.comment,ri.risk_id,
        CASE WHEN ri.risk like '%High%' THEN 1 ELSE 0 end as high_risk,
        CASE WHEN ri.risk like '%Medium%' THEN 1 ELSE 0 end as medium_risk,
        CASE WHEN ri.risk like '%Low%' THEN 1 ELSE 0 end as low_risk,
        CASE WHEN ri.risk like '%All%' THEN 1 ELSE 0 end as all_risk,
        re.result_id, 
        CASE WHEN re.result like '%Pass%' THEN 1 ELSE 0 end as pass,
        CASE WHEN re.result like '%Fail%' THEN 1 ELSE 0 end as fail
    FROM foodinspection.inspection i
            LEFT JOIN foodinspection.establishment e on i.establishment_id = e.establishment_id
            LEFT JOIN foodinspection.risk ri on i.risk_id = ri.risk_id
            LEFT JOIN foodinspection.result re on i.result_id = re.result_id
            LEFT JOIN foodinspection.inspection_violation iv on i.inspection_id = iv.inspection_id
            INNER JOIN foodinspection.violation v on iv.violation_id = v.violation_id
            """
write_query(sql, records,schema='foodinspectionDW',execute='One')

# Read test
read_query("SELECT * FROM fact_inspection LIMIT 5",schema='foodinspectionDW')


# Create `food-inspections-OLTP.csv` for initial Tableau reporting

# In[18]:


oltp_csv = read_query("""
SELECT i.inspection_id, e.*, ri.*, i.inspection_date, i.inspection_type, re.*,v.*,iv.comment
FROM inspection i
    LEFT JOIN establishment e on i.establishment_id = e.establishment_id
    LEFT JOIN risk ri on i.risk_id = ri.risk_id
    LEFT JOIN result re on i.result_id = re.result_id
    LEFT JOIN inspection_violation iv on i.inspection_id = iv.inspection_id
    INNER JOIN violation v on iv.violation_id = v.violation_id
""")

oltp_csv.to_csv("food-inspections-OLTP.csv")


# Create `food-inspections-OLAP.csv` for final Tableau reporting

# In[19]:


olap_csv = read_query("""
SELECT fi.inspection_violation_id,fi.inspection_id,de.*,fi.inspection_date,fi.inspection_type,fi.violation,fi.comment,
    dri.*,fi.high_risk,fi.medium_risk,fi.low_risk,fi.all_risk,dre.*,fi.pass,fi.fail
FROM fact_inspection fi
    INNER JOIN dim_establishment de on fi.establishment_id = de.establishment_id
    INNER JOIN dim_risk dri on fi.risk_id = dri.risk_id
    INNER JOIN dim_result dre on fi.result_id = dre.result_id
""",schema="foodinspectionDW")

olap_csv.to_csv("food-inspections-OLAP.csv")


# **Provide metadata metrics**
# 
# Below are the list of transformations used to create MySQL DDL and DML scripts
# 
# **OLTP**
# - Manually associate `risk_id` with each distinct `risk` in the **`risk`** table
# - Parsed `result` in the **`result`** table to create an additional `condition_flag` if the inspection had any further check-ups
# - Split and stacked `violation` in the **`violation`** and **`inspection_violation`** from one row to many rows as all violations are grouped together parsed by a `|` per `inspection_id`
#     - Also split out each `violation` `comment` as each `violation` will have it's own respective `comment`
#     - Inspections can have multiple violations and each `violation` will have a `comment` detailing the `violation`
#     - This needed to be parsed out in the **`inspection_violation`** table to reduce redundancy in the dataset
# - Parsed the `address` components of each `establishment` in the **`establishment`** table using the `usaddress` package
#     - This needed to be done in an automated way due to addresses having different formats depending on the area of Chicago
#     
# **OLAP**
# - Create dimension tables for `dim_risk`, `dim_result` and `dim_establishment`
# - Created flags that can numerically aggregated based on type of `risk` and `result`

# In[20]:


# End script
script_end = datetime.now()
print("Total Script Duration:", script_end - script_start)
print("Seconds for a daily pull")

# Data size
print("\n# of rows:",df.shape[0])
print("# of columns:",df.shape[1])

print("\nOriginal Dataset size (MB):", df.memory_usage().sum()*1e-6)
print("Original Dataset size (GB):", df.memory_usage().sum()*1e-9)

print("\nOLTP dataset size (MB):", oltp_csv.memory_usage().sum()*1e-6)
print("OLTP dataset size (GB):", oltp_csv.memory_usage().sum()*1e-9)

print("\nOLAP dataset size (MB):", olap_csv.memory_usage().sum()*1e-6)
print("OLAP dataset size (GB):", olap_csv.memory_usage().sum()*1e-9)

