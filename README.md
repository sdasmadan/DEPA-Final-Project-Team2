# DEPA-Final-Project-Team2
Data Engineering Platforms Final Project Team 2

Please refer to https://github.com/samgunther/DEPA-Final-Project-Team2 for official project submission. GitHub contains a packaged version of the Tableau workbook with an extract, which Canvas could not support due to size. See bottom of README for further detail.

##### INSERT TO MYSQL #####

#### 1 Login to GCP instance using SSH from "DEPA FINAL PROJECT TEAM 2" project on GCP Console instance-2

cd ..
cd sam_peter_gunther

#2 Refresh Git Hub Repo (As needed)

rm -rf DEPA-Final-Project-Team2
git clone https://github.com/samgunther/DEPA-Final-Project-Team2

#3 Enter Repo

cd DEPA-Final-Project-Team2

#4 Run python to insert to mySQL tables

python3 food-inspections.py

#5 Validate inserts are successful (password = 'rootroot')

sudo apt-get install default-mysql-server
mysql --host=34.66.161.208 --user=root --password
use foodinpection;
show tables;
select * from inspection LIMIT 5;


##### RUN DASHBOARDS #####

Open up 'Dashboards v3.twbx' (or latest from GitHub repo)

Use same login from above to login and connect to MySQL Server 
IP: 34.66.161.208
Port: 3306

Equivalent version 'Dashboards v3.twb' connects directly to GCP MySQL instance
***We recommend using the dashboards off of the extract data instead of the live data, per best performance


