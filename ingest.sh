#!/bin/bash

sudo apt-get install wget

# remove older copy of file, if it exists
rm -f foodinspectiondata.csv

# download latest data from Chicago Data Portal
wget https://data.cityofchicago.org/resource/4ijn-s7e5.csv -O foodinspectiondata.csv