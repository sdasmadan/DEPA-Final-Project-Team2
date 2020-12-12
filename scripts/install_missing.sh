#!/bin/bash

sudo apt-get update
sudo apt-get -y -qq --fix-missing install python3-pandas python3-numpy

pip install configparser
pip install pymysql
pip install sqlalchemy
pip install datetime
pip install usaddress
pip install re