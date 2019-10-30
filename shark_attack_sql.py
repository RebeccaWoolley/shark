#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import csv
from datetime import datetime, timedelta
import re


# In[2]:


sharkfile = r'c:\data\GSAF5.csv'


# In[3]:


attack_dates = []
case_number = []
country = []
activity = []
age = []
gender = []
isfatal = []
with open(sharkfile, encoding = 'UTF8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        attack_dates.append(row['Date'])
        case_number.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])


# In[4]:





# In[6]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()

cur.execute('truncate table becky.shark')

data = zip(attack_dates, case_number, country, activity, age, gender, isfatal)
q = 'insert into becky.shark(attack_date, case_number, country,  activity, age,  gender, isfatal) values(?, ?, ?, ?, ?, ?, ?)'

for d in data:
    try:
        cur.execute(q,d)
        conn.commit()
    except:
        conn.rollback()
        
