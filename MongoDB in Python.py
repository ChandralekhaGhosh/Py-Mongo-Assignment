#!/usr/bin/env python
# coding: utf-8

# ### Tasks:
# 
# * Visit the link: https://archive.ics.uci.edu/ml/datasets/Carbon+Nanotubes
# * Download the database
# * Insert Bulk CSV data in mongo
# * Perform differentv Operations

# In[1]:


import logging as lg
import pymongo
import csv
import json
import pandas as pd


# #### Creating log file

# In[2]:


lg.basicConfig(filename='mongotask.log', level=lg.INFO, format='%(asctime)s %(message)s %(levelname)s')


# In[3]:


client = pymongo.MongoClient("mongodb://test:test@cluster0-shard-00-00.xisoe.mongodb.net:27017,cluster0-shard-00-01.xisoe.mongodb.net:27017,cluster0-shard-00-02.xisoe.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-5czxgc-shard-0&authSource=admin&retryWrites=true&w=majority")
    


# #### Connect with db, creating a database and collection

# In[4]:


#### 1.Connect with db & creating a database

try:
    #establishing a connection with mongoDB
    client = pymongo.MongoClient("mongodb://test:test@cluster0-shard-00-00.xisoe.mongodb.net:27017,cluster0-shard-00-01.xisoe.mongodb.net:27017,cluster0-shard-00-02.xisoe.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-5czxgc-shard-0&authSource=admin&retryWrites=true&w=majority")
    
    # creating a DB
    
    db=client['mongo_in_py']
    lg.info('A database, name {} has been created'.format(db))

except Exception as e:
    lg.info('Error while connecting with mongoDB',e)
    
        


# In[5]:


## To verify if the database has been created
client.list_database_names()

#all database names will not show up the list untill we insert data


# In[6]:


#### Creating a collection
try:
    coll=db['mongo_task']
    lg.info('A collection, name {} has been created'.format(coll))
    
except Exception as e:
    lg.info('Error while creating a collection with mongoDB',e)


# #### Reading a csv file in python

# In[7]:


#2. Reading csv file in python

try:
    data_carbon=pd.read_csv('carbon_nanotubes.csv',delimiter=';')
    lg.info('A csv file named{} has been successfully read'.format(data_carbon))
except Exception as e:
    lg.info('Error while reading the csv file in python',e)


# In[8]:


#checking the top 10 records of data_carbon
data_carbon.head(10)


# #### Creating a function to insert all the rows & columns as documents in mongoDB collection in bulk 

# In[9]:


#Creating a function to insert all the rows & columns as documents in mongoDB collection in bulk 
def insertMany(csv_name,collection):
    
    ''' To insert all the rows of the csv file into
        mongo collection in a go'''
    
    try:
        data_jsn1=json.loads(csv_name.to_json(orient='records'))
        collection.insert_many(data_jsn1)
        lg.info('CSV rows & columns have been inserted into mongo collection successfully')
        return collection
    except Exception as e:
        lg.info('Errors occurred while inserting documents in mongo collection')


# In[10]:


insertMany( data_carbon,coll)


# In[11]:


#To find records in collection
for i in coll.find():
    print(i)


# #### Different Operations
# 
# #### single Insertion-Inserting the below document in mongo collection, coll

# In[12]:


# a) single Insertion-Inserting the below document in mongo collection, coll
lg.info('A single row has been inserted in the collection')

dict1={'name':'Atu', 
       'email':'atu@gmail.com', 
       'product':['static','video','text'],
        'company': 'LA LA LAND'
      }
coll.insert_one(dict1)

#To check
for i in coll.find():
    print(i)


# In[13]:


#Deleting the above document
coll.delete_one({'name':'atu'})


# In[14]:


for i in coll.find():
    print(i)


# #### b) Filtering the document where Chiral indice n= 2 

# In[15]:


lg.info('filtering the documents whereChiral indice n= 2 ')
for i in coll.find({'Chiral indice n': 2}):
    print(i)


# #### b) Filtering the document where Chiral indice n= 2 or 8
# 
# #### Doubt: do all documents get printed? 

# In[18]:


lg.info('Filtering the document where Chiral indice n= 2 or 8')
try:
    for i in coll.find({'Chiral indice n': {'$in':[2,8]}}):
        print(i)
except Exception as e:
    
    lg.info('Some errors while filtering occurred',e)


# #### c) Filtering all documents where Chiral indice m is greater than 5

# In[21]:


lg.info('Filtering all documents where Chiral indice m is greater than 5')
try:
    for i in coll.find({'Chiral indice m' : {'$gt' : 5}}):
        print(i)
except Exception as e:
        lg.info('Some errors while filtering occurred',e)


# #### c) Filtering all documents where Chiral indice m is lesser than 5

# In[22]:


lg.info('Filtering all documents where Chiral indice m is lesser than 5')
try:
    for i in coll.find({'Chiral indice m' : {'$lt' : 5}}):
        print(i)
except Exception as e:
        lg.info('Some errors while filtering occurred',e)


# #### d) Filtering all documents where Chiral indice m not greater than 5

# In[23]:


lg.info('Filtering all documents where Chiral indice m not greater than 5')
try:
    for i in coll.find({'Chiral indice m' : {'$not' : {'$gt' : 5}}}):
        print(i)
except:
    lg.info('Some errors while filtering occurred',e)


# #### e) Filtering all documents where Chiral indice m is equal to 5 or 6

# In[24]:


lg.info('Filtering all documents where Chiral indice m is equal to 5 or 6')
try:
    for i in coll.find({'Chiral indice m' : {'$in' : [5,6]}}):
        print(i)
except Exception as e:
    lg.info('Some errors while filtering occurred',e)


# #### f) Filtering all documents where Chiral indice n is greater than 2 and less than equal to 6

# In[25]:


lg.info('Filtering all documents where Chiral indice n is greater than 2 and less than equal to 6')
try:
    for i in coll.find({'Chiral indice n' : {'$gt' : 2} and {'$lte' : 6}}):
        print(i)
except Exception as e:
        lg.info('Some errors while filtering occurred',e)


# #### f) updating the documents where Chiral indice n is 2, replacing it with 200

# In[26]:


lg.info('updating the documents where Chiral indice n is 2, replacing it with 200')
try:
    coll.update_many({'Chiral indice n': 2}, {'$set':{'Chiral indice n' : 200}})
except Exception as e:
    lg.info('Some errors while updating occurred',e)

for i in coll.find():
    print(i)


# #### f) updating the documents Chiral indice n is lesser than equal to 5, replacing Chiral indice n with 500

# In[27]:


lg.info('updating the documents Chiral indice n is lesser than equal to 5, replacing Chiral indice n with 500')
try:
    coll.update_many({'Chiral indice n' :{'$lte' : 5}},{'$set' : {'Chiral indice n' : 500}})
except Exception as e:
    lg.info('Some errors while updating occurred',e)

for i in coll.find():
    print(i)


# #### g) updating the documents Chiral indice n is lesser than equal to 5, replacing Chiral indice m with *

# In[28]:


lg.info('updating the documents Chiral indice n is lesser than equal to 5, replacing Chiral indice m with *')
try:
    coll.find_one_and_update({'Chiral indice n' :{'$lte' : 8}}, {'$set' : {'Chiral indice m' : 1000}})
except Exception as e:
    lg.info('Some errors while updating occurred',e)
    
for i in coll.find():
    print(i)    


# #### h) Deleteing the documents where Chiral indice n is 8

# In[29]:


lg.info('Deleteing the documents where Chiral indice n is 8')
try:
    coll.delete_many({'Chiral indice n' : 8})
except Exception as e:
    lg.info('Some erros occured in deleting the documents')
    
for i in coll.find():
    print(i)


# In[30]:


pip install gitpython


# In[31]:


import git


# In[ ]:




