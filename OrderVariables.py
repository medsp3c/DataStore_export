#!/usr/bin/env python
# coding: utf-8

# In[1]:


#encoding=utf-8
from google.cloud import datastore
import json
from datetime import datetime
from datetime import timedelta  

#credentials
#GOOGLE_APPLICATION_CREDENTIALS = "C:\\Users\Jumper.ai\Desktop\Projects\packages\jumper.json"
GOOGLE_APPLICATION_CREDENTIALS = '/home/jupyter/AnalyzeSentiment/jumperdevnew.json'

datastore_client = datastore.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)


# In[2]:


# Create a function called "chunks" with two arguments, l and n:
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]


# In[3]:


def generateDates(chunksize=7,chunks=100):
    _arr=[]
    _t=datetime.today()
    for i in range(chunks):
        _arr.append([_t-timedelta(days=7),_t])
        _t=_t-timedelta(days=7)
    return _arr
        
dates=generateDates()


# In[4]:


from google.cloud import bigquery
client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
dataset_id = 'datastore'
dataset_ref = client.dataset(dataset_id)


# In[5]:


# Prepares a reference to the table
table_ref = dataset_ref.table('ordervariables')

#get table description and labels
table_description=client.get_table(table_ref).description
table_labels=client.get_table(table_ref).labels

# Dropping OrderVariables table if present
try:
    client.delete_table(client.get_table(table_ref))
except:
    print('No Table Present')
    
#specifying the schema and creating the table
schema = [
            bigquery.SchemaField('ordernumber', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('variables', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE')
        ]
    
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)
print('table {} created.'.format(table.table_id))

#updating description and labels
table.description=table_description
table.labels=table_labels
table = client.update_table(table, ["description","labels"])


# In[7]:


for date in dates:

    #get Orders data from Datastore
    query = datastore_client.query(kind='Order')
    #sellerkey = datastore_client.key('User', 6738413555286016)
    #query.add_filter('seller', '=', sellerkey)
    query.add_filter('created', '>=', date[0])
    query.add_filter('created', '<', date[1])
    results = list(query.fetch())

    print(date[0],len(results))

    for chunk in chunks(results, 5000):
        _arr=[]
        for r in chunk:
            if'variables' in r and r['variables']: # variables present with actual value
                try: #in json format
                    if isinstance(data, bytes):
                        _arr.append((r.id, r['variables'].decode('utf-8'),r['created']))
                    else: 
                        _arr.append((r.id, str(r['variables']),r['created']))
                except: # in any other format
                    _arr.append((r.id, str(r['variables']),r['created']))
            else: #empty string when variable is not available
                _arr.append((r.id, '',r['created']))
        #inserting data to bigquery
        errors = client.insert_rows(client.get_table(table_ref), _arr)

        print(len(_arr))

