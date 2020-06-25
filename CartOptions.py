#!/usr/bin/env python
# coding: utf-8

# In[31]:



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
table_ref = dataset_ref.table('cartoptions')

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
            bigquery.SchemaField('cart_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('varname', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('savetype', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('name', 'STRING', mode='NULLABLE')
        ]
    
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)
print('table {} created.'.format(table.table_id))

#updating description and labels
table.description=table_description
table.labels=table_labels
table = client.update_table(table, ["description","labels"])

# In[32]:


dates=generateDates()
for date in dates:

    #get Orders data from Datastore
    query = datastore_client.query(kind='Cart')
    #sellerkey = datastore_client.key('User', 6738413555286016)
    #query.add_filter('seller', '=', sellerkey)
    query.add_filter('created', '>=', date[0])
    query.add_filter('created', '<', date[1])
    results = list(query.fetch())

    print(date[0],len(results))

    for chunk in chunks(results, 5000):
        _arr=[]
        for r in chunk:
            if'options' in r and r['options']: # option present with actual value
                print(r['options'][0])
                try: #in json format
                    for _option in r['options']:
                        _o=json.loads(str(_option,'utf-8'))
                        _arr.append((r.id,r['created'],_o['varname'], _o['savetype'], _o['id'], _o['name']))
                except: # in any other format
                    print(_option)

        #inserting data to bigquery
        if len(_arr)>0:
            errors = client.insert_rows(client.get_table(table_ref), _arr)

        print(len(_arr))



