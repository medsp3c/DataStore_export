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


from google.cloud import bigquery
client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
dataset_id = 'datastore'
dataset_ref = client.dataset(dataset_id)


# In[5]:


# Prepares a reference to the table
table_ref = dataset_ref.table('ChatInbox_template')

#get table description and labels
table_description=client.get_table(table_ref).description
table_labels=client.get_table(table_ref).labels


# In[2]:


##getting latest timestamp from bq
query="""select max(created) as created from datastore.ChatInbox_template"""

query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="US",
)

rows = query_job.result() 
for row in rows:
    created = row[0]
    
print('last message at',created)


# In[3]:


#retrieving data after date
query = datastore_client.query(kind='ChatInbox')
query.add_filter('created', '>', created)
results = list(query.fetch())
print('results', len(results))


# In[4]:


#processing the records for bq tble
def processRecord(r):
    _arr = []
    fields = ['created','replied']
    keys = []
    byte_fields = ['template']
    
    #adding key
    _arr.append(r.id) 
    
    #adding non-key fields
    for f in fields:
        _arr.append(r[f])
    
    #adding key type fields
    for k in keys:
        try:
            _arr.append(r[k].id)
            _arr.append(r[k].kind)
        except:
            _arr.append(None)
            _arr.append(None)
            
    for b in byte_fields:
        try:
            _arr.append(r[b].decode('utf-8'))
        except:
            _arr.append(None)

    _tuple = tuple(_arr)
    return _tuple

processed_results = [processRecord(r) for r in results]


# In[6]:


#keep only images
_arr = []
for r in processed_results:
    if '"attachmenttype":"image"' in r[3]:
        _arr.append(r)
print('images received :', len(_arr))


# In[7]:


for chunk in chunks(_arr, 5000):

    #inserting data to bigquery temp table
    client.insert_rows(client.get_table(table_ref), chunk)

    print(len(chunk))

