#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing the requests library 
import requests 
import json
  
def get_flow_chart(_arr,_version,_user):
    API_ENDPOINT = "https://jumperdevnew.appspot.com/api/make-flow-chart"
    data = {'version':str(_version),
        'userid':str(_user),
        'kru_admin_hahahaha':'true'} 
    
    # sending post request and saving response as response object 
    r = requests.post(url = API_ENDPOINT, data = data) 
    response =json.loads(r.text)
    #print(response)
    
    if response['success']:
        for i in response['nodes']:
            _arr.append((_user,_version,str(i['startsource']),i['priority']))


# In[2]:


from google.cloud import bigquery

#credentials
GOOGLE_APPLICATION_CREDENTIALS = '/home/jupyter/AnalyzeSentiment/jumperdevnew.json'

client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)

query = ( ## all users and their flows
    """with cte as 
(
SELECT u.referallShopname,u.__key__.id as uid
      ,a.* 
    ,b.* except (created,vertex,message,seller,__key__,__error__,__has_error__)
    ,b.message as message_message
    ,SPLIT(b.status, "_")[ORDINAL(1)] as Flow
FROM `jumperdevnew.datastore.ChatInbox` a 
join `jumperdevnew.datastore.Message` b
on a.vertex = cast(b.vertex as string)
join `jumperdevnew.datastore.User` u
on a.seller.id=u.__key__.id
)
select referallShopname,uid,Flow,count(distinct __key__.id) as messages
from cte
where SAFE_CAST(Flow AS INT64) is not null
group by 1,2,3
order by 4 desc
"""
)

print('Running Query...')

query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="US",
)


# In[11]:


##appending flow vertex to _arr
print('Hitting API...')

_arr=[]
for row in query_job:
    try:
        get_flow_chart(_arr,row['Flow'],row['uid'])
    except:
        print(row)


# In[ ]:


##creating table 

print('Loading to Bigquery...')

dataset_id = 'datastore'
dataset_ref = client.dataset(dataset_id)

# Prepares a reference to the table
table_ref = dataset_ref.table('_FlowChart')


# Dropping OrderVariables table if present
try:
    client.delete_table(client.get_table(table_ref))
except:
    print('No Table Present')
    
#specifying the schema and creating the table
schema = [
            bigquery.SchemaField('user', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('action', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('vertex', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('priority', 'INT64', mode='NULLABLE')
        ]
    
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)

##upload data
client.insert_rows(client.get_table(table_ref), _arr)


