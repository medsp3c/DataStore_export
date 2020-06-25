#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import bigquery 

import pandas as pd


# In[2]:


#credentials
#GOOGLE_APPLICATION_CREDENTIALS = "C:\\Users\Jumper.ai\Desktop\Projects\packages\jumper.json"
GOOGLE_APPLICATION_CREDENTIALS = '/home/jupyter/AnalyzeSentiment/jumperdevnew.json'

client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)


# In[3]:


static_views=['v_seller_key','v_Chat_Statistics','v_ChatInbox_to_Order','v_Funnel_state','v_ChatInbox_sentiment'] #'v_User_AgeGender',,'v_Funnel_state_2'


# In[12]:


def createStaticView(_view):
    ##creating table 

    dataset_id = 'datastore'
    dataset_ref = client.dataset(dataset_id)
    
    #get schema from current view 
    v_table_ref = dataset_ref.table( _view)
    v_table = client.get_table(v_table_ref)
    schema = v_table.schema
    
    # Prepares a reference to the table
    sv_table_ref = dataset_ref.table('s%s' % _view)
    
    # Dropping OrderVariables table if present
    try:
        client.delete_table(client.get_table(sv_table_ref))
    except:
        print('No Table Present')
    
    #create static view table
    table = bigquery.Table(sv_table_ref, schema=schema)
    table = client.create_table(table)
    
    #loading view data into table
    job_config = bigquery.QueryJobConfig()
    job_config.destination = sv_table_ref

    query = ( ## all users and their flows
        """select * FROM `jumperdevnew.datastore.%s`""" % _view
    )
    query_job = client.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location="US",
        job_config=job_config,
    )

    query_job.result()  # Waits for the query to finish
    print("Query results loaded to table {}".format(sv_table_ref.path))


# In[13]:


for v in static_views:
    try:
        createStaticView(v)
    except:
        print('error with ', v)

