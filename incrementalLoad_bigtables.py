import BigQuery_DatastoreIncrementalLoad as bq

tables = [{'table':'ConversationAgentlog','dataset':'datastore','created':'created', 'hour' : 24, 'chunksize' : 60 }
            ,{'table':'ConversationAgentlog','dataset':'datastore','created':'stopped', 'hour' : 0, 'chunksize' : 60 }
           ,{'table':'Dashboard_conversation','dataset':'datastore','created':'created', 'hour' : 24, 'chunksize' : 60 }
           ,{'table':'User','dataset':'datastore','created':'lastActiveTime','hour': 0, 'chunksize' : 30 }
           ,{'table':'ProxyUser','dataset':'datastore','created':'created','hour': 0, 'chunksize' : 60 }
           ,{'table':'TouchPoint','dataset':'datastore','created':'created','hour': 0, 'chunksize' : 60 }
           ,{'table':'Authkey','dataset':'datastore','created':'created','hour': 0, 'chunksize' : 60 }
           ,{'table':'Variation','dataset':'datastore','created':'created','hour': 0, 'chunksize' : 60 }
           ,{'table':'Conversation','dataset':'datastore','created':'time','hour': 0, 'chunksize' : 60 , 'byte_field' : 'headerinfo'}
           ,{'table':'Order','dataset':'datastore','created':'time','hour': 0, 'chunksize' : 60, 'byte_field' : 'variables'}
           ,{'table':'ConversationCustomField','dataset':'datastore','created':'updated','hour': 0, 'chunksize' : 60}
           ,{'table':'Messenger','dataset':'datastore','created':'created', 'hour' : 0, 'chunksize' : 60 }
           ,{'table':'Cart','dataset':'datastore','created':'modified', 'hour' : 0, 'chunksize' : 60 }
           ,{'table':'Historic_dashboard_conversation','dataset':'datastore','created':'created', 'hour' : 0, 'chunksize' : 60 }
]

for t in tables:
    try:
        if 'byte_field' in t:
            bq.incrementalLoad(t['table'],t['dataset'],t['created'],t['hour'],t['chunksize'],t['byte_field'])
        else:
            bq.incrementalLoad(t['table'],t['dataset'],t['created'],t['hour'],t['chunksize'])
    except Exception as e:
        print('error on ',t['table'],':',e)