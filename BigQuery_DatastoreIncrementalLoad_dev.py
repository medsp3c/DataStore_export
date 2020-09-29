

from google.cloud import bigquery 
from google.cloud import datastore
from datetime import datetime
from datetime import timedelta  

import pandas as pd
import time


# In[5]:


#credentials
#GOOGLE_APPLICATION_CREDENTIALS = "C:\\Users\Jumper.ai\Desktop\Projects\packages\jumper.json"
#dev_GOOGLE_APPLICATION_CREDENTIALS = "C:\\Users\Jumper.ai\Desktop\Projects\packages\jumperdevnew.json"
GOOGLE_APPLICATION_CREDENTIALS = '/home/brian/jumperdevnew.json'

client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)

datastore_client = datastore.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)


# In[6]:



def incrementalLoad(_table, _dataset,_created,_hours = 0, _chunksize = 60, byte_field = None ):
    #_table - table name
    #_dataset - dataset name
    #_created - time field to filter data for
    #_hours - # of hours of data to be replaced ie 24 for the past 24 hours data to be replaced
    #_chunksize - size of chunks to be extracted from datastore ie 60 for 60 min blocks
    #byte_field - name of field in bytes to be decoded
    
    print('Loading ',_dataset, _table)
    
    #prep phase
    dataset_id = _dataset
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(_table)
    t_table_ref = dataset_ref.table('t_' + _table)
    table = client.get_table(table_ref)  # Make an API request.

    original_schema = table.schema
    
    #recreate temp table
    try:
        client.delete_table(client.get_table(t_table_ref))
    except:
        print('No Table Present')

    #specifying the schema and creating the table

    t_table = bigquery.Table(t_table_ref, schema=original_schema)
    t_table = client.create_table(t_table)
    print('table {} created.'.format(t_table.table_id))
    
   
    #getting latest created
    query = ( ## all users and their flows
            """select max(%s) FROM `jumperdevnew.%s.%s`""" % (_created,_dataset,_table) # time for conversation
        )

    query_job = client.query(
            query,
            # Location must match that of the dataset(s) referenced in the query.
            location="US",
            #job_config=job_config,
        )

    for row in query_job.result():  # Waits for the query to finish
        created = row[0]
        print('Last entry at',created)

    if byte_field: 

        table_byte_ref = dataset_ref.table(_table + '_' + byte_field)
        t_table_byte_ref = dataset_ref.table('t_' + _table + '_' + byte_field)
        table_byte_ref = client.get_table(table_byte_ref)
        
        byte_schema = table_byte_ref.schema
        
        #recreate temp table
        try:
            client.delete_table(client.get_table(t_table_byte_ref))
        except:
            print('No Byte Table Present')
            
        #specifying the schema and creating the table

        t_table_byte = bigquery.Table(t_table_byte_ref, schema=byte_schema)
        t_table_byte = client.create_table(t_table_byte)
        print('table {} created.'.format(t_table_byte.table_id))

        #if date from byte table is earlier, use that of byte table
        query = ( ## all users and their flows
                """select max(%s) FROM `jumperdevnew.%s.%s`""" % (_created,_dataset,_table + '_' + byte_field) # time for conversation
            )

        query_job = client.query(
                query,
                # Location must match that of the dataset(s) referenced in the query.
                location="US",
                #job_config=job_config,
            )

        for row in query_job.result():  # Waits for the query to finish
            b_created = row[0]
            print('Byte Last entry at',b_created)
        
        if b_created<created:
            created = b_created
            print('Byte created used')

    def generateDates(seed,chunksize=1,chunks = 100):
        _arr=[]
        _t=seed
        for i in range(chunks): # chunk sizes in minutes
            _arr.append([_t,_t+timedelta(minutes=chunksize)])
            _t=_t+timedelta(minutes=chunksize)
            if _t > datetime.utcnow().replace(tzinfo=created.tzinfo): #stop when after now time in utc
                break

        return _arr

    dates = generateDates(created - timedelta(hours = _hours),chunksize = _chunksize, chunks = 30) # generate from x hours before

    # Create a function called "chunks" with two arguments, l and n:
    def chunks(l, n):
        # For item i in a range that is a length of l,
        for i in range(0, len(l), n):
            # Create an index range for l of n items:
            yield l[i:i+n]

    def RecordToDict(_record):
        _dict = {'namespace': ''
                 , 'app': ''
                 , 'path': ''
                 , 'kind': _record.kind
                 , 'name': _record.name
                 , 'id': _record.id
                }
        return _dict



    def formatInstructions(_schema):
        _format_instructions = []
        for field in _schema:

            # to create field manually for error and has_error
            if field.name in ('__error__','__has_error__'):
                _skip = True
            else:
                _skip = False 

            #default value if data is not found
            if field.mode == 'REPEATED':
                _fallback = []
            else:
                _fallback = None

            #convert to dictionary
            _dict = {}
            _dict['name'] = field.name
            _dict['field_type'] = field.field_type
            _dict['mode'] = field.mode
            _dict['fallback'] = _fallback
            _dict['skip'] = _skip

            if field.field_type =='RECORD': #if it is a record, parse fields as well
                _dict['fields'] = formatInstructions(field.fields)
                _dict['fields_name'] = [ f['name'] for f in _dict['fields'] ]

            #append to array
            _format_instructions.append(_dict)

        return _format_instructions


    def processRecord(r,_instruction):
    #if True:    
        _arr = []

        for i in _instruction:
            if i['name'] == '__key__': #when it is the key of table
                _arr.append(RecordToDict(r.key))
            elif i['field_type'] == 'RECORD' and i['mode'] == 'REPEATED':  #when its a Kind and array
                try:
                    _arr.append([RecordToDict(x) for x in r[i['name']]])
                except:
                    _arr.append(i['fallback'])
            elif i['field_type'] == 'RECORD' and 'string' in i['fields_name']: # when its a record due to data type issues
                try:
                    _arr.append({'string':r[i['name']]})
                except:
                    _arr.append(i['fallback'])                
            elif i['field_type'] == 'RECORD': #when its a Kind
                try:
                    _arr.append(RecordToDict(r[i['name']]))
                except:
                    _arr.append(i['fallback'])
            elif i['skip']: #when it is a field to skip
                _arr.append(i['fallback'])

            elif i['name'] in r.keys(): #all other fields
                _arr.append(r[i['name']])
            else:
                _arr.append(i['fallback'])

        _tuple = tuple(_arr) #convert to tuple
        return _tuple

    def procressByte(r,_field,_created): ##purely decoding byte field
        try:
            out_byte = r[_field].decode('utf-8')
        except:
            out_byte = None
        
        return (r.id,r[_created],out_byte)
        

    _format_instructions = formatInstructions(original_schema)
    #_tuple
    
    #Insertion phase
    for d in dates:
        print('time starting', d[0])
        
        #retrieving data after date
        query = datastore_client.query(kind=_table)
        query.add_filter(_created, '>', d[0])
        query.add_filter(_created, '<=', d[1])
        results = list(query.fetch())
        print('results', len(results))
        
        if len(results) > 0: #only when there are results
            processed_results = [processRecord(r,_format_instructions) for r in results]

            for chunk in chunks(processed_results, 5000):

                #inserting data to bigquery temp table
                client.insert_rows(client.get_table(t_table_ref), chunk)

                print('loaded',len(chunk))
            
            if byte_field:
                processed_results_b = [procressByte(r,byte_field,_created) for r in results]

                for chunk in chunks(processed_results_b, 5000):

                    #inserting data to bigquery temp table
                    client.insert_rows(client.get_table(t_table_byte_ref), chunk)

                    print('loaded byte',len(chunk))
    
    #migration phase
    #moving data from temp table to table
    time.sleep(3) #delaying delete query in hope that temp table is properly filled first

    d_query = ( """Delete from `jumperdevnew.%s.%s` 
                where __key__.id in (select __key__.id from `jumperdevnew.%s.t_%s`) """ % (_dataset,_table,_dataset,_table))
    print('Delete query :', d_query)
    d_query_job = client.query(
            d_query,
            # Location must match that of the dataset(s) referenced in the query.
            location="US"
        )
    d_query_job.result()        
    
    #moving data from temp table to table
    i_query = ( """Insert into `jumperdevnew.%s.%s` 
                    SELECT * except (pos) 
                    FROM ( SELECT *,
                            LEAD(__key__.id) OVER (PARTITION BY __key__.id ORDER BY %s) pos
                            FROM `jumperdevnew.%s.t_%s` ) a
                          WHERE pos IS NULL """ % (_dataset,_table,_created,_dataset,_table))
    print('Insert query :', i_query)
    i_query_job = client.query(
            i_query,
            # Location must match that of the dataset(s) referenced in the query.
            location="US"
        )
    i_query_job.result() 
    
    if byte_field:
        d_query = ( """Delete from `jumperdevnew.%s.%s_%s` 
                where id in (select id from `jumperdevnew.%s.t_%s_%s`) """ % (_dataset,_table,byte_field,_dataset,_table,byte_field))
        print('Delete query :', d_query)
        d_query_job = client.query(
                d_query,
                # Location must match that of the dataset(s) referenced in the query.
                location="US"
            )
        d_query_job.result()        

        #moving data from temp table to table
        i_query = ( """Insert into `jumperdevnew.%s.%s_%s` 
                    SELECT * except (pos) 
                    FROM ( SELECT *,
                            LEAD(id) OVER (PARTITION BY id ORDER BY %s) pos
                            FROM `jumperdevnew.%s.t_%s_%s` ) a
                          WHERE pos IS NULL """ % (_dataset,_table,byte_field,_created,_dataset,_table,byte_field,))
        print('Insert query :', i_query)
        i_query_job = client.query(
                i_query,
                # Location must match that of the dataset(s) referenced in the query.
                location="US"
            )
        i_query_job.result() 