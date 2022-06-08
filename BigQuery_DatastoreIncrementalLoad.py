

from google.cloud import bigquery 
from google.cloud import datastore
from datetime import datetime
from datetime import timedelta  

import pandas as pd
import time

import logging


client = bigquery.Client()
datastore_client = datastore.Client()



def incrementalLoad_cursor(_table, _dataset,_created,_minutes = 0, partition_field = None ):
    """
    Loads data from Datastore to Bigquery using cursors
    #_table - table name
    #_dataset - dataset name
    #_created - time field to filter data for
    #_minutes - # of minutes of data to be replaced ie 60 for the past 1 hour data to be replaced
    #byte_field - name of field in bytes to be decoded
    
    returns dict with number of records expected to load and number of records that failed to load
    """
    
    
    logging.info('Loading  ' + _dataset + ' '+ _table)

    def mapObjectToBQColumn(_obj ):
        """maps pythonic object types from  to BQ data types"""
        if isinstance(_obj,datastore.key.Key):
            return "reference"
        elif isinstance(_obj,bool):
            return "boolean"
        elif isinstance(_obj,str):
            return "string"
        elif isinstance(_obj,int):
            return "integer"
        elif isinstance(_obj,bytes):
            return "blob"
        elif hasattr(_obj,'timestamp'):
            return "date_time"
        else:
            return None

    def MultiDataTypeHandling(_obj, _field_names):
        """check object's data type against fields names and return a dict suitable for loading to BQ"""
        data_type = mapObjectToBQColumn(_obj)

        if data_type and data_type in _field_names:
            if data_type == "reference":
                return {data_type: RecordToDict(_obj), "provided" : data_type}
            else:
                return {data_type: _obj, "provided" : data_type}
        elif data_type == "string" and "text" in _field_names:
            return {"string": _obj, "provided" : "text" }
        elif data_type not in _field_names:
            return {"provided" : f"Error: {data_type} not found"}
        else:
            return {}

    def RecordToDict(_record):
        """formats a datastore key to a dict suitable for loading to BQ"""
        _dict = {'namespace': ''
                 , 'app': ''
                 , 'path': ''
                 , 'kind': _record.kind
                 , 'name': _record.name
                 , 'id': _record.id
                }
        return _dict


    def formatInstructions(_schema):
        """creates formating instructions based on Bigquery Table schema"""
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
        """processes a single Datastore record using instructions from BQ Schema"""
    #if True:    
        _arr = []

        for i in _instruction:
            if i['name'] == '__key__': #when it is the key of table
                _arr.append(RecordToDict(r.key))

            elif i['field_type'] == 'RECORD' and 'provided' in i['fields_name']:  #multiple data type issues

                if r[i['name']] and i['mode'] == 'REPEATED': #when data is present in record and its an array by definition
                    try: 
                        _arr.append([MultiDataTypeHandling(x,i['fields_name']) for x in r[i['name']]])
                    except:
                        _arr.append(i['fallback'])
                elif r[i['name']]: #when data is present in record and its not an array by definition
                    try:
                        _arr.append(MultiDataTypeHandling(r[i['name']],i['fields_name'])) 
                    except:
                        _arr.append(i['fallback'])
                else:
                    _arr.append(i['fallback'])


            elif i['field_type'] == 'RECORD' and i['mode'] == 'REPEATED':  #when its a Kind and array
                try:
                    _arr.append([RecordToDict(x) for x in r[i['name']]])
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

    def cursor_query(_query, cursor = None, limit = 10):
        query_iter = _query.fetch(start_cursor=cursor, limit=limit)
        page = next(query_iter.pages)

        tasks = list(page)
        next_cursor = query_iter.next_page_token

        return tasks, next_cursor   

    #prep phase
    dataset_id = _dataset
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(_table)
    t_table_ref = dataset_ref.table('t_' + _table)
    table = client.get_table(table_ref)  # Make an API request.

    original_schema = table.schema
    
    #recreate temp table
    t_query = ( ## all users and their flows
            """create or replace table %s.t_%s as 
                select * FROM `%s.%s`
                where false""" 
            % (_dataset,_table,_dataset,_table) # time for conversation
        )
    
    print('Temp table creation query :', t_query) 
    try:
        t_query_job = client.query(
                t_query,
                # Location must match that of the dataset(s) referenced in the query.
                location="EU",
                #job_config=job_config,
            )

        t_query_job.result() 
    except Exception as e:
        logging.error("Error running query :" + t_query)
        logging.error(e)  
    
   
    #getting latest created
    query = ( ## all users and their flows
            """select max(%s) FROM `%s.%s`""" % (_created,_dataset,_table) # time for conversation
        )

    query_job = client.query(
            query,
            # Location must match that of the dataset(s) referenced in the query.
            location="EU",
            #job_config=job_config,
        )

    for row in query_job.result():  # Waits for the query to finish
        created = row[0]
        if not created:
            created = datetime.utcnow()
        logging.info('Last entry at ' + str(created))

    _format_instructions = formatInstructions(original_schema)
    #_tuple
    
    #Insertion phase
    now = datetime.utcnow()
    created = created - timedelta(minutes = _minutes) #adjusting for lookback 
    query = datastore_client.query(kind=_table)
    query.add_filter(_created, '>', created)
    query.add_filter(_created,'<=',now)
    cursor = None
    n = 0
    processed_output = { "total": 0, "error": 0 }
    
    while n == 0 or cursor:
        n+=1
        results, cursor = cursor_query(query,cursor)
        processed_output['total'] += len(results)
            
        if results:
                      
            processed_results = [processRecord(r,_format_instructions) for r in results]
            bq_output = client.insert_rows(client.get_table(t_table_ref), processed_results)
            
            if bq_output: #object will be a filled array if there is an error while loading.
                processed_output['error'] +=len(results)
                logging.warning(bq_output)
            
        if n%100==0:
            print("Loaded " + str(n*10) + " records to t_" + _table )
            
    
    #migration phase
    #moving data from temp table to table
    time.sleep(3) #delaying delete query in hope that temp table is properly filled first

    if partition_field: ##delete based on D-1 on partition field to reduce bytes processed
        d_query = ( """-- tag: incremental load - tag ends/
                    Delete from `%s.%s` 
                    where __key__.id in (select __key__.id from `%s.t_%s`) 
                           and date(%s) >= date_sub(current_date,interval 1 day) """ % (_dataset,_table,_dataset,_table,partition_field))        
    else:
        d_query = ( """-- tag: incremental load - tag ends/
                    Delete from `%s.%s` 
                    where __key__.id in (select __key__.id from `%s.t_%s`) """ % (_dataset,_table,_dataset,_table))
    print('Delete query :', d_query)
    try:
        d_query_job = client.query(
            d_query,
            # Location must match that of the dataset(s) referenced in the query.
            location="EU"
        )
        d_query_job.result() 
    except Exception as e:
        logging.error("Error running query :" + d_query)
        logging.error(e)       
    
    #moving data from temp table to table
    i_query = ( """-- tag: incremental load - tag ends/
                    Insert into `%s.%s` 
                    SELECT * except (pos) 
                    FROM ( SELECT *,
                            LEAD(__key__.id) OVER (PARTITION BY __key__.id ORDER BY %s) pos
                            FROM `%s.t_%s` ) a
                          WHERE pos IS NULL """ % (_dataset,_table,_created,_dataset,_table))
    print('Insert query :', i_query)
    try:
        i_query_job = client.query(
            i_query,
            # Location must match that of the dataset(s) referenced in the query.
            location="EU"
        )
        i_query_job.result() 
    except Exception as e:
        logging.error("Error running query :" + i_query)
        logging.error(e)

    
    #logging phase
    #keeping a copy of keys and metadata in a table
    i_query = ( """ -- tag: incremental load - tag ends/
                    Insert into `logging.incrementalLoad_log` 
                    SELECT __key__.id
                        , "%s" as table 
                        ,cast("%s" as timestamp) as load_time
                        , "%s" as filter_datefield
                        , cast("%s" as timestamp) as filter_timestamp
                        , count(*) as _count
                    FROM  `%s.t_%s` 
                    group by 1,2,3,4,5""" % (_table,str(now),_created,str(created),_dataset,_table))
    print('Logging query :', i_query)
    try:
        i_query_job = client.query(
            i_query,
            # Location must match that of the dataset(s) referenced in the query.
            location="EU"
        )
        i_query_job.result() 
    except Exception as e:
        logging.error("Error running query :" + i_query)
        logging.error(e)
    
    #daily log phase
    #create daily log table
    
    i_query = ( """ -- tag: create snapshot table - tag ends/
                    CREATE TABLE IF NOT EXISTS 
                    `logging.%s_log_%s`
                    partition by date(load_time)
                    AS
                    SELECT *,cast("%s" as timestamp) as load_time 
                    FROM `datastore.t_%s`
                    WHERE 1 = 2
                     """ % (_table,now.strftime('%Y%m%d'),str(now),_table))
    print('Create daily log table :', i_query)

    i_query_job = client.query(
            i_query,
            # Location must match that of the dataset(s) referenced in the query.
            location="EU"
        )
    try:
        i_query_job = client.query(
            i_query,
            # Location must match that of the dataset(s) referenced in the query.
            location="EU"
        )
        i_query_job.result() 
    except Exception as e:
        logging.error("Error running query :" + i_query)
        logging.error(e)

    #insert data to daily log table
    i_query = ( """ -- tag: snapshot table - tag ends/
                    Insert into `logging.%s_log_%s` 
                    SELECT *,cast("%s" as timestamp) as load_time FROM
                    `datastore.t_%s` """ % (_table,now.strftime('%Y%m%d'),str(now),_table))
    print('Insert daily log :', i_query)

    try:
        i_query_job = client.query(
            i_query,
            # Location must match that of the dataset(s) referenced in the query.
            location="EU"
        )
        i_query_job.result() 
    except Exception as e:
        logging.error("Error running query :" + i_query)
        logging.error(e)
     
    return processed_output

