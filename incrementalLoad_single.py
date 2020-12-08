import BigQuery_DatastoreIncrementalLoad_dev as bq

tables = [
           {'table':'CodeDiscountList','dataset':'datastore','created':'updated', 'hour' : 0, 'chunksize' : 1440 }
]

for t in tables:
    try:
        if 'byte_field' in t:
            bq.incrementalLoad(t['table'],t['dataset'],t['created'],t['hour'],t['chunksize'],t['byte_field'])
        else:
            bq.incrementalLoad(t['table'],t['dataset'],t['created'],t['hour'],t['chunksize'])
    except Exception as e:
        print('error on ',t['table'],':',e)