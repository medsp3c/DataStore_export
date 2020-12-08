#
#!/usr/bin/env bash
#!/bin/bash
BQDATASET='datastore'
PROJECT='jumperdevnew'
TIME=$(date +%H%M)

GCSPATH=gs://dataanalytic/datastore-bigtables-`date -u +"%Y-%m-%dT%H:%M:%SZ"`
gcloud beta datastore export --kinds='Conversation','Variation','ProxyUser','Authkey','TouchPoint','User','Order','Dashboard_conversation','ConversationAgentlog','ConversationCustomField','Messenger','Cart','Historic_dashboard_conversation','ChatInbox','CodeDiscountList','Audittrails'  --project=$PROJECT $GCSPATH

echo $(date)
sleep 5m
	
COLORS=(Conversation Variation ProxyUser Authkey TouchPoint User Order Dashboard_conversation ConversationAgentlog ConversationCustomField Messenger Cart Historic_dashboard_conversation ChatInbox CodeDiscountList Audittrails) 
for KIND in "${COLORS[@]}"
do
    bq load --source_format=DATASTORE_BACKUP --replace $PROJECT:$BQDATASET.${KIND} $GCSPATH/all_namespaces/kind_$KIND/all_namespaces_kind_$KIND.export_metadata
done
