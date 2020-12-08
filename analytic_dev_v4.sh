#
#!/usr/bin/env bash
#!/bin/bash
BQDATASET='datastore'
PROJECT='jumperdevnew'
TIME=$(date +%H%M)

GCSPATH=gs://dataanalytic/datastore-`date -u +"%Y-%m-%dT%H:%M:%SZ"`
gcloud beta datastore export --kinds='Action','Discount','Rule','Transaction','Product','Subscription','PageMessenger','LineMessenger','Youtube','Agency','Address','Message','Post','Zone','Zoneshipping','Template','Faqpresets','FaqDialogflowConnect','Agent_conversation','InactiveRelation','Coupon','Outlet'  --project=$PROJECT $GCSPATH

echo $(date)

COLORS=(Action Discount Rule Transaction Product Subscription PageMessenger LineMessenger Youtube Agency Address Message  Post  Zone Zoneshipping Template Faqpresets FaqDialogflowConnect Agent_conversation InactiveRelation Coupon Outlet)
for KIND in "${COLORS[@]}"
do
    bq load --source_format=DATASTORE_BACKUP --replace $PROJECT:$BQDATASET.${KIND} $GCSPATH/all_namespaces/kind_$KIND/all_namespaces_kind_$KIND.export_metadata
done

#keep a copy for 0th hour
case $TIME in
	(0000) echo BACKUP ;;
	(*) gsutil -m rm -r -f $GCSPATH;;
esac

echo $(date)
