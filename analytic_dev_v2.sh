#
#!/usr/bin/env bash
#!/bin/bash
BQDATASET='datastore'
PROJECT='jumperdevnew'
TIME=$(date +%H)

GCSPATH=gs://dataanalytic/datastore-`date -u +"%Y-%m-%dT%H:%M:%SZ"`
gcloud beta datastore export --kinds='Action','Discount','Rule','ChatInbox','User','Order','Conversation','Transaction','Product','Variation','ProxyUser','Subscription','PageMessenger','LineMessenger','Youtube','Agency','Address','Cart','Message','StripeUser','PaypalUser','Shopify','Woocommerce','Printful','Amazon','Square','Post','Authkey','Payu','Razorpay','Zone','Zoneshipping','LinePageMessenger','Twitter','PageImessage','PageWhatsapp','Easyparcel','Easyship','Simplypost','Bigcommerce','Ocbc','PayMayaUser','Pesopay','Wirecard','Template','Faqpresets','FaqDialogflowConnect','TouchPoint','Dashboard_conversation','Agent_conversation' --project=$PROJECT $GCSPATH

echo $(date)

COLORS=(Action Discount Rule ChatInbox User Order Conversation Transaction Product Variation ProxyUser Subscription PageMessenger LineMessenger Youtube Agency Address Cart Message StripeUser PaypalUser Shopify Woocommerce Printful Amazon Square Post Authkey Payu Razorpay Zone Zoneshipping LinePageMessenger Twitter PageImessage PageWhatsapp Easyparcel Easyship Simplypost Bigcommerce Ocbc PayMayaUser Pesopay Wirecard Template Faqpresets FaqDialogflowConnect TouchPoint Dashboard_conversation Agent_conversation)
for KIND in "${COLORS[@]}"
do
    bq load --source_format=DATASTORE_BACKUP --replace $PROJECT:$BQDATASET.${KIND} $GCSPATH/all_namespaces/kind_$KIND/all_namespaces_kind_$KIND.export_metadata
done

#keep a copy for 0th hour
case $TIME in
	(00) echo BACKUP ;;
	(*) gsutil -m rm -r -f $GCSPATH;;
esac

echo $(date)