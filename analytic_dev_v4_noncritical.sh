#
#!/usr/bin/env bash
#!/bin/bash
BQDATASET='datastore'
PROJECT='jumperdevnew'
TIME=$(date +%H%M)

GCSPATH=gs://dataanalytic/datastore-noncritical-`date -u +"%Y-%m-%dT%H:%M:%SZ"`
gcloud beta datastore export --kinds='StripeUser','PaypalUser','Shopify','Woocommerce','Printful','Amazon','Square','Payu','Razorpay','LinePageMessenger','Twitter','PageImessage','PageWhatsapp','Easyparcel','Easyship','Simplypost','Bigcommerce','Ocbc','PayMayaUser','Pesopay','Wirecard','TwoCheckoutUser','PaystackUser','SquarePay','BillplzUser','Ipay88User','BangkokBank','Enets','MidtransUser','PaytmUser','Role' --project=$PROJECT $GCSPATH

echo $(date)

COLORS=(StripeUser PaypalUser Shopify Woocommerce Printful Amazon Square Payu Razorpay LinePageMessenger Twitter PageImessage PageWhatsapp Easyparcel Easyship Simplypost Bigcommerce Ocbc PayMayaUser Pesopay Wirecard	TwoCheckoutUser PaystackUser SquarePay BillplzUser Ipay88User BangkokBank Enets MidtransUser PaytmUser Role)
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
