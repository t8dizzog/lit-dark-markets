# Purpose: Read Comma Separated CSV File to create GCP Pub/Sub subscriptions to CME Smart Stream topics
# Author: Matt Tait
# ------------------------------------------
INPUT=script_input_data_4.csv
OLDIFS=$IFS
IFS=','
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
while read Symbol Trade_Topic TOB_Topic L2_Topic Trade_Prefix TOB_Prefix L2_Prefix
do
	trd_sub="marketdata-${Trade_Prefix}-${Symbol,,}-json-sub"
	tob_sub="marketdata-${TOB_Prefix}-${Symbol,,}-json-sub"
  l2_sub="marketdata-l2-${Symbol,,}-json-sub"
  gcloud dataflow jobs run ${Symbol,,}-trd-ps-to-bq --gcs-location gs://dataflow-templates-us-central1/latest/PubSub_Subscription_to_BigQuery --region us-central1 --max-workers 3 --num-workers 1 --staging-location gs://lit-dark-markets/tmp --parameters inputSubscription=projects/lit-dark-markets/subscriptions/${trd_sub},outputTableSpec=lit-dark-markets:canon.${Symbol,,}_trd
  gcloud dataflow jobs run ${Symbol,,}-tob-ps-to-bq --gcs-location gs://dataflow-templates-us-central1/latest/PubSub_Subscription_to_BigQuery --region us-central1 --max-workers 3 --num-workers 1 --staging-location gs://lit-dark-markets/tmp --parameters inputSubscription=projects/lit-dark-markets/subscriptions/${tob_sub},outputTableSpec=lit-dark-markets:canon.${Symbol,,}_tob
  gcloud dataflow jobs run ${Symbol,,}-l2-ps-to-bq --gcs-location gs://dataflow-templates-us-central1/latest/PubSub_Subscription_to_BigQuery --region us-central1 --max-workers 3 --num-workers 1 --staging-location gs://lit-dark-markets/tmp --parameters inputSubscription=projects/lit-dark-markets/subscriptions/${l2_sub},outputTableSpec=lit-dark-markets:canon.${Symbol,,}_l2
done < $INPUT
IFS=$OLDIFS
