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
        gcloud pubsub subscriptions create $trd_sub --topic=$Trade_Topic --topic-project=cmegroup-marketdata-js --expiration-period=31d
        gcloud pubsub subscriptions create $tob_sub --topic=$TOB_Topic --topic-project=cmegroup-marketdata-js --expiration-period=31d
        gcloud pubsub subscriptions create $l2_sub --topic=$L2_Topic --topic-project=cmegroup-marketdata-js --expiration-period=31d
done < $INPUT
IFS=$OLDIFS
