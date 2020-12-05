# Purpose: Read Comma Separated CSV File to create GCP Pub/Sub subscriptions to CME Smart Stream topics
# Author: Matt Tait
# ------------------------------------------
INPUT=script_input_data_4.csv
OLDIFS=$IFS
IFS=','
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
while read Symbol Trade_Topic TOB_Topic L2_Topic Trade_Prefix TOB_Prefix L2_Prefix
do
	trd_table="lit-dark-markets:canon.${Symbol,,}_trd"
	tob_table="lit-dark-markets:canon.${Symbol,,}_tob"
        l2_table="lit-dark-markets:canon.${Symbol,,}_l2"
        bq mk --time_partitioning_type=DAY --table $trd_table /home/matait/scratch/trd.json
        bq mk --time_partitioning_type=DAY --table $tob_table /home/matait/scratch/tob.json
        bq mk --time_partitioning_type=DAY --table $l2_table /home/matait/scratch/l2.json
done < $INPUT
IFS=$OLDIFS
