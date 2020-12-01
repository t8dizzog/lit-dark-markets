import base64
import json
import feedparser
import sys
from google.cloud import storage
from google.cloud import pubsub
from google.cloud import logging

last_dissem_id = 1


def parse_forex(event, context):
    
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    payload = json.loads(pubsub_message)
    print(payload)

    client = logging.Client()
    client.get_default_handler()
    client.setup_logging()
    logger = client.logger(__name__+"_logger")

    logger.log_text(f"Function name {sys._getframe(  ).f_code.co_name} triggered by message {context.event_id} at time {context.timestamp} ", severity="INFO")
        
    #Initialize clients
    gcs_client = storage.Client("lit-dark-markets")
    client = logging.Client()
    client.get_default_handler()
    client.setup_logging()
    logger = client.logger(__name__+"_logger")


    #Retrieve RT msg key names from GCS
    bucket = gcs_client.get_bucket(payload["bucket"])
    object = bucket.blob(payload["object"])
    contents = object.download_as_string()
    contents = contents.decode("utf-8")
    keys = contents.split(",")

    #Retrieve RT msg values from GCS
    feed = feedparser.parse(payload["feed_url"])          
    entries = feed.entries

    
    #For each record in feed, bind keys to vals. Publish to PubSub only if new message
    new_record_found=False
    for e in entries:
        global last_dissem_id
        values = e.get("summary").split(",")
        values = [v.replace("\"","") for v in values]
        record = dict(zip(keys,values))
        dissem_id = int(record.get("Original_Dissemination_ID"))
        if dissem_id>last_dissem_id:
            new_record_found=True
            json_obj = json.dumps(record)
            logger.log_text(f"Function name {sys._getframe(  ).f_code.co_name} found new record to publish: {json_obj}", severity="INFO")
            last_dissem_id=dissem_id
    if not new_record_found:
        logger.log_text(f"Function name {sys._getframe(  ).f_code.co_name} found no new dissem IDs and will not publish ", severity="INFO")

