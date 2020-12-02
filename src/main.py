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

    log_client = logging.Client()
    log_client.get_default_handler()
    log_client.setup_logging()
    logger = log_client.logger(__name__+"_logger")
    logger.log_text(f"Function name {sys._getframe(  ).f_code.co_name} triggered by message {context.event_id} at time {context.timestamp} ", severity="INFO")
        
    #Initialize clients
    gcs_client = storage.Client("lit-dark-markets")
    
    log_client = logging.Client()
    log_client.get_default_handler()
    log_client.setup_logging()
    logger = log_client.logger(__name__+"_logger")

    publisher = pubsub.PublisherClient()
    topic_path = f"projects/{log_client.project}/topics/{payload['out_topic']}"
 
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
            future = publisher.publish(topic_path, json_obj.encode("utf-8"))
            future.add_done_callback(get_callback(future, json_obj, logger))
    if not new_record_found:
        logger.log_text(f"Function name {sys._getframe(  ).f_code.co_name} found no new dissem IDs and will not publish ", severity="INFO")

def get_callback(f, data, logger):
    def callback(f):
        try:
            logger.log_text(f"Publish failure: {f.result} for RT record {data}", severity="INFO")
        except:  # noqa
            logger.log_text("Handle exception {} for record {}.".format(f.exception(), data), severity="ERROR")

    return callback

