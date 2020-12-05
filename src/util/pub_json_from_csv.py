import csv
from time import sleep
from sys import argv
from google.cloud import pubsub, logging
import json
import sys

def get_callback(f, data, logger, fs, id):
    def callback(f):
        try:
            logger.log_text(
                f"Method: {f.result} has completed publication of RT ID {id}; functions dict len is {len(fs)}", severity="DEBUG")
            fs.pop(id)
        except:
            logger.log_text("Handle exception {} for record {}.".format(
                f.exception(), data), severity="ERROR")

    return callback

if(len(argv) == 4):

    #Initialize clients
    log_client = logging.Client()
    log_client.get_default_handler()
    log_client.setup_logging()
    logger = log_client.logger(__name__+"_logger")

    publisher = pubsub.PublisherClient()
    
    #Assign args to vars
    pathToCsv = argv[1]
    print(argv[0])
    topicName = argv[2]
    sleepTime = int(argv[3])
    topic_path = f"projects/{log_client.project}/topics/{topicName}"
    futures={}

    with open(pathToCsv, 'r') as file:
        msg_reader = csv.reader(
            file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        #Strip leading chars
        msgs = [str(msg)[2:-2:] for msg in msg_reader][1::]
        for msg in msgs:
            json_rep = json.loads(msg)
            mdTradeEntryId = (json_rep.get("payload")[0]).get("tradeSummary").get("mdTradeEntryId")
            logger.log_text(
                f"Local test function name {sys._getframe(  ).f_code.co_name} found new record to publish: {json_rep}", severity="DEBUG")
            future = publisher.publish(topic_path, msg.encode("utf-8"))
            futures[mdTradeEntryId] = future
            future.add_done_callback(get_callback(future, json_rep, logger, futures, mdTradeEntryId))
            sleep(sleepTime)

else:
    print('Pass in pathToCsv, topicName, sleepTime (seconds) as arg')
