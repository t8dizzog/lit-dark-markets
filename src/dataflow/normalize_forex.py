import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

from google.cloud import storage

class EnrichCmeTrades(beam.DoFn):

    def process(self, element, static_data):
        import logging
        logging.info(f"static_data:\n {static_data}")
        curr_pair = static_data.get("product_code")
        logging.info(f"curr_pair:\n {curr_pair}")
        curr_pair_l = curr_pair.split("_")
        element["currency_pair"]=curr_pair
        element["currency_1"]=curr_pair_l[0]
        element["currency_2"]=curr_pair_l[1]
        logging.info(f"TLDR-NEW-DOFN: Enriched dict:\n {element}")
        return element

class ConvertToStandardFormat(beam.DoFn):
    
    def process(self, element):
        import apache_beam as beam
        import json
        import logging
        from datetime import datetime

        #Get payload and its entries from PubSub message's data element
        payload =json.loads(element.data.decode("utf-8")).get("payload")
        trade_summary = payload[0].get("tradeSummary")
        instrument = payload[0].get("instrument")
        trade_timestamp = payload[0].get("lastUpdateTime")
        
        #Get payload attribute entries
        ts = float(element.attributes.get("SendingTime"))/1000.0
        timestamp = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S.%f000')
        
        #Get payload trade summary entries
        trade_id = trade_summary.get("mdTradeEntryId")
        aggressor = trade_summary.get("aggressorSide")
        size = trade_summary.get("tradeQty")
        price = trade_summary.get("tradePrice")
        
        #Get payload instrument entries
        venue = instrument.get("exchangeMic")
        symbol = instrument.get("symbol")
        product_type = instrument.get("productType")
        product_code = instrument.get("productCode")
        
        output_msg_dict = dict( zip(
            ("trade_id","timestamp","venue","symbol","aggressor","seller","buyer",\
            "trade_timestamp","size","price","product_type","product_code"),
            (trade_id,timestamp,venue,symbol,aggressor,None,None,\
            trade_timestamp,size,price,product_type,product_code)))
        
        logging.info(f"TLDR1: Output msg dict:\n {output_msg_dict}")
        return output_msg_dict
        
class WriteBatchesToGCS(beam.DoFn):
    # TODO: Make into my own
    def __init__(self, gcs_output_path, filename_prefix=""):
        self.gcs_output_path = gcs_output_path
        self.filename_prefix = filename_prefix

    def process(self, batch, window=beam.DoFn.WindowParam):
        """Write one batch per file to a Google Cloud Storage bucket. """
        import apache_beam as beam

        mdy_format = "%Y/%b/%d"
        ts_format = "%H%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        mdy = window.start.to_utc_datetime().strftime(mdy_format)
        # TODO: Make filename prefix a top-level folder, as well as a filename prefix
        filename = "/".join([self.gcs_output_path, mdy, f"{self.filename_prefix}{window_start}_{window_end}.txt"])

        with beam.io.gcp.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for element in batch:
                f.write("{}\n".format(json.dumps(element)).encode("utf-8"))


class MakeBatchesByWindow(beam.PTransform):
    # TODO: Make into my own
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """
    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its
            # publish timestamp.
            | "Window into Fixed Intervals"
            >> beam.WindowInto(window.FixedWindows(self.window_size))
            | "Add timestamps to messages" >> beam.ParDo(AddTimestamps())
            # Use a dummy key to group the elements in the same window.
            # Note that all the elements in one window must fit into memory
            # for this. If the windowed elements do not fit into memory,
            # please consider using `beam.util.BatchElements`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements
            | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
            | "Groupby" >> beam.GroupByKey()
            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )


class AddTimestamps(beam.DoFn):
    #TODO: Make into my own
    import apache_beam as beam
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server. It
        is bound to each element by Beam at runtime.
        """
        import datetime
        yield {
            "message_body": (element.data).decode("utf-8"),
                # element['data'].decode('utf-8'),
            "publish_time": datetime.datetime.utcfromtimestamp(
                float(publish_time)
            ).strftime("%Y-%m-%d %H:%M:%S.%f"),
        }

def run(config_bucket, input_subscription_json, otc_subscription, bq_output_table,
        pubsub_output_topic, gcs_output_path, side_input_query, window_size=1.0, pipeline_args=None):
    # TODO Verify that save_main_session=True is required by DoFns which rely on modules imported into the global
    # namespace (but not the DF namespace)

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    # Get currency futures subscriptions
    #TODO: retrieve this using beam.io classes; not using gcs client classes
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(config_bucket)
    json_object = bucket.blob(input_subscription_json)
    config_dict = json.loads(json_object.download_as_text())
    subs = config_dict.get("futures_subscriptions")
    subs_dict = {}


    #side_p = beam.Pipeline(options=pipeline_options)
    p = beam.Pipeline(options=pipeline_options)

    #Get static product data as a side input
    mapping_p_coll = p | "Read product static data" >> beam.io.gcp.bigquery.ReadFromBigQuery(query=side_input_query, use_standard_sql=False)

    for sub in subs:
        key = sub.rsplit("-", 3)[1]
        p_coll = p | f"Read trades: {key} sub" >> beam.io.ReadFromPubSub(
            subscription=sub, with_attributes=True)
        subs_dict[key] = p_coll
        print(f"Just added key {key}\n")

    merged_pcolls = subs_dict.values() | "Merge FX futures trades subscriptions" >> beam.Flatten(
    )
    
    merged_pcolls | "Window into" >> MakeBatchesByWindow(window_size) | "Write to GCS" >> beam.ParDo(WriteBatchesToGCS(
        gcs_output_path,"trade_")) 
        
    flattend_cme_p_coll = merged_pcolls | "Convert to vendor-agnostic JSON trade format" >> beam.ParDo(
       ConvertToStandardFormat())
    
    flattend_cme_p_coll | "Enrich with static data" >> beam.ParDo(EnrichCmeTrades(), static_data=beam.pvalue.AsDict(mapping_p_coll))
        

    result = p.run()
    result.wait_until_finish()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--config_bucket",
    type=str,
    default="lit-dark-markets",
    help="Path to bucket of config files.\n"
)
parser.add_argument(
    "--input_subscription_json",
    type=str,
    default="normalize_forex_config.json",
    help="JSON list of Cloud Pub/Sub CME currency futures subscriptions for pipeline input.\n"
)
parser.add_argument(
    "--otc_subscription",
    type=str,
    default="projects/lit-dark-markets/subscriptions/marketdata-forex-rt-json-sub",
    help="The Cloud Pub/Sub GTR currency OTC product subscription for pipeline input.\n",
)
parser.add_argument(
    "--bq_output_table",
    type=str,
    default="test_trades",
    help="BQ table for merged set of standardized messages.\n",
)
parser.add_argument(
    "--pubsub_output_topic",
    type=str,
    default="projects/lit-dark-markets/topics/marketdata-normalized-fx-trades-json",
    help="Pubsub topic for merged set of standardized messages.\n",
)
parser.add_argument(
    "--gcs_output_path",
    type=str,
    default="gs://lit-dark-markets-audit/",
    help="Path for GCS audit output; include filename prefix (but not event type).\n",
)
parser.add_argument(
    "--side_input_query",
    type=str,
    default="select symbol,currency_pair,cme_contract_size from `lit-dark-markets.fx.product_symbol`",
    help="Query to get symbol-to-currency-pair mapping.\n",
)
parser.add_argument(
    "--window_size",
    type=float,
    default=1.0,
    help="Output file frequency in minutes.\n",
),

known_args, pipeline_args = parser.parse_known_args()

run(
    known_args.config_bucket,
    known_args.input_subscription_json,
    known_args.otc_subscription,
    known_args.bq_output_table,
    known_args.pubsub_output_topic,
    known_args.gcs_output_path,
    known_args.side_input_query,
    known_args.window_size,
    pipeline_args,
)
