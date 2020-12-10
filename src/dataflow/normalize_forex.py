import argparse
import json
import apache_beam as beam



from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

from google.cloud import storage


class WriteBatchesToGCS(beam.DoFn):
    # TODO: Attribution
    def __init__(self, gcs_output_path):
        self.gcs_output_path = gcs_output_path

    def process(self, batch, window=beam.DoFn.WindowParam):
        """Write one batch per file to a Google Cloud Storage bucket. """
        import apache_beam as beam

        mdy_format = "%Y/%b/%d"
        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        mdy = window.start.to_utc_datetime().strftime(mdy_format)
        filename = "/".join([self.gcs_output_path, mdy, window_start+"-"+window_end])

        with beam.io.gcp.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for element in batch:
                f.write("{}\n".format(json.dumps(element)).encode("utf-8"))


class MakeBatchesByWindow(beam.PTransform):
    # TODO: Attribution
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
    #TODO: Attribution
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
        pubsub_output_topic, gcs_output_path, window_size=1.0, pipeline_args=None):
    # TODO Verify that save_main_session=True is required by DoFns which rely on modules imported into the global
    # namespace (but not the DF namespace)

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    # Get currency futures subscriptions
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(config_bucket)
    json_object = bucket.blob(input_subscription_json)
    config_dict = json.loads(json_object.download_as_text())
    subs = config_dict.get("futures_subscriptions")
    subs_dict = {}

    p = beam.Pipeline(options=pipeline_options)

    for sub in subs:
        key = sub.rsplit("-", 3)[1]
        p_coll = p | f"read trades: {key} sub" >> beam.io.ReadFromPubSub(
            subscription=sub, with_attributes=True)
        subs_dict[key] = p_coll
        print(f"Just added key {key}\n")

    merged_pcolls = subs_dict.values() | "merge forex trades subscriptions" >> beam.Flatten(
    ) | "Window into" >> MakeBatchesByWindow(window_size) | "Write to GCS" >> beam.ParDo(WriteBatchesToGCS(gcs_output_path))

    result = p.run()
    result.wait_until_finish()

    # po = NormalizeForexOptions(flags=argv)
    # po.view_as(StandardOptions).streaming = True
    # p = Pipeline(options=po)

    # TODO: Confirm that the _all_options bit is necessary. I shouldn't have to access
    # a private dict
    # project= p.options._all_options.get("project")

    # table_path = p.options.output_table

    # TODO: Validate that all expected fields exist
    # Merge all PCollections
    # merged_pcolls = subs_dict.values() | "merge input subscriptions" >> Flatten()

    # Write messages to BigQuery
    # merged_pcolls | Map(lamda x: bytes.decode(x)) |"write trades to BQ" >> beam.WriteToBigQuery(
    #                                             # Write the messages
    #                                             table_path,
    #                                             write_disposition=beam.BigQueryDisposition.WRITE_APPEND)

    # result = p.run()
    # result.wait_until_finish()


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
    "--window_size",
    type=float,
    default=1.0,
    help="Output file frequency in minutes.\n",
)
known_args, pipeline_args = parser.parse_known_args()

run(
    known_args.config_bucket,
    known_args.input_subscription_json,
    known_args.otc_subscription,
    known_args.bq_output_table,
    known_args.pubsub_output_topic,
    known_args.gcs_output_path,
    known_args.window_size,
    pipeline_args,
)
