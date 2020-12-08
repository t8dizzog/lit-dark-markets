from apache_beam import *
from apache_beam.options.pipeline_options import *
#import apache_beam.options as PipelineOptions
import google.auth
from google.cloud import storage
import json
import google.auth

def run(argv=None):
    #credentials, project_id = google.auth.default()

    po = NormalizeForexOptions(flags=argv)
    po.view_as(StandardOptions).streaming = True
    p = Pipeline(options=po)

    #TODO: Confirm that the _all_options bit is necessary. I shouldn't have to access
    # a private dict
    project= p.options._all_options.get("project")

    gcs_client = storage.Client("lit-dark-markets")
    bucket = gcs_client.get_bucket(p.options.config_bucket)
    json_object = bucket.blob(p.options.config_json)
    table_path = p.options.output_table
    config_dict = json.loads(json_object.download_as_text())
    
    #TODO: Validate that all expected fields exist
    subs = config_dict.get('futures_subscriptions')
    subs_dict = {}
    for sub in subs:
        key = str.split(sub,"-")[2]
        pcoll = p | f"read trades froms subs {sub}" >> io.ReadFromPubSub(subscription=f"projects/{project}/subscriptions/{sub}")
        subs_dict[key] = pcoll

    #Merge all PCollections
    merged_pcolls = subs_dict.values() | "merge input subscriptions" >> Flatten()

    #Write messages to BigQuery
    merged_pcolls | "write trades to BQ" >> io.WriteToBigQuery(
                                                # Write the messages
                                                table_path,
                                                write_disposition=io.BigQueryDisposition.WRITE_APPEND)

    result = p.run()
    result.wait_until_finish()

#Accept GCS URL of config JSON as a param 
class NormalizeForexOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--config_bucket',
        help='Name of bucket containing config resources',
        default='lit-dark-markets')
    parser.add_argument(
        '--config_json',
        help='JSON object that specifies: otc_subscriptions (list); futures_subscriptions (list); output_bq_table (k/v); output_pubsub_topic (k/v)',
        default='normalize_forex_config.json'),
    parser.add_argument(
        '--output_table',
        help='Path to BQ table that stores trade messages',
        default='some_nonsense.json')

run()