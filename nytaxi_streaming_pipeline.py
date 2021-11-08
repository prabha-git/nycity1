import argparse
import time
import logging
import json
import typing
from datetime import datetime
from pytz import timezone
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner

# ### functions and classes

class CommonLog(typing.NamedTuple):
    ride_id: str
    timestamp_utc: str
    event_type: str

beam.coders.registry.register_coder(CommonLog, beam.coders.RowCoder)

def parse_json(element):
    print(element)
    row = json.loads(element.decode('utf-8'))
    return CommonLog(**row)

def add_processing_timestamp(element):
    row = element._asdict()
    row['event_timestamp'] = row.pop('timestamp_utc')
    row['processing_timestamp'] = datetime.now(timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    return row

def is_pickup_dropoff(element):
    return element['ride_status'] == 'pickup' or element['ride_status'] == 'dropoff'

class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")
        output = {'page_views': element, 'timestamp': window_start}
        yield output

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_topic', required=True, help='Input Pub/Sub Topic')
    parser.add_argument('--agg_table_name', help='BigQuery table name for aggregate results')
    parser.add_argument('--raw_table_name', required=True, help='BigQuery table name for raw inputs')
    #parser.add_argument('--window_duration', required=True, help='Window duration')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('streaming-minute-traffic-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_topic = opts.input_topic
    raw_table_name = opts.raw_table_name
    #pickup_dropoff_table="nycity1:taxi.ride_pickup_dropoff"

    # Table schema for BigQuery
    ride_table_schema = {
        "fields": [
            {
                "name": "ride_id",
                "type": "STRING",
                "mode":"REQUIRED"
            },
            {
                "name": "event_timestamp",
                "type": "TIMESTAMP"
            },
            {
                "name": "processing_timestamp",
                "type": "TIMESTAMP"
            },
            {
                "name": "event_type",
                "type": "STRING"
            }
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)



    parsed_msgs = (p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic)
                     | 'ParseJson' >> beam.Map(parse_json).with_output_types(CommonLog)
                     | 'AddTimeStamp' >> beam.Map(add_processing_timestamp))
    
    #Streaming to ride table
    (parsed_msgs 
        | "WriteToRide" >> beam.io.WriteToBigQuery(raw_table_name,
                                                       schema=ride_table_schema,
                                                       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    
    # # Streaming data to pickup_dropoff table
    # (parsed_msgs 
    #     | "FilterPickupDropoff" >> beam.Filter(is_pickup_dropoff)
    #     | "WriteToPickupDropoff" >> beam.io.WriteToBigQuery(pickup_dropoff_table,
    #                                                    schema=ride_table_schema,
    #                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    # )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()