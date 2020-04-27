"""
This dataflow job will insert data from a Pub Sub topic into a Bigquery table

Input:
- Pubsub subscription name
- Big query table name

Output:
- Bigquery dataset:tablename

"""

import argparse
import logging
import json
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import BigQueryDisposition

import pandas as pd
import re


# {"eventtime":"2020-04-26 00:00", "eventtext":"abc", "eventint":"123"}
class StreamExampleDoFn(beam.DoFn):
    def __init(self):
        beam.DoFn.__init__(self)

    def pubsubJsonTransform(self, messageContent):
        print(messageContent)
        # In Java there's an option for ignoring unknown values, but in Python not sure if there's a better way. In this example we will just pull the values we want to match the schema
        jsonData = json.loads(messageContent)
        jsonDest = {"eventtime":jsonData['eventtime'], "eventtext":jsonData['eventtext'], "eventint":jsonData['eventint']}
        return [jsonDest]

    def process(self, element):
        """ Utility function to do any additional processing before writing the message into big query
        """

        return self.pubsubJsonTransform(element)


class StreamDataOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--dataSubscription',
            # required=True,
            default='projects/dbk-login-log-anlysis-wug01-qa/subscriptions/streaming_example_sub',
            help='Pub subscription name')
        parser.add_argument(
            '--bigQueryTableRef',
            # required=True,
            default='dbk-login-log-anlysis-wug01-qa:streaming_dataflow_example.streaming_example',
            help='Target Big Query table name')


''' 
    Command line options sample values for:
        Running directly:
        '--runner=DirectRunner',
    
        Running in Google Dataflow:
        '--runner=DataflowRunner',    
        '--project=dbk-login-log-anlysis-wug01-qa',
        '--staging_location=gs://dbk-login-log-anlysis-dataflow-qa/staging',
        '--temp_location=gs://dbk-login-log-anlysis-dataflow-qa/temp',
        '--job_name=stream_pubsub_example',
        
        For generating template
        '--template_location=gs://dbk-login-log-anlysis-dataflow-qa/templates/stream_pubsub_example_template'
'''

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)
    user_options = pipeline_options.view_as(StreamDataOptions)
    print(user_options)
    lines = p | 'Read From Pub sub' >> beam.io.ReadFromPubSub(subscription=user_options.dataSubscription).with_output_types(bytes)
    decode = lines | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
    jsonObj = decode | 'Get JSON' >> (beam.ParDo(StreamExampleDoFn()))
    jsonObj | 'Write to Big Query' >> beam.io.WriteToBigQuery(user_options.bigQueryTableRef, write_disposition=BigQueryDisposition.WRITE_APPEND)
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()