import argparse
import logging
import re
import csv
import os
from datetime import datetime, timedelta
import datetime 
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions, GoogleCloudOptions, SetupOptions

def big_query_unload():

    from google.cloud import bigquery

    client = bigquery.Client()

    table_id = "ks-test-environment.sample_transactions_US.sample_transactions_US_table"
    print("Table_id variable is created")
    table_id_bq = "ks-test-environment:sample_transactions_US.sample_transactions_US_table" #created so that it can be used to read from big query in apache beam pipeline

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        schema=[
            bigquery.SchemaField("timestamp", bigquery.SqlTypetotal_transaction_amounts.TIMESTAMP),
            bigquery.SchemaField("origin", bigquery.SqlTypetotal_transaction_amounts.STRING),
            bigquery.SchemaField("destination", bigquery.SqlTypetotal_transaction_amounts.STRING),
            bigquery.SchemaField("amount", bigquery.SqlTypetotal_transaction_amounts.NUMERIC),
        ],
    )
    print("Job config with schema is created")

    uri = "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
    print("uri is created")

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config,
    )

    print("Job load from table from url is created")

    load_job.result() 
    print("load job is completed with results")

    destination_table = client.get_table(table_id)  # Make an API request.
    print("destination table is created")

    return(table_id_bq)
    #return("Loaded {} rows.".format(destination_table.num_rows))


class split_file(beam.DoFn):
    def parse_file(element):
        for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
            return line

#Ptransformation to filter data based on amount
class CalcAmount(beam.DoFn):
    def has_amount(row_element, amount):
        return row_element['amount'] > amount

#Ptransformation to filter data based on year   
class ExcludeYear(beam.DoFn):
    def has_date(row_element, date):
        return row_elemen['date'] > date 

class CompositeTransform(beam.PTransform):
  def expand(self, input_columns):
    a = (
        input_columns
            | 'split file' >> beam.Map(lambda x: parse_file(x))
            | 'Filter transaction amount' >> beam.Filter(has_amount, 20)
            | 'Filter Date' >> beam.Filter(has_date, '2009-12-31')
            | 'Sum' >> beam.CombinePerKey(sum)
     ) 
  
    # Transform logic goes here.
    return a

def run(argv=None, save_main_session=True):

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =  'service_account_dflow.json' #Get the service account details/key and save it to your cloud editor
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv) 

    
    #These are all the pipeline options you're combining together
    pipeline_options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',
        project='ks-test-environment',
        job_total_transaction_amount='transactionspipeline',    
        temp_location='gs://temp_storage_transactions/temp_storage', #This is a temporary location in GCP Clpoud storage that needs to be created before hand for the function to work
        region='us-central1'
        )
        

    bq_table_spec = big_query_unload()
    #using the above function to retrieve the table location for where the sample data sits in Big Query
    
    SQL_query = '''select timestamp as date, amount as transaction_amount 
               from `ks-test-environment.sample_transactions_US.sample_transactions_US_table`
               '''
    table_schema = ('date:TIMESTAMP, transaction_amount:NUMERIC')

    with beam.Pipeline(options=beam_options) as pipeline:
       lines= (
            pipeline
            | 'ReadTable' >> beam.io.ReadFromBigQuery(query = SQL_query, use_standard_sql=True)
            | 'Composite Transform' >> CompositeTransform() 
        )
        
        lines | beam.io.WriteToBigQuery(
                                    'ks-test-environment:sample_transactions_US.total_amount_transactions',  
                                    schema = table_schema,
                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                                 )
        
        lines.run().wait_until_finish()
        
if __total_transaction_amount__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()



