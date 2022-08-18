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