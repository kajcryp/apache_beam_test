# apache_beam_test

This repository is to show the code and the thought process to how I create a batch job in python for some sample cloud data. 

## Stage 1:
The first step was to try and retrieve the data from the uri path: gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv
To do this, I looked into the GCP Documentation and found these two pages:
  - https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-clustered
  - https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv

The first link is to find the schema for the path as this dataset wasn't easy to find. This with the help of the second link allowed me to write the python that would extract and load the data into BigQuery, where I could then do further data exploration.

This part of the code was seen in the following file: bigquery_unload.py

This was all run in the google cloud editor.
![image](https://user-images.githubusercontent.com/67463671/185476941-573e5d18-4859-48b8-b907-6c04839e8768.png)



## Stage 2:
Afterwards I underwent some data exploration in Big Query to find what the final data output should be after the apache beam batch job is created and processed. The results can be shown here:

![image](https://user-images.githubusercontent.com/67463671/185475704-51ee7672-e4d0-46b0-8d14-4cabc78ab66b.png)

## Stage 3:
Now was when I looked at creating the apache beam pipeline. I had issues witj 
