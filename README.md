# Readme
This project provides handy examples for getting starting on any data related projects. 
The examples are meant to be simple, but also easily enough to reuse for highly scalable pipelines.
Here are the examples scenarios:

-  GCS -> Pubsub -> Dataflow -> Bigquery pipeline
   - This is a very common use cases. Some application has some data, that is generated periodically. 
   We want to visualize it, do machine learning, analysis, yadi yadi yadda. This pipeline allows you to
   get generic data to the right place. Once data is in Bigquery then we are free to use it however we like. 
   - Resources to setup before building this example:
      - Pub Sub Topic
      - Pub Sub Subscription
      - Google Cloud Storage Bucket
      - Big Query Table
         - To create the table, run 
            bq mk --table project_id:dataset.table schema.json
            
   - Components that we will be building:
      - App engine, to continuously generate test data, and inserting data into Google Cloud Storage
      - Google cloud function that gets triggered every time a new file gets inserted to Google Cloud Storage. 
      It'll read the file, and publish the data to a Pub Sub Topic
      - Google streaming dataflow job, that will read the data from a pub sub subscription, and stores the data into a BigQuery table.
        
   
- Cloud composer
   - QA automation tests
      - Create a dag, that runs a BigQuery SQL command. Report success / failure based on test results
       
       
- Pubsub
    - Simple python subscriber from Google's example. 
        Note: Run it in IntelliJ debugger, all that's needed is the GOOGLE_APPLICATION_CREDENTIALS env variable