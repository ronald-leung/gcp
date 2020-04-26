# Readme
This project provides handy examples for getting starting on any data related projects. 
The examples are meant to be simple, but also easily enough to reuse for highly scalable pipelines.
Here are the examples scenarios:

-  GCS -> Pubsub -> Dataflow -> Bigquery pipeline
   - This is a very common use cases. Some application has some data, that is generated periodically. 
   We want to visualize it, do machine learning, analysis, yadi yadi yadda. This pipeline allows you to
   get generic data to the right place. Once data is in Bigquery then we are free to use it however we like. 
   
- Cloud composer
   - QA automation tests
      - Create a dag, that runs a BigQuery SQL command. Report success / failure based on test results
       
