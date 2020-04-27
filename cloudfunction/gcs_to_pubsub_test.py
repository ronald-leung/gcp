import logging
import os
from google.cloud import storage
from google.cloud import pubsub_v1

client = storage.Client()

def read_file(filename):
    print('Reading the full file contents:', filename)

    storage_client = storage.Client()

    bucket = storage_client.bucket("dbk-login-log-anlysis-stream-dataflow-example")
    blob = bucket.blob("testdata.txt")
    data = blob.download_as_string().decode("utf-8")
    eventList = data.split("\n")

    print("Downloaded data:\n")
    print(data)

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_name}`

    project_id = "dbk-login-log-anlysis-wug01-qa"
    topic_name = "stream-example"
    topic_path = publisher.topic_path(project_id, topic_name)

    for event in eventList:
        if len(event) <= 0:
            continue
        print("Publishing: ", event)
        data = event.encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(future.result())


read_file("gs://dbk-login-log-anlysis-stream-dataflow-example/testdata.txt")