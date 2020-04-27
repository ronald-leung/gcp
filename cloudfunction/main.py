from google.cloud import storage
from google.cloud import pubsub_v1

def gcs_data_to_pubsub_example(data, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.
    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(data['bucket']))
    print('File: {}'.format(data['name']))
    print('Metageneration: {}'.format(data['metageneration']))
    print('Created: {}'.format(data['timeCreated']))
    print('Updated: {}'.format(data['updated']))

    storage_client = storage.Client()

    bucket = storage_client.bucket(data['bucket'])
    blob = bucket.blob(data['name'])
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


    return "Done"