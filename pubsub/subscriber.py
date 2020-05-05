from google.cloud import pubsub_v1

project_id = "dbk-minerva-cug01-dev"
subscription_name = "test-ron-sub-json"
timeout = 20.0  # "How long the subscriber should listen for
# messages in seconds"

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_name}`
subscription_path = subscriber.subscription_path(
    project_id, subscription_name
)

def callback(message):
    print("Received message: {}".format(message))
    message.ack()

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback
)
print("Listening for messages on {}..\n".format(subscription_path))

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except:  # noqa
        streaming_pull_future.cancel()