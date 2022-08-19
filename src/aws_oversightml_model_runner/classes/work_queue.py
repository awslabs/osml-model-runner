import json
import logging

import boto3
import botocore

from aws_oversightml_model_runner.utils.constants import BOTO_CONFIG


class WorkQueue:
    def __init__(
        self,
        queue_url: str,
        wait_seconds: int = 20,
        visible_seconds: int = 20 * 60,
        num_messages: int = 1,
    ):
        self.sqs_client = boto3.client("sqs", config=BOTO_CONFIG)
        self.queue_url = queue_url
        self.wait_seconds = wait_seconds
        self.visible_seconds = visible_seconds
        self.num_messages = num_messages
        pass

    def __iter__(self):
        while True:

            try:
                queue_response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    AttributeNames=["All"],
                    MessageAttributeNames=["All"],
                    MaxNumberOfMessages=self.num_messages,
                    VisibilityTimeout=self.visible_seconds,
                    WaitTimeSeconds=self.wait_seconds,
                )

                logging.debug("Dequeued processing request {}".format(str(queue_response)))

                if "Messages" in queue_response:
                    for message in queue_response["Messages"]:
                        message_body = message["Body"]
                        logging.debug("Message Body {}".format(message_body))

                        try:
                            region_request = json.loads(message_body)

                            yield message["ReceiptHandle"], region_request

                        except json.JSONDecodeError:
                            logging.warning(
                                "Skipping message that is not valid JSON: {}".format(message_body)
                            )
                            yield None, None
                else:
                    yield None, None

            except botocore.exceptions.ClientError as error:
                logging.error("Unable to retrieve messaage from queue: {}".format(error))
                yield None, None

    def finish_request(self, receipt_handle):

        try:
            # Remove the message from the queue since it has been successfully processed
            self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except botocore.exceptions.ClientError as error:
            logging.error("Unable to remove messaage from queue: {}".format(error))

    def reset_request(self, receipt_handle, visibility=0):

        try:
            self.sqs_client.change_message_visibility(
                QueueUrl=self.queue_url, ReceiptHandle=receipt_handle, VisibilityTimeout=visibility
            )
        except botocore.exceptions.ClientError as error:
            logging.error("Unable to reset messaage visibility: {}".format(error))

    def send_request(self, request):
        try:
            self.sqs_client.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(request))
        except botocore.exceptions.ClientError as error:
            logging.error("Unable to send message visibility: {}".format(error))
