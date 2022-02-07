
import boto3
from botocore.exceptions import ClientError

class SQSClient:
    """ An abstraction over the AWS SQS api, allowing for simple consumption of SQS """

    def __init__(self, session, region):

        # Initialise SQS client
        self.sqs = session.client(
            'sqs',
            region_name=region,
        )
        print("SQSClient Initialised")

    def get_queue(self, name):
        """ Get Queue in SQS, returning the QueueURL """
        response = self.sqs.get_queue_url(
            QueueName=name,
        )
        queue_url = response['QueueUrl']
        return queue_url

    def delete_queue(self, queue_url):
        """ Delete the queue, given the following URL """
        response = self.sqs.delete_queue(QueueUrl=queue_url)
        print("Deleted Queue -> %s" % queue_url)
        return ("Queue at URL {0} deleted").format(queue_url)

    def send_message(self, **kwargs):
        """ Send message to the specified SQS queue """
        response = self.sqs.send_message(**kwargs)
        msg_id = response['MessageId']
        print("Message ID -> %s" % msg_id)
        return msg_id

    def get_next_messages(self, queue_url, maxCount):
        """ Receive Messages from Queue """
        response = self.sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[""],
            MaxNumberOfMessages=maxCount,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30,
            WaitTimeSeconds=5,
        )
        if not 'Messages' in response:
            return []
        messages = response['Messages']
        return messages

    def delete_message(self, queue_url, receipt_handle):
        """ delete Message from Queue given receipt_handle """
        try:
            self.sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                print("Queue does not exist, nothing to do")
            else:
                print("Unexpected error: %s" % e)
                raise

        return "deleted :  {0}".format(receipt_handle)