import boto3
import logging


logger = logging.getLogger()
logger.setLevel(logging.WARNING)

sns = boto3.client('sns')
textract = boto3.client('textract')

def lambda_handler(event, context):
    topic_arn = 'your-topic-arn'
    textract_role = 'your-textract-role'
    


    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        try:
            # Start Textract asynchronous processing, use env vars
            response = textract.start_document_text_detection(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': key
                    }
                },
                NotificationChannel={
                    'SNSTopicArn': topic_arn,
                    'RoleArn': textract_role
                }
            )

            logger.info(f"File {key} is sent to Textract.")

        except Exception as e:
            logger.error(f"Error processing file {key} from bucket {bucket}: {str(e)}")
            continue

    return {
        'statusCode': 200,
        'body': 'Textract processing initiation is complete!'
    }