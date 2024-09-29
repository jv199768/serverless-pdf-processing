import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

textract = boto3.client('textract')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # The SNS message with job information
            sns_message = json.loads(record['Sns']['Message'])
                                                                                                                            

            # Accessing the keys for getting Textract results
            job_id = sns_message['JobId']
            status = sns_message['Status']

            # Accessing the keys for destination
            bucket = sns_message['DocumentLocation']['S3Bucket']
            s3_object_key = sns_message['DocumentLocation']['S3ObjectName']
            file_name = s3_object_key.split('/')[1].split('.')[0]

            if status == 'SUCCEEDED':
                # Proceed to get the document text detection results
                response = textract.get_document_text_detection(JobId=job_id)

                # Collect extracted text
                detected_text = []
                for item in response.get('Blocks', []):
                    if item['BlockType'] == 'LINE':
                        detected_text.append(item['Text'])

                # Save collected text to S3
                output_key = f"processed/{file_name}.txt"
                s3.put_object(
                    Bucket=bucket,
                    Key=output_key,
                    Body="\n".join(detected_text)
                )
                logger.warning(f"Detected text is written to S3/{output_key}")

            elif status == 'FAILED':
                logger.info(f"Job {job_id} failed.")

        except KeyError as e:
            logger.error(f"KeyError: Missing expected key {str(e)} in the message: {sns_message}")
        except Exception as e:
            logger.error(f"Error processing job {job_id}: {str(e)}")

    return {
        'statusCode': 200,
        'body': 'Notification processed successfully!'
    }
