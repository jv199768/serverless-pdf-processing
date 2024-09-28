import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    textract = boto3.client('textract')

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        try:
            # Call Textract
            response = textract.detect_document_text(
                Document={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': key
                    }
                }
            )

            # Extract detected text
            detected_text = []
            for item in response.get('Blocks', []):
                if item['BlockType'] == 'LINE':
                    detected_text.append(item['Text'])

            # Join detected text into a single string
            text_output = '\n'.join(detected_text)

            # Create a new key for the output text file
            file_name = key.split('/')[1].split('.')[0]
            output_key = f'processed/{file_name}.txt'

            # Write the detected text to the S3 bucket
            s3.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=text_output
            )

            logger.info(f'Detected Text is written to: {output_key}')

        except Exception as e:
            logger.error(f'Error processing file {key} from bucket {bucket}: {str(e)}')
            continue

    return {
        'statusCode': 200,
        'body': json.dumps('Textract processing complete!')
    }