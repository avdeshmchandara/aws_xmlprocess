"""
This lambda function is used to send message to FIFO
for each pipe delim file created in S3 bucket
"""
import json
import boto3
import ast

sqr = boto3.resource('sqs')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    """
    Lambda start function, based on event trigger
    If event is empty or incorrect, will try to use fixed file
    The message will be sent to FIFO as soon as pipe delim file created with meta data info
    """
    srcBucket="pipedelimfiles-for-table"
    srcFile="repair_order/repair_order/RO_3612514224.csv"

    #print("event: ", event)
    try:
        srcBucket=event['Records'][0]['s3']['bucket']['name']
        srcFile=event['Records'][0]['s3']['object']['key']
    except:
        print("Will be using default values from Lambda")
    print("fileName:     ", srcFile)
    print("sourceBucket: ", srcBucket)
    
    if "log_table" in srcFile:
        return {
            'statusCode': 200,
            'body': json.dumps('Log Tables will not be uploaded to Redshift')
        }        
    
    queue = sqr.get_queue_by_name(QueueName='sendToRedShiftUpload.fifo')
    response = queue.send_message(
        MessageBody= (srcBucket+":"+srcFile).strip(),
        MessageGroupId='testGrp1'
    )

    # The response is NOT a resource, but gives you a message ID and MD5
    print(response.get('MessageId'))
    print(response.get('MD5OfMessageBody'))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }