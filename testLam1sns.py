from __future__ import print_function

import json

print('Loading function')


def lambda_handler(event, context):
    srcBucket = "xml-files-em"
    trgBucket = "flatten-json-em"
    srcFile   = "RO_3612514224.xml"
    
    message = json.loads(event['Records'][0]['Sns']['Message'])
    try:
        srcBucket = message['Records'][0]['s3']['bucket']['name']
        srcFile = message['Records'][0]['s3']['object']['key']
    except:
        print("Will be using default values from Lambda")
        
    print("will be using fileName:     ", srcFile)    
    print("will be using sourceBucket: ", srcBucket)
    print("will be using targetBucket: ", trgBucket)

    return {
        'statusCode': 200,
        'body': json.dumps("Done")
    }