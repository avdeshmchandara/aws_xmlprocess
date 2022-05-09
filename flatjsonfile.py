####################################
#Receives messages from da-request queue and send to da reports queue 
import json
import boto3
import sys
import logging
import pymysql
import os
import time
import datetime
import random
import string
from datetime import datetime
from datetime import date
import uuid
from urllib.parse import unquote_plus
from itertools import chain, starmap

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'
strenv = str(os.environ.get('env'))
strvpc = str(os.environ.get('vpc'))
strsubnet = str(os.environ.get('subnet'))
totalenicnt = os.environ.get('totalenis')
rds_client = boto3.client('rds')
s3_resource = boto3.resource('s3')
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')
rds_client = boto3.client('rds')
sqs = boto3.resource('sqs')
sqsclient = boto3.client('sqs')
ec2_resource = boto3.resource('ec2')
today = str(date.today())
# Get the queue
requestsqueue = sqs.get_queue_by_name(QueueName='nssp-'+strenv+'-get-da-request-file-queue')
reportdetailsqueue = sqs.get_queue_by_name(QueueName='nssp-'+strenv+'-get-da-report-details-queue')
reportdetailsjumboqueue = sqs.get_queue_by_name(QueueName='nssp-'+strenv+'-get-da-report-details-jumbo-queue')

def flatten_json_iterative_solution(dictionary):
    #Flatten a nested json file
    def unpack(parent_key, parent_value):
        #Unpack one level of nesting in json file
        # Unpack one level only!!!
        if isinstance(parent_value, dict):
            for key, value in parent_value.items():
                temp1 = parent_key + '_' + key
                yield temp1, value
        elif isinstance(parent_value, list):
            i = 0 
            for value in parent_value:
                temp2 = parent_key + '_'+str(i) 
                i += 1
                yield temp2, value
        else:
            yield parent_key, parent_value    
    # Keep iterating until the termination condition is satisfied
    while True:
        # Keep unpacking the json file until all values are atomic elements (not dictionary or list)
        dictionary = dict(chain.from_iterable(starmap(unpack, dictionary.items())))
        # Terminate condition: not any value in the json file is dictionary or list
        if not any(isinstance(value, dict) for value in dictionary.values()) and \
          not any(isinstance(value, list) for value in dictionary.values()):
            break
    return dictionary

def getRDSConnection():
    dbtoken = rds_client.generate_db_auth_token(DBHostname = str(os.environ.get('dbHost')), Port = str(os.environ.get('dbPort')), DBUsername = str(os.environ.get('dbUser')))
    dbConnection = pymysql.connect(auth_plugin_map = {'mysql_cleartext_password':None},
                            user = str(os.environ.get('dbUser')),
                            host = str(os.environ.get('dbHost')),
                            password = dbtoken,
                            cursorclass=pymysql.cursors.DictCursor,
                            ssl={'ca': 'rds-combined-ca-bundle.pem'},
                            database =  str(os.environ.get('dbName')))    
    return dbConnection
try:
    dbconn = getRDSConnection()
except Exception as e:
    print("An exception occured while connecting to database. {}".format(e))
    raise
def lambda_handler(event, context):
    for record in event['Records']:
        jsondata = json.loads(event['Records'][0]['body'])
        print('printing jsondata')
        print(jsondata)
        flat = flatten_json_iterative_solution(jsondata)
        s3bucketname = str(flat['Records_0_s3_bucket_name'])
        filenameext = str(flat['Records_0_s3_object_key'])
        planfilepath = 's3://'+s3bucketname+'/'+filenameext
        rhandle = record['receiptHandle']
    print("Printing request report file path " +planfilepath)
    print('Printing message receipt handle '+rhandle)
    filename = filenameext.lower()[:-4]
    rptid = filename.split('_')[-2]
    requestid = filename.split('_')[-1]
    if (filenameext.__contains__("NoSheet")):
        sheetname = "NoSheet"
        sheetnum = "0"
    else:
        sheetname = filename.split('_')[-3]
        sheetnum = sheetname[3:]
    time.sleep(1)
    if (filenameext.__contains__("NoSheet")):
        dupsql = " SELECT REQUEST_ID FROM NSSP.REQUEST_MSG_LOG WHERE REQUEST_ID ="+requestid+"  AND REQUEST_REPORT_ID = "+rptid+" AND REQUEST_QUEUE ='Y' "
    else:
        dupsql = " SELECT REQUEST_ID FROM NSSP.REQUEST_MSG_LOG WHERE REQUEST_ID ="+requestid+"  AND REQUEST_REPORT_ID = "+rptid+" AND SHEET_NUM ="+sheetnum+" AND REQUEST_QUEUE ='Y' "
    print(dupsql)
    if not dbconn.open:
        dbconn.ping(reconnect=True)
    else:
        print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(requestid)+" REPORT_ID:"+str(rptid)+" SHEET_NUM:"+str(sheetnum)+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:DB connection is already open")
    cursor1 = dbconn.cursor()
    reccnt = cursor1.execute(dupsql)
    print("Printing row count: "+str(reccnt))
    cursor1.close()
    if ( int(reccnt) > 0 ):
        print("Message is duplicate for Request ID: "+str(requestid)+" and Report ID: "+str(rptid)+" and Sheet Number: "+sheetnum)
        print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(requestid)+" REPORT_ID:"+str(rptid)+" SHEET_NUM:"+str(sheetnum)+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:Message is duplicate")
    else:
        try:
            time.sleep(0.1)
            if (filenameext.__contains__("NoSheet")):
                selsql = " SELECT CAST(REQUEST_ID AS CHAR) AS REQID,CAST(REPORT_NUMBER AS CHAR) AS RPTID,CAST(SHEET_NUM AS CHAR) AS SHEET_NUM,REQUEST_PLAN_LCNSEE,REPORT_NAME,CAST(PRODUCT_GRP_ID AS CHAR) AS PRODUCT_GRP_ID,\
                REQUEST_FILE_S3_PATH AS PLAN_FILE_PATH,REPORT_LOCATION_IN_S3 AS REQUEST_S3_PATH,SHEET_NAME,PRODUCTS,CAST(WORKFLOW_ID AS CHAR) AS WORKFLOW_ID,CAST(PLAN_REQUEST_ETL_ID AS CHAR) AS PLAN_REQUEST_ETL_ID,COUNTY,ZIPCODE,EXCLPROVIDENFIED,\
                IGNSINGLEELEMMATCH,USESECNAME,INCLCOMPNAME,INCLBDC,INCLTC,REPHOSPBASEPROV,SUPRSANCPROV,HPN_INDICATOR AS HPN_IND,HPN_TIER_INFO,HPN_PRODUCTS,HPN_MARKET_COVERAGE_ID,FILE_DELIMITER,STATE_CODE,\
                TIER_INFO,CUSTOM_FLAG,CUSTOM_PRODUCTS,CUSTOM_STATES,CUSTOM_ZIPCODES,CUSTOM_SWAP,PRODUCT_STATE_FLAG FROM V_REQUEST_REPORT_QUEUE WHERE REQUEST_ID = "+str(requestid)+" AND REPORT_NUMBER= "+str(rptid)
            else:
                selsql = " SELECT CAST(REQUEST_ID AS CHAR) AS REQID,CAST(REPORT_NUMBER AS CHAR) AS RPTID,CAST(SHEET_NUM AS CHAR) AS SHEET_NUM,REQUEST_PLAN_LCNSEE,REPORT_NAME,CAST(PRODUCT_GRP_ID AS CHAR) AS PRODUCT_GRP_ID,\
                REQUEST_FILE_S3_PATH AS PLAN_FILE_PATH,REPORT_LOCATION_IN_S3 AS REQUEST_S3_PATH,SHEET_NAME,PRODUCTS,CAST(WORKFLOW_ID AS CHAR) AS WORKFLOW_ID,CAST(PLAN_REQUEST_ETL_ID AS CHAR) AS PLAN_REQUEST_ETL_ID,COUNTY,ZIPCODE,EXCLPROVIDENFIED,\
                IGNSINGLEELEMMATCH,USESECNAME,INCLCOMPNAME,INCLBDC,INCLTC,REPHOSPBASEPROV,SUPRSANCPROV,HPN_INDICATOR AS HPN_IND,HPN_TIER_INFO,HPN_PRODUCTS,HPN_MARKET_COVERAGE_ID,FILE_DELIMITER,STATE_CODE,\
                TIER_INFO,CUSTOM_FLAG,CUSTOM_PRODUCTS,CUSTOM_STATES,CUSTOM_ZIPCODES,CUSTOM_SWAP,PRODUCT_STATE_FLAG FROM V_REQUEST_REPORT_QUEUE WHERE REQUEST_ID = "+str(requestid)+" AND REPORT_NUMBER= "+str(rptid)+ " AND SHEET_NUM = "+sheetnum
            print(selsql)
            cur = dbconn.cursor()
            row_count = cur.execute(selsql)
            if (row_count > 0 ):
                print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(requestid)+" REPORT_ID:"+str(rptid)+" SHEET_NUM:"+str(sheetnum)+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:Got parameters from DB")
                try:
                    rows = cur.fetchone()
                    insertsql= " INSERT INTO NSSP.REQUEST_MSG_LOG ( REQUEST_ID, REQUEST_REPORT_ID, SHEET_NUM,REQUEST_QUEUE,REQUEST_QUEUE_TS) VALUES (%s, %s, %s, %s, %s) "
                    cur1 = dbconn.cursor()
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cur1.execute(insertsql, (rows['REQID'],rows['RPTID'],rows['SHEET_NUM'],'Y',timestamp))
                    cur1.close()
                    print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(rows['REQID'])+" REPORT_ID:"+str(rows['RPTID'])+" SHEET_NUM:"+str(rows['SHEET_NUM'])+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:Inserted into REQUEST_MSG_LOG table")
                    msg_b = json.dumps(rows, indent = 4)
                    print(msg_b)
                    #Send request details to report details queue
                    resp = sqsclient.send_message(QueueUrl=reportdetailsqueue.url, DelaySeconds=0, MessageBody=msg_b)
                    if ( len(resp['MessageId']) > 0 ):
                        print('Successfully sent message to nssp-'+strenv+'-get-da-report-details-queue for request id '+str(rows['REQID'])+' and report id '+str(rows['RPTID'])+' and sheet name '+str(rows['SHEET_NAME'])+' and sheet number '+str(rows['SHEET_NUM']))
                        print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(rows['REQID'])+" REPORT_ID:"+str(rows['RPTID'])+" SHEET_NUM:"+str(rows['SHEET_NUM'])+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:Sent message to reports queue")
                    dbconn.commit()
                    print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(rows['REQID'])+" REPORT_ID:"+str(rows['RPTID'])+" SHEET_NUM:"+str(rows['SHEET_NUM'])+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:Comitted into REQUEST_MSG_LOG table")
                except Exception as e:
                    print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(rows['REQID'])+" REPORT_ID:"+str(rows['RPTID'])+" SHEET_NUM:"+str(rows['SHEET_NUM'])+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:{}".format(e))
                    print("An exception occured. {}".format(e))
                    raise
            else:
                cur.close()
                print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(requestid)+" REPORT_ID:"+str(rptid)+" SHEET_NUM:"+str(sheetnum)+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:No data was received from Database")
                if (filenameext.__contains__("NoSheet")):
                    raise Exception ("No data was received from Database for request id "+str(requestid)+" and report id "+str(rptid))
                else:
                    raise Exception ("No data was received from Database for request id "+str(requestid)+" and report id "+str(rptid)+" and sheet number "+str(sheetnum))
        except Exception as e:
            print("[NSSP_TRACE] COMPONENT:nssp-"+strenv+"-get-da-request-details-lambda REQUEST_ID:"+str(requestid)+" REPORT_ID:"+str(rptid)+" SHEET_NUM:"+str(sheetnum)+" TIMESTAMP:"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+" WF_RUN_ID:"+" JOB_RUN_ID:"+" EVENT:{}".format(e))
            print("An exception occured. {}".format(e))
            raise
            
            
            
            
          #################################################
          ################                testAvdesh.py
          #################################################
          
          import json
import boto3
import sys
import logging
import os
import time
import datetime
import random
import string
from datetime import datetime
from datetime import date
import uuid
from urllib.parse import unquote_plus
from itertools import chain, starmap
####################################
strenv = str(os.environ.get('env'))
glueclient = boto3.client("glue")
sqsresource = boto3.resource('sqs', region_name = 'us-east-1')
sqsclient = boto3.client('sqs', region_name = 'us-east-1')
myglue = boto3.client('glue', region_name="us-east-1") 


sqs_poll_size = os.environ["SQS_POLL_SIZE"]
raw_queue_url = os.environ["RAW_QUEUE_URL"]
#raw_queue_url = "https://sqs.us-east-1.amazonaws.com/323183112872/nssp-dev-get-da-request-file-queue"
raw_queue_url = "https://sqs.us-east-1.amazonaws.com/323183112872/nssp-dev-get-da-request-file-queue-updatev0"
sqs_poll_size = 5
glue_workflow = 'nssp-dev-match-npi-wkfl'


msg_b = {
"REQID": "1000132",
"RPTID": "4061",
"SHEET_NUM": "0",
"REQUEST_PLAN_LCNSEE": "LH017-2-01",
"REPORT_NAME": "Blue High Performance Network",
"PRODUCT_GRP_ID": "3",
"PLAN_FILE_PATH": "s3://nssp-dev-s3-cpl-upload-sa/OrderColTest_1000132.csv",
"REQUEST_S3_PATH": "s3://nssp-dev-s3-da-match-request-sa/OrderColTest_1000132.csv_NoSheet_4061_1000132.csv",
"SHEET_NAME": "NoSheet",
"PRODUCTS": "",
"WORKFLOW_ID": "2",
"PLAN_REQUEST_ETL_ID": "1142",
"COUNTY": "('')",
"ZIPCODE": "('')",
"EXCLPROVIDENFIED": "N",
"IGNSINGLEELEMMATCH": "N",
"USESECNAME": "N",
"INCLCOMPNAME": "N",
"INCLBDC": "N",
"INCLTC": "N",
"REPHOSPBASEPROV": "N",
"SUPRSANCPROV": "N",
"HPN_IND": "Y",
"HPN_TIER_INFO": "N",
"HPN_PRODUCTS": "('AL3E','AZ2E','CAE1','CA6E','CO3E','CT2E','FL3E','GA2E','IL2E','IN1Q','KY2E','LA2E','ME1R','DC3E','MA2E','MIE3','MN2E','MSE1','MO8E','MO7E','NV1H','NH2E','NJ5E','NY8E','NY9E','NC1E','OH1C','OK2E','OR1E','EPA7','EPA5','RI2E','SC8E','TN2E','TX1E','UT1E','VA2E','EWA5','WA2E','WI2E')",
"HPN_MARKET_COVERAGE_ID": "(1,4,7,8,9,10,11,12,13,14,15,16,17,18,19,20,23,24,25,28,29,30,31,32,33,34,35,36,41,42,44,45,46,47,51,52,54,55,57,60,61,62,64,66,68,69,70,71,72,74,75,76,80,81,82,85,87,91,92,97,98,99,100,103,104,105,106,107,108,110,111,112,113,115,116,117,118,119,124,126,129,130,131,132,133,134,135,136,137,139,142,143,145,146,147,150,151,155,156,158,159,160)",
"FILE_DELIMITER": "COMMA",
"STATE_CODE": "",
"TIER_INFO": "1",
"CUSTOM_FLAG": "N",
"CUSTOM_PRODUCTS": "('')",
"CUSTOM_STATES": "('')",
"CUSTOM_ZIPCODES": "('')",
"CUSTOM_SWAP": "N",
"PRODUCT_STATE_FLAG": "N"
}



def sendMessagesToQueue(qUrl, Nmsg):
    for i in range(Nmsg):
        reqid = str(i).zfill(6)
        msg_b["REQID"] = reqid
        response = sqsclient.send_message(
            QueueUrl=qUrl,
            MessageBody=json.dumps(msg_b)
        )    
        print(i, "  send msg response:  ", response)



def getMessagesFromQueue(qUrl, Nmsg=1):  #Nmsg can not be more than 10
    # Receive message from SQS queue
    if Nmsg>10:
        Nmsg = 10
    response = sqsclient.receive_message(
        QueueUrl=qUrl,
        MaxNumberOfMessages=Nmsg,
        VisibilityTimeout=60 #14400
    )
    print('SQS Response Recieved:  \n', response)
    if('Messages' in response):
        return response['Messages']
    else:
        print("No messages in queue.")
        return None


def triggerGlueWorkFlow(bucket_name, file_key):
    try:
        input= {
            'bucket_name': bucket_name,
            'file_key': file_key
        }
        print("Bucket Name: {}".format(bucket_name))
        print("File Key {}".format(file_key))
        response = client.start_workflow_run(
            Name=myglue,
            RunProperties={
                'string': 'string'
            }
        )
        print("Invoked Glue workflow successfully: {}".format(response['executionArn']))
    except Exception as e:
        print(str(e))
        print("Failed to trigger stepfunction")
        raise e



SendMsgToQueue = True
ReadAndDeleteMsg = True



def lambda_handler(event, context):
    Nmsg = 12
    
    if SendMsgToQueue:
        sendMessagesToQueue(raw_queue_url, Nmsg)
    
    queue_poll = 0
    totalMessages = 0
    if ReadAndDeleteMsg:
        while queue_poll < sqs_poll_size:
            queue_poll += 1
            messages = getMessagesFromQueue(raw_queue_url)
            if(messages):
                for message in messages:
                    receipt_handle = message['ReceiptHandle']
                    messageBody = json.loads(message['Body'])
                    reqID = messageBody['REQID']
                    print("for mesgID:  ", message['MessageId'],  "    reqID:  ", reqID)
                    
                    # Delete received message from queue
                    sqsclient.delete_message(
                        QueueUrl=raw_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    totalMessages += 1
                    print("Total messages processed and deleted:   {}".format(totalMessages))
            else:
                queue_poll = sqs_poll_size
    
    print("queue_poll  and  sqs_poll_size  :", queue_poll, sqs_poll_size)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
