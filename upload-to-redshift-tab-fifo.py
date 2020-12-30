"""
This lambda function is used to upload data to redshift for each message of FIFO
Only one message will be worked on at a time, once its sucussfull another message will be pulled
from the queue. The upload will be either insert or upsert(explicit delete then insert) 
by comparing doc_id and create time stamp
"""
from datetime import datetime
import boto3
import time
import psycopg2
import collections
import re
import csv
import fileHelper
import os
import json
from functools import reduce
import itertools
import ast
s3c = boto3.client('s3')
s3r = boto3.resource('s3')

#Main function to push data on redshift
def PushCSVtoRedshiftTab(srcBucket, srcFile, doc_id, ro_ts):
    dn, dext = os.path.splitext(srcFile)
    dbschema = dn.split("/")[0]
    dbschema = "cc_staging"
    mytable = dn.split("/")[1]
    print("dbschema:  ", dbschema)
    print("tablename:  ", mytable)
    
    print("working for file: "+srcBucket+"/"+srcFile)
    print("May be inserted to table: "+ dbschema+"."+mytable+"  for doc_id and ro_ts  "+str(doc_id)+"  "+str(ro_ts))
    conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com', port='5439', user='tbguser', password='Tbguser12')
    cur=conn.cursor()
    cur.execute("begin;")
    
    queryS = "select estimate_doc_id from "+ dbschema+"."+mytable +" where estimate_doc_id =" + "'"+str(doc_id)+"'" + " ;"        
    print(queryS)
    cur.execute(queryS)
    rows = cur.fetchall()
    if not len(rows):
        print("estimate_doc_id: "+str(doc_id)+" is not in DB, Insert then EXIT! ")
        executeCom = "copy "+ dbschema+"."+mytable + " from 's3://" + srcBucket + "/" + srcFile + "' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
        cur.execute(executeCom)
        cur.execute("commit;")
        cur.close()
        conn.close()
        return
    else:
        print("estimate_doc_id: "+str(doc_id)+" is in DB, good for next step...")
        queryS = "select src_created_ts from "+ dbschema+"."+mytable +" where estimate_doc_id =" + "'" + str(doc_id) + "'" +" and src_created_ts >= " +"'" + str(ro_ts) + "'" +" ;"
        print(queryS)
        cur.execute(queryS)
        rows = cur.fetchall()
        if len(rows):
            print("Current TS: "+ro_ts+" is less or equal to DB TS: "+str(rows))
            print("Donot do anything!! EXIT")
            cur.close()
            conn.close()
            return
        else:
            print("Current TS: "+ro_ts+" is greater than atleast one of Database TS: "+str(rows))
            print("Delete All entries for estimate_doc_id: "+str(doc_id))
            delQuery = "delete from "+ dbschema+"."+mytable +" where estimate_doc_id = '" + str(doc_id) + "';" 
            print("delQuery: ", delQuery)
            cur.execute(delQuery)
            conn.commit()
            print("Insert current entry for current TS:     "+str(ro_ts)+"  then Exit!")
            executeCom = "copy "+ dbschema+"."+mytable + " from 's3://" + srcBucket + "/" + srcFile + "' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
            print("executeCom: ", executeCom)
            cur.execute(executeCom)
            cur.execute("commit;")
            cur.close()
            conn.close()
            return

#Get comparing info: doc_id and created_timestamp from a given pipe delim file
def getROTS(filename):
    ro_num = "repair_order_num"
    doc_id = "estimate_doc_id"
    ro_ts = "src_created_ts"
    #filename = "/tmp/" + filename.strip().split('/')[-1]
    f = [i.strip('\n').split("|") for i in open(filename, 'r')]
    ro_num_ind = f[0].index(ro_num)
    doc_id_ind = f[0].index(doc_id)
    ro_ts_ind = f[0].index(ro_ts)
    f = open(filename, 'r')
    lineone = f.readlines()[1].strip() 
    f.close()
    fields = lineone.split('|')
    return (fields[ro_num_ind], fields[doc_id_ind], fields[ro_ts_ind])

#Get number of lines from pipe delim file, to determine if file is empty
def getNumOfLines(filename):
    #filename = "/tmp/" + filename.strip().split('/')[-1]
    file = open(filename, 'r')
    nonempty_lines = [line.strip("\n") for line in file if line != "\n"]
    line_count = len(nonempty_lines)
    file.close()
    return line_count

def lambda_handler(event, context):
    """
    Lambda start function, based on event trigger
    If event is empty or incorrect, will try to use fixed file
    The pipe delim file will be inserted/upserted on redshift
    """
    srcBucket="pipedelimfiles-for-table"
    srcFile="repair_order/repair_order/RO_3612514224_uptime.csv"
    srcFile="repair_order/repair_order/20191001_040553410_21770891_1BB9BBD3-CA76-4B3A-A48B-267C245DA493_3600114739_RO_Update.csv"

    #print("event: ", event)
    try:
        srcBucket = event['Records'][0]['body'].split(':')[0]
        srcFile = event['Records'][0]['body'].split(':')[1]
    except:
        print("Will be using default values from Lambda")

    print("fileName:     ", srcFile)
    print("sourceBucket: ", srcBucket)

    tmpFile = '/tmp/' + os.path.basename(srcFile)
    print("tmpFile: ", tmpFile)
    s3c.download_file(srcBucket, srcFile, tmpFile)
    
    num_lines = getNumOfLines(tmpFile)
    
    print("Total Number of Lines in File: ", num_lines)
    if(num_lines < 2):
        return {
            'statusCode': 200,
            'body': json.dumps("Empty File:")
        }
    
    ro_num, doc_id, ro_ts = getROTS(tmpFile)
    print("ro_num = " +  str( ro_num) )
    print("doc_id = " +  str( doc_id) )    
    print("ro_ts  = " +  str( ro_ts ) )
    
    
    PushCSVtoRedshiftTab(srcBucket, srcFile, doc_id, ro_ts)  

    '''
    while True:
        try:
            PushCSVtoRedshiftTab(srcBucket, srcFile, ro_num, ro_ts)        
            break
        except Exception as e:
            print("PushCSVtoRedshiftTab Failed: " + str(e)+" Sleeping for 10 sec before retrying")
            time.sleep(10)
    '''


    return {
        'statusCode': 200,
        'body': json.dumps("Done")
    }