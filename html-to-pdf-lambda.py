import pdfkit
import os,json
import boto3
import subprocess
import urllib
import convhelper
import io
import csv

s3c = boto3.client('s3')
s3r = boto3.resource('s3')
file_system_path=os.environ['FILE_SYSTEM_PATH'] 

config = pdfkit.configuration(wkhtmltopdf="/opt/bin/wkhtmltopdf")
print("Config::  ", config)
#subprocess.run("ldd /opt/bin/wkhtmltopdf".split(), check=True)
#subprocess.run("wkhtmltopdf -V".split(), check=True) 

def chkFilePresent(file, path):
    print("checking for file:  ", path, "/", file)
    if os.path.exists(path+"/"+file):
        return True
    else:
        return False

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))
    try:
        fileName       = event['fileName']
        sourceBucket   = event['sourceBucket']
    except:
        print("will be using local fileName: ", fileName)
        print("will be using local sourceBucket: ", sourceBucket)
        
        
    IndexFile = fileName.replace( fileName.slpit(".")[-1]
    
    #s3_object = s3c.get_object(Bucket=your_bucket, Key=key_of_obj)
            
        
    
    srcFileName, anaFileName, targetFile = convhelper.getFilesNamePath(sourceBucket, fileName)
    print("srcFileName,  anaFileName, targetFile :", srcFileName, anaFileName, targetFile)
    extn = fileName.split(".")[-1]
    targetFile = fileName.replace(extn, "pdf")
    convertedFile = targetFile.split("/")[-1]

    
    if chkFilePresent(srcFileName, file_system_path):
        print("File  ", srcFileName, "   is present at:  ", file_system_path, "    no need to copy")
    else:
        print("copying file:  ", srcFileName, "    to    ", file_system_path)
        convhelper.cpToFileSystem(sourceBucket, fileName, srcFileName, file_system_path)
    
    replaceStrFrm = 'rows="5"'
    replaceStrTo  = 'rows="25"'
    with open(file_system_path+"/"+srcFileName, 'r') as file :
        filedata = file.read().replace(replaceStrFrm, replaceStrTo)
        file.close()

    srcFileNameTmp = srcFileName + "_tmp"
    with open(file_system_path+"/"+srcFileNameTmp, 'w') as file:
        file.write(filedata)
        file.close()
    
    bad_words = ['http://', 'https://']
    with open(file_system_path+"/"+srcFileNameTmp) as oldfile, open(file_system_path+"/"+srcFileName, 'w') as newfile:
        for line in oldfile:
            if not any(bad_word in line for bad_word in bad_words):
                newfile.write(line)    
    
    try:
        os.remove(file_system_path+"/"+convertedFile)
    except Exception as e:
        print("Unable to remove file:   ", e )
    
    #print("filedata::    ", filedata)
    print("file_system_path+srcFileName, file_system_path+convertedFile   ", file_system_path, srcFileName, file_system_path, convertedFile)    

    try:
        pdfkit.from_file(file_system_path+"/"+srcFileName, file_system_path+"/"+convertedFile, configuration=config, options={"enable-local-file-access": None})
    except Exception as e:
        print("Unable to run::   ", e)
        return e
    
    print("sourceBucket, targetFile, convertedFile, file_system_path  -- ", sourceBucket, targetFile, convertedFile, file_system_path )
    convhelper.cpFrmFileSystemToS3(sourceBucket, targetFile, convertedFile, file_system_path)    
    
    if os.path.exists(file_system_path+"/"+convertedFile):
        print("file present at:   ", file_system_path+"/"+convertedFile , "    and its size: ", os.path.getsize(file_system_path+"/"+convertedFile) )
    
    input = {}
    print("event::   ", event)
    if 'fileName' in event: #MUST
        input['fileName'] = event['fileName'] 
    if 'sourceBucket' in event: #MUST
        input['sourceBucket'] = event['sourceBucket']
    if 'fileType' in event:
        input['fileType'] = event['fileType']
    else:
        input['fileType'] = 'DOC'
    if 'errorBucket' in event:
        input['errorBucket'] = event['errorBucket']
    else:
        input['errorBucket'] = 'medpro-dev-ds-error-logs-bucket'
    if 'stepFuncExecId' in event:
        input['stepFuncExecId'] = event['stepFuncExecId']
    else:
        input['stepFuncExecId'] = 'LambdaTestEvent'
    if 'extractionStatus' in event:
        input['extractionStatus'] = event['extractionStatus']
    else:
        input['extractionStatus'] = 'IN_PROGRESS'
    if 'anaFileName' in event:
        input['anaFileName'] = event['anaFileName']
    else:
        input['anaFileName'] = anaFileName
    if 'targetFile' in event:
        input['targetFile']  = event['targetFile']
    else:
        input['targetFile']  = targetFile
    #print("Returned input::   ", input)
    return input