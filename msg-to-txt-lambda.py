import json
import boto3
import os, re, shutil, glob
import convhelper
import time
import independentsoft
import outlookmsgfile
import extract_msg
from independentsoft.msg import Message
from independentsoft.msg import Attachment
from zipfile import ZipFile
from boto3.s3.transfer import TransferConfig
import random, string

s3c = boto3.client('s3') 
s3r = boto3.resource('s3')
sqs = boto3.client('sqs')

file_system_path=os.environ['FILE_SYSTEM_PATH']
#sqs_queue_url=os.environ['SQS_QUEUE_URL']
#file_system_path="/tmp"

regexes = [r'^~wrd\d{3}.jpg$', r'^[0-9a-f]{6}.png$', r'^image\d{3}.',
r'^att\d{5}.', r'^image[0-9a-f]{6}.', r'^[0-9a-f]{32}.(jpg|png)$',
r'^(?=.\d)(?=.[a-f])[0-9a-f]{6}.(jpg|png)$', r'^outlookemoji-.png$',
r'^how to unzip.html$', r'^.[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}.(jpg|png)$',
r'^untitled attachment \d{5}.', r'^lock.png$',"Binary file*"
]
combined = "(" + ")|(".join(regexes) + ")"

def cleanString(y):
    fname = [char for char in y if char.isalnum() or char == '.']
    fname = ''.join(fname)
    fname, fext = os.path.splitext(fname)
    fname = fname.replace(".", "") + fext
    return fname

def convMsgToText(sourceBucket, srcFileName, anaFileName):
    #txt_file = msg_file.rsplit('.',1)[0] + "-analysis.txt"
    myfile = open(file_system_path+'/'+ anaFileName, "w") 
    myfile.write(outlookmsgfile.load(file_system_path+'/'+ srcFileName).as_string()) 
    myfile.close()
    return

def saveMsgBodyIndependent(sourceBucket, srcFileName, anaFileName):
    msg = Message(file_path = file_system_path+'/'+srcFileName)
    fullText = []
    fullText.append(str(msg.sender_name))
    fullText.append(str(msg.subject))
    fullText.append(str(msg.body))
    fullText = '\n'.join(fullText)
    myfile = open(file_system_path+'/'+ anaFileName, "w")
    myfile.writelines(fullText)
    myfile.close()
    return


def get_proper_attachment_name(srcFileName, attachment_name, counter, add_random=False):
    src_file_prefix = os.path.splitext(srcFileName)[0]
    clean_attachment_name, clean_attachment_ext = os.path.splitext(cleanString(attachment_name))
    clean_attachment_name = clean_attachment_name[:6]
    if add_random:
        clean_attachment_name += ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    levels_suffix = ""
    counter_str =  "{0:0=2d}".format(counter)
    if ".msg-attachment" in src_file_prefix:
        levels_suffix = re.search("\.msg-attachment([_\d]+)", src_file_prefix).groups()[0]
        src_file_prefix = src_file_prefix.replace(".msg-attachment"+levels_suffix, "")
    return src_file_prefix + "." + "_".join([counter_str, clean_attachment_name]) + "_".join([".msg-attachment"+levels_suffix, counter_str]) + clean_attachment_ext


def save_msg_attachments(sourceBucket, srcFileName, targetFile):
    target_key = "/".join(targetFile.split("/")[:-1])
    attchlist = []
    try:
        print("Trying Msg Attachment save using IndependentSoft")
        msg = Message(file_path = file_system_path+'/'+srcFileName)
        get_attachment_file_name = lambda x: x.file_name
        save_attachemnt_func = lambda attachment, efsFileName: attachment.save(efsFileName)

    except Exception as e:
        print("Failed saveMsgAttachmentIndependent with error -- ", e)
        print("Try saving msg attachment using Extract_Msg")
        msg = extract_msg.Message(file_system_path+'/'+srcFileName)
        get_attachment_file_name = lambda x: x.longFilename
        save_attachemnt_func = lambda attachment, efsFileName: (
                attachment.save(customPath = file_system_path),
                os.rename(str( file_system_path + '/' +  str(attachment.longFilename)),  efsFileName )
            )
        
    print("Total Number of attachments: " + str(len(msg.attachments)) )
    counter = 00
    for attachment in msg.attachments:
        attachment_name = get_attachment_file_name(attachment)
        
        try:
            print("--> attachment fileName:     ", attachment.file_name, attachment.longFilename)
        except Exception as e:
            print("--> file_name method not working!!!   -- ", e)
            try:
               print("--> attachment fileName:     ", attachment.longFilename) 
            except Exception as ee:
                print("--> longFilename method not working!!!   -- ", ee)            
        print("--> attachment_name:    ", attachment_name)
        
        # attachment_extenshion = attachment_name.split(".")[-1]
        if re.match(combined, attachment_name):
            print(attachment_name + "  *** mail signature file: will not be saved ***")
            continue
        else:
            print(attachment_name + "  ==> will be working on attachment file <==")
        if ".zip" in attachment_name.lower():
            save_attachemnt_func(attachment, file_system_path + "/" + attachment_name)
            zipfile = ZipFile(open(file_system_path + "/" + attachment_name, 'rb'))
            for name in zipfile.namelist():
                if(len(name.split('/')[-1]) > 0):
                    counter = counter + 1
                    proper_file_name = get_proper_attachment_name(srcFileName, name, counter)
                    s3r.meta.client.upload_fileobj(zipfile.open(name), Bucket=sourceBucket, Key=target_key + "/" + proper_file_name)
                    attchlist.append(proper_file_name)
        else:
            counter = counter + 1
            proper_file_name = get_proper_attachment_name(srcFileName, attachment_name, counter)
            efsFileName = file_system_path + "/" + proper_file_name
            save_attachemnt_func(attachment, efsFileName)
            print("Saving Attachment:  from " + efsFileName + "  to " + sourceBucket + "/" + target_key + "/" + proper_file_name)
            s3r.Object(sourceBucket, target_key + "/" + proper_file_name).put(Body=open(efsFileName, 'rb'))
            attchlist.append(proper_file_name)
            
    return attchlist
            
def chkFilePresent(file, path):
    print("checking for file:  ", path, "/", file)
    if os.path.exists(path+"/"+file):
        return True
    else:
        return False

def lambda_handler(event, context):
    print("Event = " + str(event))
    fileName = event['fileName']
    sourceBucket = event['sourceBucket']

    srcFileName, anaFileName, targetFile = convhelper.getFilesNamePath(sourceBucket, fileName)

    if "tmp" in file_system_path:
        convhelper.cpToTmpFolder(sourceBucket, fileName, srcFileName)
    else:
        if chkFilePresent(srcFileName, file_system_path):
            print("File  ", srcFileName, "   is present at:  ", file_system_path, "    no need to copy")
        else:
            print("copying file:  ", srcFileName, "    to    ", file_system_path)
            convhelper.cpToFileSystem(sourceBucket, fileName, srcFileName, file_system_path)

    print(" srcFileName, anaFileName, targetFile ::  ", srcFileName, "    ", anaFileName, "    ", targetFile )
    attchlist = []
    print("Working On Attachments")
    try:
        attchlist = save_msg_attachments(sourceBucket, srcFileName, targetFile)
    except Exception as e:
        print("Failed saveMsgAttachmentExtractMsg as well with error -- ", e)
        print("****     No attachment will be saved for this message     ****")
        # Copy file to error bucket
        s3c.copy_object(ACL='private', Bucket=event["errorBucket"], Key="msg_files_failed_to_save_attacment/" +
                re.sub("\W+", "_", type(e).__name__) + "/" + srcFileName, CopySource={'Bucket': sourceBucket, 'Key':fileName})
    
    print("Working On Message Body")    
    try:
        print("Trying saving msg body using IndependentSoft")
        saveMsgBodyIndependent(sourceBucket, srcFileName, anaFileName)
    except Exception as e:
        print("Failed IndependentSoft body msg read with error -- ", e)
        print("Try body msg read with Outlookmsg:", str(e))
        convMsgToText(sourceBucket, srcFileName, anaFileName)

    if "tmp" in file_system_path:
      convhelper.cpFrmTmpToS3(sourceBucket, targetFile, anaFileName) 
    else:
      convhelper.cpFrmFileSystemToS3(os.environ['DOCUMENT_EXTRACT_RESULTS'], targetFile, anaFileName, file_system_path)

    if(len(attchlist)>0):
        for myfile in attchlist:
            print("Will be submitting job for: ", sourceBucket, myfile)
          
    input = {}
    print("event::   ", event)
    if 'fileName' in event: #MUST
        input['fileName'] = event['fileName'] 
    if 'sourceBucket' in event: #MUST
        input['sourceBucket'] = event['sourceBucket']
    
    if 'fileType' in event:
        input['fileType'] = event['fileType']
    else:
        input['fileType'] = 'MSG'
        
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
    print("Returned input::   ", input)
    return input