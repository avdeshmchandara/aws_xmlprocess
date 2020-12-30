"""
This lambda function is used to convert any cc one xml file to flat json file.
"""
import json
import boto3
import os
import csv
import collections
import xml.etree.cElementTree as ElementTree
import xmltodict
import fileHelper
from itertools import chain
from functools import reduce

s3c = boto3.client('s3')
s3r = boto3.resource('s3')

#Find dict for a tab only
#This function is used to find EventTopic value only
def getValueForTab(xmlfilename="Update.xml", forTab="EventTopic"):
    tabdict = {}
    myroot = ElementTree.parse(xmlfilename).getroot()
    for child in reduce(lambda x, _: chain.from_iterable(x), range(0), myroot):
        if(str(child.tag).split('}')[1]==forTab):
            tabdict[forTab] = child.text
            return  tabdict

#In any given xml file, find get all tabs, returned as a list
#Below function is generic but optimized for cc one xml file to start at Payload [ #of childs ]
def getListOfTabs(srcFile="Update.xml", childN=2):
    listof_tabs = []
    tree = ElementTree.parse(srcFile)
    root = tree.getroot()
    for child in reduce(lambda x, _: chain.from_iterable(x), range(childN), root):
            listof_tabs.append(str(child.tag).split('}')[1])
        #print(listof_tabs)
    return listof_tabs


#This will be used to flat Out the Dictionary
def flatten_dict(dd, separator='.', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }

#Flat the list of Dict dynamically with 0 indexing
def flatten_listDict(d, sep="."):
    obj = collections.OrderedDict()
    def recurse(t, parent_key=""):
        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t
    recurse(d)
    return obj

#From Each tab, extract Info
def getTabInfoDict(myxml="parsed_xml", mytab="DocumentInfo"):
    outdict = {}
    for k, v in myxml.items():
        if mytab in k:
            ksplt: str = str(k.split(mytab)[1][1:])
            #mykey = mytab
            #if(len(ksplt)):
            #    mykey = mytab + "." + ksplt
            #outdict[mykey] = v
            outdict[ksplt] = v
    return outdict

#This function is used to replace all True/False with 1/0
def replaceTrueFalseTo10(mydict):
    for key, value in mydict.items():
        #print(key,"   key_val  : ",value)
        if value == "true":
            mydict[key] = 1
        if value == "false":
            mydict[key] = 0
    return mydict

#This is main function with convert xml to flat json
#The xml file is expected to be copied at /tmp folder
#It will also append EventTopic
def parseXmlToJson(tmpFile, jsonFile):
    with open(tmpFile, 'rb') as f:
        xml_content = xmltodict.parse(f)
    flattened_xml = flatten_dict(xml_content)
    listof_tabs = getListOfTabs(tmpFile)
    #print(listof_tabs)
    alltabsInfo = {}
    for xtab in listof_tabs:
        alltabsInfo[xtab] = replaceTrueFalseTo10( flatten_listDict( getTabInfoDict(flattened_xml, xtab) ) )
    try:
        alltabsInfo["EventTopic"] = getValueForTab(tmpFile) 
    except:
        alltabsInfo["EventTopic"] = None
    
    aj = open(jsonFile, "w")
    aj.write(json.dumps(alltabsInfo, indent=4))
    aj.close()


def lambda_handler(event, context):
    """
    Lambda start function, based on event trigger
    If event is empty or incorrect, will try to use fixed file
    The xml will be copied to tmp before converting it to flat json
    """
    srcBucket = "xml-files-em"
    trgBucket = "flatten-json-em"
    srcFile   = "labor_assignments/9060754C-C363-4F22-A127-FC46AE0FD50F_LaborAssignment_Create.xml"
    print("event: ", event)

    try:
        srcBucket = event['Records'][0]['s3']['bucket']['name']
        srcFile = event['Records'][0]['s3']['object']['key']
        
    except:
        print("Will be using default values from Lambda")
        
    tmpFile = '/tmp/' + os.path.basename(srcFile)
    frmExtn = ".xml"
    toExtn = ".json"
    toFile = tmpFile.replace(frmExtn, toExtn)
    print("fileName:     ", srcFile)    
    print("sourceBucket: ", srcBucket)
    print("targetBucket: ", trgBucket)
    print("tmpFile: ", tmpFile)
    print("toFile: ", toFile)

    fileHelper.cpToTmpFolder(srcBucket, srcFile)
    #s3c.download_file(srcBucket, srcFile, tmpFile)
    parseXmlToJson(tmpFile, toFile)
    fileHelper.cpFrmTmpToS3(srcFile, trgBucket, frmExtn, toExtn)
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(srcFile)
    }