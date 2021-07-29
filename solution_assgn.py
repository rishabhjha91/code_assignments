import os
import xml.etree.ElementTree as ET
import requests
import urllib.request
import zipfile
import pandas as pd
import numpy as np

import s3_upload_download
from s3_upload_download import upload_file
from s3_upload_download import s3_engine

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

def get_xmlroot(file_name):
    '''
    Create root of xml
    '''
    
    full_file = os.path.abspath(os.path.join(file_name))
    mytree = ET.parse(full_file)
    myroot = mytree.getroot()
    return myroot


def find_url(myroot):
    '''
    Find the zip link
    '''
    
    contents=[]
    for x in myroot[1][0]:
        if x.tag == 'str':
            contents.append(x.text)
    logger.info('Found zip link to download XML')
    return(contents[1])


def find_xml(zip_url):
    '''
    Unzip and convert into XML file
    Remove the downloaded zip file
    '''
    
    remote = urllib.request.urlopen(zip_url)  # read remote file
    zip_data = remote.read()  # read from remote file
    remote.close()  # close urllib request
    
    dwnld_zip = open('assign.zip', 'wb')  # write binary to local file
    dwnld_zip.write(zip_data)
    dwnld_zip.close()

    handle_zip = zipfile.ZipFile('assign.zip')
    handle_zip.extractall()
    handle_zip.close()
    logger.info('Unzip and converting into XML')
    os.remove('assign.zip')

    
    
def main():
    '''
    Download XML file from the link given in the assignment, converts the xml data into csv
    and uploads to s3
    Credentials of s3 bucket is given in module which connects to specific s3 bucket
    At last, it deletes the generated xml from the current directory.
    An option for deleting the csv file is also given(commented, in case the file is inaccessible on s3)
    Downloading the csv from s3 is also as an option.
    '''
    
    
    url1= 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'
    dwnld_xml= requests.get(url1)

    file_name= 'assignment.xml'
    with open(file_name, 'w') as f:
        f.write(dwnld_xml.text)
    dwnld_xml.close()
    logger.info('Got the downloaded xml from the link given in the assignment')
   
    xml_return = get_xmlroot(file_name)
    zip_url = find_url(xml_return)
    find_xml(zip_url)

    extract_xml = 'DLTINS_20210117_01of01.xml'
    zip_xml = get_xmlroot(extract_xml)
    logger.info('Getting xml data of DLTINS_20210117_01of01 XML as per the requirement')

    
#     Create dictionary from reqd attributes

    logger.info('Creating dictionary having keys and value pair according to the requirement')
    dict1={'FinInstrmGnlAttrbts.Id':[],
           'FinInstrmGnlAttrbts.FullNm':[],
           'FinInstrmGnlAttrbts.ClssfctnTp':[],
           'FinInstrmGnlAttrbts.CmmdtyDerivInd':[],
           'FinInstrmGnlAttrbts.NtnlCcy':[],
           'Issr':[]
          }
    for x in zip_xml.findall('.//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts'):
        id1 = x.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id').text
        fullNm = x.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm').text
        clssfctnTp = x.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp').text
        cmmdtyDerivInd = x.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd').text
        ntnlCcy = x.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy').text

        dict1['FinInstrmGnlAttrbts.Id'].append(id1)
        dict1['FinInstrmGnlAttrbts.FullNm'].append(fullNm)
        dict1['FinInstrmGnlAttrbts.ClssfctnTp'].append(clssfctnTp)
        dict1['FinInstrmGnlAttrbts.CmmdtyDerivInd'].append(cmmdtyDerivInd)
        dict1['FinInstrmGnlAttrbts.NtnlCcy'].append(ntnlCcy)

    for x in zip_xml.findall('.//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr'):
        dict1['Issr'].append(x.text)
    
    
    # Create DataFrame from the dictionary and convert to csv
    logger.info('Creating DataFrame from the dictionary and csv file')
    df= pd.DataFrame(dict1)
    assgn_file = 'python_solved_assgn.csv'
    df.to_csv(assgn_file,index=False)


    # Upload csv to s3
    logger.info('Uploading the file to S3')
    file_upload= upload_file(assgn_file)
    logger.info(file_upload)
    
                
    # Remove donloaded xml and csv files from current directory
    logger.info('Deleting the code generated XML files from the current directory')
    os.remove(file_name)     # XML file generated from the link
    os.remove(extract_xml)   # XML file from which data is extracted
#     os.remove(assgn_file)    # If we want to delete the generated csv file


    # *** To download the file from s3***
#     logger.info('Downloding file from S3')  
#     s3_client = s3_engine()
#     s3_client.download_file('rishabhbucket93','python_solved_assgn.csv','python_solved_assgn.csv')
    
if __name__ == "__main__":
    main()