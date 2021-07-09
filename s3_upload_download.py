# To upload file to S3

import boto3
import boto
from botocore.exceptions import ClientError


import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

AWS_S3_BUCKET = 'rishabhbucket93'
AWS_ACCESS_KEY_ID = 'AKIA3H2D4YU27LBA3VLA'
AWS_SECRET_ACCESS_KEY = '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'

def s3_engine():
    '''
    Create s3 connection client
    '''
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
    )
    logger.info('Successfully created S3 client')
    return s3_client

def upload_file(file_name, bucket = AWS_S3_BUCKET, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = s3_engine()
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logger.info(e)
        return ("Upload to s3- Fail")
    return ("Upload to s3- Pass")
