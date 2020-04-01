import boto3
import logging
import configparser
from botocore.exceptions import ClientError

from .connect_to_sf import get_config_info

def upload_file_to_s3(file_name, bucket, object_name):
    """Upload a file to an S3 bucket
    
    :param file_name: File to upload. 
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    logging.info(f'Loading file: {file_name} to bucket: {bucket} with name: {object_name}')
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    
    profile = get_config_info('AwsProfile')
    
    session = boto3.Session(profile_name=profile)

    # Upload the file
    s3_client = session.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        logging.info(response)
    except ClientError as e:
        logging.error(e)
        return False
    
    print('Uploaded file to S3 with object name: ', object_name)
    return True