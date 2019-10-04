import boto3
import logging
import configparser
from botocore.exceptions import ClientError

from connect_to_sf import run_sql


def load_file_with_stage(conn
                        , file_path
                        , stage_name
                        , table_name
                        , staging_bucket_name
                        , staging_bucket_folder=''):
    '''loads a file into SF using a predefined stage.
    :type conn: Snowflake connection object
    :param conn: The Snowflake connection object

    :type file_path: String
    :param file_path: path to the csv file to load. csv file must:
        1. Be ordered in the exact same order as the columns in the target table
        using nulls where the column doesn't have the data.
        2. Not have a header row.
    
    :type stage_name: String
    :param stage_name: The fully qualified path to the stage in SF. Like
        my_db.my_schema.my_stage.
    
    :type table_name: String
    :param table_name: The fully qualified path to the stage in SF. Like
        my_db.my_schema.my_stage.

    :type staging_bucket_name: String
    :param staging_bucket_name: name of the bucket. Shouldn't contain any '/'

    :type staging_bucket_folder: String, Optional
    :param staging_bucket_folder: name of the folder in the bucket. 
    '''
    object_name = staging_bucket_folder + file_path.split('/')[-1]
    logging.info(f'Uploading file: {object_name} to staging bucket: {staging_bucket_name}')
    upload_to_stage_success = upload_file_to_s3(file_path
                                         , staging_bucket_name
                                         , object_name)
                                        
    if not upload_to_stage_success:
        logging.error(f'Failed to upload file {file_path} to staging bucket')
        return False
    
    # loads only the specific file into the table
    load_sql = f'''
    COPY INTO {table_name}
    FROM @{stage_name}/{object_name}
    FILE_FORMAT = (type = csv)
    force = true
    '''

    load_results = run_sql(conn, load_sql)
    try:
        load_success = True
        logging.info('Load Results: ' + load_results[0]['status'])
    except TypeError:
        logging.error('Error Loading results into table: ' + load_results[0])
        load_success = False
    print('Load Success: ' + str(load_success))

    return load_success


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
    
    try:
        profile = configparser.ConfigParser()['DEFAULT']['AwsProfile']
    except KeyError:
        print("Error: Couldn't find AWS Profile in config file")
        return False
    
    session = boto3.Session(profile_name=profile)

    # Upload the file
    s3_client = session.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        logging.info(response)
    except ClientError as e:
        logging.error(e)
        return False
    return True