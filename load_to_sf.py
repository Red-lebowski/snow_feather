import logging
import configparser

from .connect_to_sf import run_sql
from .s3_utils import upload_file_to_s3

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

