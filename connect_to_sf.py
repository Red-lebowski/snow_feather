import configparser
from snowflake.connector import connect, DictCursor, errors

def get_config_info(key):
    config = configparser.RawConfigParser()
    config.read('snow_feather_config.ini')
    config_section = config['DEFAULT']

    try:
        item = config_section[key]
    except KeyError:
        print(f"Error: Couldn't find {key} in config file")
        exit()
        return False
    return item


def create_connection(config_profile='DEFAULT'):
    '''creates a connection from ini file

    :param config_profile: config profile name
    :returns: snowflake connection object
    '''
    
    for key in ['SnowflakeUser','SnowflakePassword','SnowflakeAccount','SnowflakeWarehouse','SnowflakeDatabase','SnowflakeRole']:
        item = get_config_info(key)
        if not item: 
            print(f'{key} missing from ini file')
            exit()

    conn = connect(
        user      =get_config_info('SnowflakeUser'),
        password  =get_config_info('SnowflakePassword'),
        account   =get_config_info('SnowflakeAccount'),
        warehouse =get_config_info('SnowflakeWarehouse'),
        database  =get_config_info('SnowflakeDatabase'),
        role      =get_config_info('SnowflakeRole')
    )
    print('Connected to Snowflake.')
    return conn
    

def run_sql(conn, sql, as_json=True):
    if as_json:
        cur = conn.cursor(DictCursor)
    else:
        cur = conn.cursor()
        
    try:
        cur.execute(sql)
        res = cur.fetchall()
    except (errors.ProgrammingError) as e:
        print("Statement error: {0}".format(e.msg))
        res = ('Statement error: ' + str(e.msg),)
    except:
        print("Unexpected error: {0}".format(e.msg))
    finally:
        cur.close()
    return res