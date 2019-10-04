import configparser
from snowflake.connector import connect, DictCursor, errors


def create_connection(config_profile='DEFAULT'):
    '''creates a connection from ini file

    :param config_profile: config profile name
    :returns: snowflake connection object
    '''

    config = configparser.RawConfigParser()
    config.read('config.ini')
    config_section = config[config_profile]

    config_keys = config_section.keys()
    for key in ['User', 'Password', 'Account', 'Warehouse', 'Database', 'Role']:
        if not key in config_keys: 
            print(f'{key} missing from ini file')
            exit()

    conn = connect(
        user      =config_section['SnowflakeUser'],
        password  =config_section['SnowflakePassword'],
        account   =config_section['SnowflakeAccount'],
        warehouse =config_section['SnowflakeWarehouse'],
        database  =config_section['SnowflakeDatabase'],
        role      =config_section['SnowflakeRole']
    )
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