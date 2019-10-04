import os
import logging
import datetime

def init_logger(folder_path=''):
    '''Wrapper for setting up logs for this run.

    :return: Boolean, if operation was succesful
    '''
    if not os.path.exists('./logs'):
        os.makedirs('./logs')

    now = datetime.datetime.now().isoformat()
    file_name = f'run_{now}.log'

    full_path = folder_path + file_name if folder_path else f'./logs/{file_name}'
    print(f'Created log file: {full_path}')

    logging.basicConfig(filename=full_path, 
                        filemode='w',
                        level=logging.INFO,
                        format='%(levelname)-10s: %(message)s'
    )
    return True