import logging
import time


formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
formatter1 = logging.Formatter('%(message)s')

moment = time.strftime("%Y-%b-%d__%H_%M_%S", time.localtime())

filename1 = 'Logfile/' + 'logfile' + moment + '.log'
filename2 = 'Logfile/' + 'BQfile' + moment + '.log'


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter1)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


logger = setup_logger('loher_file', filename1)
# second file logger
super_logger = setup_logger('test_file', filename2)

if __name__ == '__main__':
    setup_logger('name','log_file', level=logging.INFO)
    # convert('logfile')
