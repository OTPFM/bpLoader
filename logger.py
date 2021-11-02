"""Logger declaration and parametrization"""
import os
import sys
import logging
from logging import DEBUG, getLogger, StreamHandler, Formatter
from logging.handlers import TimedRotatingFileHandler


LOGGER = getLogger(__name__)
LOGGER.setLevel(DEBUG)
LOG_FILE = "log.txt"

logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format='[%(levelname)s] %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

# handler = StreamHandler(sys.stdout)
# handler.setFormatter(Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
#                                datefmt='%m/%d/%Y %I:%M:%S %p'))
# LOGGER.addHandler(handler)
# handler = TimedRotatingFileHandler(LOG_FILE, when='W6')
# LOGGER.addHandler(handler)
