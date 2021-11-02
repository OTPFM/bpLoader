from blbrgPrice import blbrg
import datetime as dt
import pathlib
import os
import time
import json
from logger import *
# from IPython.display import clear_output
from dataclasses import dataclass, field, asdict
from shutil import copyfile
from enum import Enum

import pRequests

import win32event
import win32api
from winerror import ERROR_ALREADY_EXISTS

### SINGLETON
mutex = win32event.CreateMutex(None, False, 'name')
last_error = win32api.GetLastError()

if last_error == ERROR_ALREADY_EXISTS:
    print("Instance already running.")

else:
    REQ_SOURCE = pathlib.Path('requests')
    RESP_SOURCE = pathlib.Path('responses')
    REQ_DEBUG_SOURCE = pathlib.Path('requests_debug')

    for folder in REQ_SOURCE, RESP_SOURCE, REQ_DEBUG_SOURCE:
        if not os.path.isdir(folder):
            os.mkdir(folder)

    requests = dict()
    factory = pRequests.RequestFactory(REQ_SOURCE,
                                       RESP_SOURCE,
				       REQ_DEBUG_SOURCE,
                                       pRequests.frequencyUpdater(),
                                       pRequests.BloombergAdapter(blbrg))
    while True:
        factory.f.wait()
        factory.step()


