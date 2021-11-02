import time
import pathlib
from shutil import copyfile
import os
import logging
import random
from enum import Enum
import concurrent.futures
from logger import *
import json
from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod

class frequencyType(Enum):
    FAST = 1
    BASE = 3
    SLOW = 10

class Adapter(ABC):
    """
        Price downloading adapter abstract class
    """
    @abstractmethod
    def create_dump(self, request_json):
        pass

class BloombergAdapter(Adapter):
    def __init__(self, bloomber_price_loader):
        self.b = bloomber_price_loader

    def create_dump(self, request_json):
        self.b1 = self.b.from_dict(**request_json)
        self.b1.get_dump()

class TestAdapter(Adapter):
    def create_dump(self, request_json):

        time.sleep(random.random() * 2)

        with open(request_json['path'], "w") as outf:
            json.dump({"That's it" : "Hey"}, outf)
        LOGGER.info("HANDLING TEST")

class frequencyUpdater:
    """
        Logic to slow down main's while loop.
        Stores sleep value which serves as seconds to sleep for the loop.
        frequencyType BASE is default sleep value.
        By calling .speed() method sleep value will be reduced to
        frequencyType FAST for COUNTER_BASE iterations, then resumes.
    """
    COUNTER_BASE = 10

    def __init__(self):
        self.base_sleep = frequencyType.BASE.value
        self.base_counter = frequencyUpdater.COUNTER_BASE

        self._sleep = self.base_sleep
        self.counter = self.base_counter

    def speed(self):
        self.sleep = frequencyType.FAST.value

    @property
    def sleep(self):
        if self.base_sleep != self._sleep:
            if self.counter < 1:
                self._sleep = self.base_sleep
                self.counter = self.base_counter
            else:
                self.counter -= 1
        return self._sleep

    @sleep.setter
    def sleep(self, new_sleep):
        self._sleep = new_sleep

    def wait(self):
        time.sleep(self.sleep)

@dataclass(order=True)
class Request:
    """
        DataClass for request object.
        Object is initilaized as request json appeares
    """
    mtime: float
    content: dict = field(repr=False)
    alive: bool = True
    error: bool = False
    sort_index: int = field(init=False, repr=False)

    # content : dict = field(default_factory=dict, init=False, repr=False)
    # print(asdict(a))

    def __post_init__(self):
        object.__setattr__(self, 'sort_index', self.mtime)

    def error_occured(self):
        object.__setattr__(self, 'error', True)
        object.__setattr__(self, 'content', '')

    @classmethod
    def fromFile(cls, REQ_SOURCE, REQ_DEBUG_SOURCE, file_name, retry=False):
        mtime = pathlib.Path(os.path.join(REQ_SOURCE, file_name)).stat().st_mtime
        error = False
        try:
            with open(os.path.join(REQ_SOURCE, file_name), 'r') as j:
                content = json.load(j)
                try:
                    LOGGER.info(f' request {file_name} opened for ticker: {content["bloomberg_code"]}.')
                except KeyError:
                    LOGGER.warning(f' request {file_name} opened for ticker. No content loaded.')

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            if retry:
                LOGGER.info(f' JSONDecodeError or UnicodeDecodeError @ {file_name} w {e}.')
                error = True
                content = {}
            else:
                time.sleep(0.5)
                return cls.fromFile(REQ_SOURCE, REQ_DEBUG_SOURCE, file_name, retry=True)

        except OSError:
            LOGGER.warning(f' request {file_name} went missing.')
            error = True
            content = {}

        try:
            copyfile(os.path.join(REQ_SOURCE, file_name), os.path.join(REQ_DEBUG_SOURCE, file_name))
            os.remove(os.path.join(REQ_SOURCE, file_name))
        except OSError:
            LOGGER.warning(f' unable to delete {file_name}.')

        return cls(mtime=mtime, error=error, content=content)


class DuplicatedEntryError(Exception):
    """
    Exception raised for errors when requests directory
    processed and during merging its contents to
    requests in memory a duplicate occurred.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Duplicated request in memory found. Resolved by keeping one. Undefined behaviour."):
        self.message = message
        super().__init__(self.message)


class RequestFactory:
    DELETE_AFTER_SECS = 20
    def __init__(self,
                 REQ_SOURCE: str,
                 RESP_SOURCE : str,
                 REQ_DEBUG_SOURCE: str,
                 frequency_updater: frequencyUpdater,
                 adapter : Adapter):
        self.f = frequency_updater
        self.REQ_SOURCE = REQ_SOURCE
        self.REQ_DEBUG_SOURCE = REQ_DEBUG_SOURCE
        self.RESP_SOURCE = RESP_SOURCE
        self.adapter = adapter
        self.logging = logging
        self.requests_in_memory = dict()
        self.requests_from_directory = list()

        self.number_of_requests_before = 0
        self.number_of_requests_after = 0

    def get_requests_from_directory(self):
        """
            Check Requests directory. Load items to memory.
        """
        self.number_of_requests_before = 0
        self.number_of_requests_before += len(self.requests_in_memory)

        for req_file_name in os.listdir(self.REQ_SOURCE):
            if req_file_name not in self.requests_in_memory.keys():
                self.f.speed()  # if requests dir modified speed up iteration
                self.requests_from_directory.append(
                    {req_file_name: Request.fromFile(self.REQ_SOURCE, self.REQ_DEBUG_SOURCE,req_file_name)}
                )
        self.number_of_requests_before += len(self.requests_from_directory)

    def mergeDict(self):
        """
            Merging requests from directory to requests in memory

            Attributes:
                init_dict -- in memory dict (should be referenced)
                list_of_dicts -- extracted dicts from requests directory

        """
        for d in self.requests_from_directory:
            self.requests_in_memory = {**self.requests_in_memory, **d}

        self.requests_from_directory = list()
        self.number_of_requests_after = len(self.requests_in_memory)

    def handle_many_sync(self):
        """
            Calls handle_one for all requests
            in a sync fashion.
        """
        for requestObject in list(self.requests_in_memory.keys()):
            self.handle_one(requestObject)

    def handle_many_async(self):
        """
            Calls handle_one for all requests in threads.
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            requestObjects = list(self.requests_in_memory.keys())
            results = [executor.submit(self.handle_one, r) for r in requestObjects]

            for f in concurrent.futures.as_completed(results):
                f.result()

    def handle_one(self, requestTag):
        """
            Process request from memory by filename.
            Calls adapter - in prod blbrg, to handle response data.

        """
        if self.requests_in_memory[requestTag].error and self.requests_in_memory[requestTag].alive:
            LOGGER.info(f' {requestTag} is a bad request.')
            self.requests_in_memory[requestTag].alive = False
            with open(os.path.join(self.RESP_SOURCE, requestTag), "w") as outf:
                json.dump({"error": "JSON format error."}, outf)

        elif self.requests_in_memory[requestTag].alive and not self.requests_in_memory[requestTag].error:
            request_json = {**self.requests_in_memory[requestTag].content,
                            **{'path': os.path.join(self.RESP_SOURCE, requestTag)}}

            self.adapter.create_dump(request_json)
            LOGGER.info(f' request {requestTag} successfully sent.')
            self.requests_in_memory[requestTag].alive = False

        elif time.time() - self.requests_in_memory[requestTag].mtime > RequestFactory.DELETE_AFTER_SECS:
            LOGGER.info(f' request {requestTag} discarded.')
            del self.requests_in_memory[requestTag]

    def process(self):
        """
            Load batch from directory then merge it to memory.
        :return:
        """
        self.get_requests_from_directory()
        self.mergeDict()

        if self.number_of_requests_before != self.number_of_requests_after:
            raise DuplicatedEntryError

    def step(self):
        """
            Handling all requests.
        :return:
        """
        try:
            self.process()
        except DuplicatedEntryError:
            LOGGER.warning("Duplicate found in memory. One of them cleared.")

        self.handle_many_async()







