import datetime as dt
import sys
import os
from pandas.tseries.offsets import BDay
import numpy as np
import json
import blpapi
import re


class blbrg():
    """
    bloomberg data provider
    """
    def __init__(self, ticker, is_historic=False, start_date=None, end_date=None,
                 attributes="OPEN HIGH LOW PX_LAST VOLUME", path="dump.json", error=False):
        self.ticker = ticker
        self.error = error
        self.path = path

        if self.error:
            self.clean = "JSON format error."
        else:
            # Read in input attrib info
            self.attributes = attributes.split()
            self.is_ohlc =  not any([atr not in "OPEN HIGH LOW PX_LAST VOLUME".split()for atr in self.attributes])
            self.start_date = start_date
            self.end_date = end_date
            self.is_historic = is_historic

            # Get raw data from API
            self.getRaw()
            # Check for errors in raw
            if not self.is_error():
                if self.is_ohlc:
                    self.clean = self.clean_raw()

                elif not self.is_ohlc:
                    self.clean = self.clean_raw_not_ohlc()

    def clean_raw_not_ohlc(self):
        clean = {}
        for attrs in self.attributes:
            extracted = re.search(f'({attrs} = )(.*)(\n)', self.raw)
            if extracted is not None:
                clean[attrs] = extracted.group(2)
        return clean

    def is_error(self):
        self.error = re.search('(message = ")(.*?")', self.raw)
        if self.error:
            self.error = self.error.group(2)
            self.clean = self.error
            return True
        return False

    def getRaw(self):
        if not self.is_historic and self.is_ohlc:
            self.raw = self.get(self.attributes + ['CRNCY'])
        else:
            # Ohlc historic or not ohlc data and only one field can be pulled at a time
            self.raw = self.get(self.attributes)


    def get(self, attribs):
        """Get price from bloomberg terminal real-time or historic"""
        try:
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost('localhost')
            sessionOptions.setServerPort(8194)
            session = blpapi.Session(sessionOptions)

            if not session.start():
                print("Failed to start session.")

            if not session.openService("//blp/refdata"):
                print("Failed to open //blp/refdata")

            refDataService = session.getService("//blp/refdata")

            if self.is_historic:
                request = refDataService.createRequest("HistoricalDataRequest")
            else:
                request = refDataService.createRequest("ReferenceDataRequest")

            request.getElement("securities").appendValue(self.ticker)

            for atr in attribs:
                request.getElement("fields").appendValue(atr)

            if self.is_historic:
                request.set("periodicityAdjustment", "ACTUAL")
                request.set("periodicitySelection", "DAILY")
                request.set("startDate", self.start_date)
                request.set("endDate", self.end_date)

            session.sendRequest(request)

            result_string = ""
            try:
                while (True):
                    ev = session.nextEvent(500)
                    for msg in ev:
                        result_string += str(msg)
                    if ev.eventType() == blpapi.Event.RESPONSE:
                        break
            finally:
                session.stop()
            return result_string
        except blpapi.InvalidArgumentException:
            return "Failed to start session."

    def clean_raw(self):
        attributes = self.attributes.copy()
        try:
            if self.is_historic:
                attributes.insert(0, 'date')
                clean = {key: list() for key in attributes}

                for grain in self.raw.split("fieldData = {")[1:]:
                    for idx, atr in enumerate(attributes):
                        datapoint = grain[grain.find(atr) + len(atr + ' ='):][
                                    :grain[grain.find(atr) + len(atr + ' ='):].find('\n')]
                        datapoint = datapoint.strip()
                        if idx: # TO FLOAT
                            try:
                                datapoint = float(datapoint) # .strip()
                            except ValueError:
                                datapoint = None
                            except TypeError:
                                pass        
                        clean[atr].append(datapoint)

                clean = {key: val[::-1] for key, val in clean.items()}

                # REMOVE LINES WITH NONE IN THEM
                none_idx = set()
                for key in clean.keys():

                    # IF A WHOLE COLUMN IS NONE THEN DO NOTHING
                    # if all(v is None for v in clean[key]):
                    #     none_idx = set()
                    #     break

                    for idx, value in enumerate(clean[key]):
                        if isinstance(value, type(None)):
                            none_idx.add(idx)

                for idx in sorted(none_idx, reverse=True):
                    for key in clean.keys():
                        del clean[key][idx]
                # REMOVE LINES WITH NONE IN THEM

            else:
                clear = self.raw.replace("\n", "")
                clear = re.search(r'(fieldData = {)(.*?})', clear).group(2).strip()
                attributes.insert(0, 'CRNCY')
                clean = dict()
                for i, key in enumerate(attributes):
                    if i:
                        try: # TO FLOAT
                            clean[key] = float(re.search(f'({key} = )(\d+.\d+)', clear).group(2))
                        except AttributeError: # JUMP OVER NO VOLUME DATA FOR INSTANCE IN CASE OF VIX Index
                            continue
                        except TypeError: # UNABLE TO CONVERT STRING TO FLOAT
                            clean[key] = None
                    else:
                        clean[key] = re.search('(CRNCY = ")(\w+)', clear).group(2)
            return clean
        except AttributeError:
            self.error = True
            return {"error": self.raw}

    def get_json(self):
        return {"security": self.ticker,
                    "timestamp": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
                    "data": self.clean}

    def get_dump(self):
        with open(self.path, "w") as outf:
            json.dump(self.get_json(), outf)

    @classmethod
    def from_dict(cls, **kwargs):
        attributes = "OPEN HIGH LOW PX_LAST VOLUME"
        if all(k in kwargs for k in ('kind', 'bloomberg_code', 'path')):
            error = False

        try:
            path = kwargs['path']
            is_historic = kwargs['kind'] == "historical"
            ticker = kwargs['bloomberg_code']
            if 'attributes' in kwargs.keys():
                attributes = kwargs['attributes']

        except KeyError:
            return cls(ticker='ERROR', error=True, path=path)

        if is_historic and not error:
            try:
                start_date = kwargs['start_date']
                end_date = kwargs['end_date']
            except KeyError:
                return cls(ticker='ERROR', error=True, path=path)

        if is_historic:
            return cls(ticker=ticker,
                       is_historic=is_historic,
                       attributes=attributes,
                       start_date=start_date.replace('-',''),
                       end_date=end_date.replace('-',''),
                       path=path,
                       error=error)

        if not is_historic and kwargs['kind'] == "real-time":
            return cls(ticker=ticker,
                       is_historic=is_historic,
                       attributes=attributes,
                       start_date=None,
                       end_date=None,
                       path=path,
                       error=error)
        else:
            return cls(ticker='ERROR', error=True, path=path)

class frequencyUpdater:
    def __init__(self, SLEEP_SECONDS_BASE, COUNTER_BASE):
        self.base_sleep = SLEEP_SECONDS_BASE
        self.base_counter = COUNTER_BASE

        self._sleep = self.base_sleep
        self.counter = self.base_counter

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


# if __name__ == "__main__":
#     bl = blbrg.from_dict(**{'path': 'TESZT1','kind':'historical', 'bloomberg_code': 'VIX Index', 'start_date':'2021-01-10', 'end_date':'2021-01-20'})
#     with open('TESZT1.txt', 'w') as fout_:
#         fout_.write(bl.raw)
#
#     bl = blbrg.from_dict(**{'path': 'TESZT2','kind':'historical', 'bloomberg_code': 'TSLA US Equity','start_date':'2021-01-10', 'end_date':'2021-01-18'})
#     with open('TESZT2.txt', 'w') as fout_:
#         fout_.write(bl.raw)

    # bl = blbrg.from_dict(**{'path': 'TESZT1','kind':'real-time', 'bloomberg_code': 'VIX Index'})
    # with open('TESZT1.txt', 'w') as fout_:
    #     fout_.write(bl.raw)
    #
    # bl = blbrg.from_dict(**{'path': 'TESZT2','kind':'real-time', 'bloomberg_code': 'TSLA US Equity'})
    # with open('TESZT2.txt', 'w') as fout_:
    #     fout_.write(bl.raw)