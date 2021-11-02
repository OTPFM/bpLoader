# bpLoader
Price Data Server 

run.py working as a server: checking request/ directory for .json request files. Building the response to response/ directory with the same name as the original request.

Solution guarantees singleton instance of the server on windows and parallel downloading capability.

Request json files should look like:

Prompt data:

```{"kind": "real-time", "attributes": "EQY_SPLIT_DT", "bloomberg_code": "NVDA US Equity"}```

Historical data:

```{"kind": "historical", "attributes": "PX_LAST", "start_date": "2020-02-04", "end_date": "2021-01-29", "bloomberg_code": "QQQ US Equity"}```

Prompt data with several FLD (attribute):

```{"kind": "real-time", "attributes": "EQY_SPLIT_ADJUSTMENT_FACTOR SPLIT_DATE_REALTIME", "bloomberg_code": "GOOG US Equity"}```

Each ticker should be in a separate json file to maximize async functionality.
