[![Build Status](https://travis-ci.org/cerndb/netapp-api-python.svg?branch=master)](https://travis-ci.org/cerndb/netapp-api-python)
[![Coverage Status](https://coveralls.io/repos/github/cerndb/netapp-api-python/badge.svg?branch=master)](https://coveralls.io/github/cerndb/netapp-api-python?branch=master)

## About
This is a human-made Python implementation of relevant parts of NetApp's
ZAPI.

### Supported Systems

Currently, only DFM 6.3/6.4 and ONTAP is supported.

### Implemented Features

DFM:
- Events (just reading)

ONTAP:
- Volumes (reading)
- Snapshots (reading)

## Setup

1. `mkvirtualenv netapp-api-python`
2. `workon netapp-api-python`
3. `pip install -r requirements.txt`


## Testing

Set the environment variables to a host running NetApp OCUM
`NETAPP_HOST`, `NETAPP_USERNAME`, and `NETAPP_PASSWORD` and run
`pytest`. To test ONTAP set `ONTAP_HOST`, etc.

## Generating documentation

Documentation via Sphinx is available (more or less). You can generate
HTML documentation by going to /doc/ and entering `make html`.

The generated documentation is
[available on GitHub pages](https://cerndb.github.io/netapp-api-python/).

## License

Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
In applying this license, CERN does not waive the privileges and immunities
granted to it by virtue of its status as Intergovernmental Organization
or submit itself to any jurisdiction.

