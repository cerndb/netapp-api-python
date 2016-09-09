## Setup

1. `mkvirtualenv netapp-ocum-events`
2. `workon netapp-ocum-events`
3. `pip install -r requirements.txt`


## Testing

Set the environment variables `NETAPP_HOST`, `NETAPP_USERNAME`, and
`NETAPP_PASSWORD` and run `pytest`.

## Generating documentation

Documentation via Sphinx is available (more or less). You can generate
HTML documentation by going to /doc/ and entering `make html`.
