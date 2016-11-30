# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as Intergovernmental Organization
# or submit itself to any jurisdiction.

"""
This is a Python implementation of NetApp's OCUM event logging API.

**Examples:**

Connect to a server::

    s = Server(hostname="netapp-1234", username="admin",
               password="admin123")

Return a specific event by its ID::

    event = s.events.single_by_id(13)

Return all events::

    for event in s.events:
            print(event)

Get all events after a specific one::

    for event in s.events.filter(greater_than_id=13):
            print(event)


Some parameters can be passed as a list::

    for event in s.events.filter(greater_than_id=13,
                                 severity=['critical', 'error']):
            print(event)

Events are fetched automatically in lazy increments if pagination is
enabled::

    for event in s.events.filter(max_records=4):
            print(event)
            # Will perform multiple queries under the hood
"""

from datetime import datetime
import logging

import netapp.vocabulary as V

import pytz
import requests
import lxml.etree

log = logging.getLogger(__name__)

ONTAP_MAJORVERSION = 1
ONTAP_MINORVERSION = 0
OCUM_API_URL = '/apis/XMLrequest'
XMLNS = 'http://www.netapp.com/filer/admin'
XMLNS_VERSION = "%d.%d" % (ONTAP_MAJORVERSION, ONTAP_MINORVERSION)

DEFAULT_APP_NAME = "netapp-ocum-events"
LOCAL_TIMEZONE = "Europe/Zurich"

"The default connection timeout, in seconds"
DEFAULT_TIMEOUT = 4


def _child_get_string(parent, string_name):
    """
    Helper function: search parent for its corresponding string value
    with a given key.
    """
    xpath_query = 'a:%s/text()' % string_name

    matches = parent.xpath(xpath_query,
                           namespaces={'a': XMLNS})

    assert len(matches) < 2, "Should only match at most one string value!"

    return matches[0] if matches else ""


def _child_get_dict(parent, string_name):
    """
    Helper function: search a parent for its corresponding key/value
    dictionary with a given key.

    Structure is:
    <string_name>
      <key-value-pair>
          <key>my-key</key>
          <value></value>
      </key-value-pair>
      ...
      <key-value-pair></key-value-pair>
    </string_name>
    """
    dataset = {}

    xpath_query = 'a:%s/*' % string_name
    children = parent.xpath(xpath_query,
                            namespaces={'a': XMLNS})

    log.debug("Begin parsing children of %s" % string_name)

    for child in children:
        key = _child_get_string(child, 'key')

        if len(child) == 1:
            # Reading the documentation, this should never happen.
            # Reading actual logs: this happens.
            log.debug("Key %s had no corresponding value!" % key)
            value = ""
        else:
            value = _child_get_string(child, 'value')

        log.debug("Saw {property} pair: {key}: {value}"
                  .format(property=string_name,
                          key=key,
                          value=value))

        dataset[key] = value

    return dataset


class Server(object):
    """
    The Server is a stateless, connectionless configuration container
    for a netapp monitoring system. There is no need for closing it, but
    if you want to, a close() function is available to terminate the
    connection. All API calls are made on event access etc.

    It implements a subset of the official NetApp API related to events.

    See docstrings for `__init__` for more information on instantiation.
    """

    class EventLog(object):
        """
        An iterator over all the storage engine's events. Will (by
        default) iterate over *all* events, but may optionally filter
        out events by ID.
        """

        def __iter__(self):

            # Applying an empty filter <==> get everything
            return self.filter()

        def single_by_id(self, id):
            """
            Return a specific single event with a given ID. Raises a
            KeyError if no such event exists.
            """

            api_call = V.event_iter(V.event_id(str(id)))

            for event in self.server._get_events(api_call):
                return event
            else:
                raise KeyError("No such ID!")

        def filter(self, **kwargs):
            """
            Return an Iterator over items matching the specified filter
            queries, passed in as keyword arguments. Available
            filters/keywords are:

            :param severities: information, warning, error,
              critical. Raises an Exception if none of these. Options are
              case-insensetive strings.
            :param states: NEW, OBSOLETE etc
            :param greater_than_id: only show events with ID:s greater
              than (e.g. logged after) the given one.
            :param time_range: tuple of start, end timestamp in local time
              Unix timestamps. Timestamps are *inclusive*.
            :param max_records: paginate results with max_records as the
              maximum number of entries. Will make several queries!
            :param source: Lists events against the specified source
              resource key. If the provided resource key specifies a
              group, lists events against all members in that group.

            :param timout: Timeout in seconds, after which the query
             will return an empty results if nothing was found. Defaults
             to 0 if not provided, or if a time interval was provided.
            """

            severities = kwargs.get('severities', None)
            states = kwargs.get('states', None)
            greater_than_id = kwargs.get('greater_than_id', None)
            time_range = kwargs.get('time_range', None)
            max_records = kwargs.get('max_records', None)
            timeout = kwargs.get('timeout', 0)
            source = kwargs.get('source', None)

            api_call = V.event_iter()

            if greater_than_id is not None:
                api_call.append(V.greater_than_id(str(greater_than_id)))

            if time_range is not None:
                start_time, end_time = time_range

                api_call.append(
                    V.time_range(
                        V.event_timestamp_range(
                            V.end_time(end_time),
                            V.start_time(start_time))))

            if states is not None:
                event_states = map(V.event_state, states)
                api_call.append(V.event_state_filter_list(*event_states))

            if severities is not None:
                obj_statuses = map(V.obj_status, severities)
                api_call.append(V.event_severities(*obj_statuses))

            if max_records is not None:
                api_call.append(V.max_records(str(max_records)))

            if source is not None:
                api_call.append(V.source(str(source)))

            api_call.append(V.timeout(str(timeout)))

            return self.server._get_events(api_call)

        def __init__(self, server):
            self.server = server

    @property
    def events(self):
        """
        A gettable-only property representing all events known to the
        server. Performs eager HTTP fetch on every read. Corresponds to
        an empty filter call.
        """
        return Server.EventLog(self)

    def __init__(self, hostname, username, password, port=443,
                 transport_type="HTTPS", server_type="OCUM",
                 app_name=DEFAULT_APP_NAME,
                 timeout_s=DEFAULT_TIMEOUT):
        """
        Instantiate a new server connection. Provided details are:

        :param hostname: the hostname of the server (or IP number)
        :param transport_type: HTTP or HTTPS (default is HTTPS)
        :param server_type: only OCUM currently supported
        :param app_name: the name of the calling app, as reported to the
          server
        :param timeout_s: The timeout in seconds for each connection to
          a NetApp filer. Passed as-is to Requests.
        """

        self.hostname = hostname
        self.auth_tuple = (username, password)
        self.api_url = "https://%s:%d%s" % (hostname, port, OCUM_API_URL)
        self.app_name = app_name
        self.session = requests.Session()
        self.timeout_s = timeout_s

    def close(self):
        """
        Close any open connection to the server. The Server object
        becomes unusable after this.
        """

        self.session.close()

    def _get_events(self, api_call):
        """
        Internal convenience wrapper function. Will return a generator
        of events corresponding to the provided query. May make several
        queries if the results were paginated.

        Raises an Exception if the call failed. Good luck interpreting
        the error message -- it is most likely useless.
        """

        page_left_to_process = True

        while page_left_to_process:

            next_tag, raw_events = self.perform_call(api_call)

            for ev in raw_events:
                yield Event(ev)

            # is there another page?
            if next_tag is None:
                break
            else:
                next_api_call = api_call

                # According to the specification, we need to preserve
                # all options, but we also need to replace any previous
                # occurrences of 'tag'.

                tag_element = next_api_call.find('tag')

                if tag_element is not None:
                    next_api_call.remove(tag_element)

                next_api_call.append(V.tag(next_tag))

                api_call = next_api_call

    def perform_call(self, api_call):
        """
        Perform an API call as represented by the provided XML data,
        returning a tuple of next_tag, records, where next_tag is None
        if there were no further pages.

        Raises an APIError on erroneous API calls.
        """

        query_root = V.netapp(api_call,
                              xmlns=XMLNS,
                              version=XMLNS_VERSION,
                              nmsdk_app=self.app_name)

        request = lxml.etree.tostring(query_root, xml_declaration=True,
                                      encoding="UTF-8")


        log.debug("Performing request: %s" % request)

        r = self.session.post(self.api_url, verify=False, auth=self.auth_tuple,
                              data=request,
                              headers={'Content-type': 'application/xml'},
                              timeout=self.timeout_s)

        # FIXME: prettify this handling
        r.raise_for_status()

        log.debug("Response code: %s:\n" % r.status_code)
        log.debug("XML Response: %s: " %
                  lxml.etree.tostring(lxml.etree.fromstring(r.content),
                                      pretty_print=True,
                                      encoding="UTF-8"))

        # If we got here, the request was OK. Now for verifying the
        # status...
        response = lxml.etree.fromstring(r.content)

        status = response.xpath('/a:netapp/a:results/@status',
                                namespaces={'a': XMLNS})[0]

        if status != 'passed':
            reason = response.xpath('/a:netapp/a:results/@reason',
                                    namespaces={'a': XMLNS})[0]

            errno = response.xpath('/a:netapp/a:results/@errno',
                                   namespaces={'a': XMLNS})[0]

            raise APIError(message=reason, errno=errno,
                           failing_query=request)
        else:
            num_records = int(response.xpath(('/a:netapp/a:results/'
                                              'a:num-records/text()'),
                                             namespaces={'a': XMLNS})[0])

            records = response.xpath('/a:netapp/a:results/a:records/*',
                                     namespaces={'a': XMLNS})

            assert num_records == len(records)

            next_tag = None

            potential_next_tag = response.xpath(('/a:netapp/a:results/'
                                                 'a:next-tag/text()'),
                                                namespaces={'a': XMLNS})
            if potential_next_tag:
                next_tag = potential_next_tag[0]

            return next_tag, records


class Event(object):
    """
    A nicer representation of a logging event. Should only be
    instantiated by the API functions (don't roll your own!).

    """

    about = None
    "A string describing the event further."
    category = None
    "The category of the event"
    condition = None
    "What condition the event is in"
    id = None
    "The internal ID of the event as given by the logger"
    impact_area = None
    "The event's impact area"
    impact_level = None
    "The event's impact level"
    name = None
    "The event's canonical name"
    severity = None
    "The severity of the event: warning, information, critical, or error"
    source_name = None
    "The name of the resource that produced the event"
    source_resource_key = None
    "The key of the resource that produced the event"
    source_type = None
    "The type of source that produced the event"
    state = None
    "The current state of the event: NEW, OBSOLETE etc"
    event_type = None
    "The type of the event"

    datetime = None
    """A timezone-aware datetime object describing the same date as
    ``timestamp``"""
    timestamp = None
    "The UNIX timestamp the event was reported (as reported by the API)"

    arguments = {}
    "A dictionary representing key-value arguments. May vary between events."

    def __init__(self, raw_event):

        # FIXME: extract event-arguments as well, if relevant
        self.about = _child_get_string(raw_event, 'event-about')
        self.category = _child_get_string(raw_event, 'event-category')
        self.condition = _child_get_string(raw_event, 'event-condition')
        self.id = int(_child_get_string(raw_event, 'event-id'))
        self.impact_area = _child_get_string(raw_event, 'event-impact-area')
        self.impact_level = _child_get_string(raw_event, 'event-impact-level')
        self.name = _child_get_string(raw_event, 'event-name')
        self.severity = _child_get_string(raw_event, 'event-severity')
        self.source_name = _child_get_string(raw_event, 'event-source-name')
        self.source_resource_key = _child_get_string(
            raw_event,
            'event-source-resource-key')
        self.source_type = _child_get_string(raw_event, 'event-source-type')
        self.state = _child_get_string(raw_event, 'event-state')
        self.event_type = _child_get_string(raw_event, 'event-type')

        unix_timestamp_localtime = int(_child_get_string(raw_event,
                                                         'event-time'))
        self.datetime = datetime.fromtimestamp(unix_timestamp_localtime,
                                               pytz.timezone(LOCAL_TIMEZONE))
        self.timestamp = unix_timestamp_localtime

        self.arguments = _child_get_dict(raw_event, 'event-arguments')

    def __str__(self):
        datestring = "{:%c}"
        return "[%d] %s: [%s] %s (%s)" % (self.id,
                                          datestring.format(self.datetime),
                                          self.severity, self.state, self.name)

    def __eq__(self, other):
        return(isinstance(other, self.__class__)
               and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)


class APIError(Exception):
    """
    An Exception logging an api-related error and its context. Note that
    an APIError typically occurs *after* a successful transfer of a
    command to the API itself.

    Noteworthy properties are errno (error number), msg (error message)
    and failing_query (the XML query that was processed as the error
    occurred, if available).
    """
    def __init__(self, message="", errno=None, failing_query=None):
        self.msg = message
        self.errno = errno
        self.failing_query = failing_query

    def __str__(self):
        str = "API Error %s: %s. Offending query: \n %s" % (self.errno,
                                                            self.msg,
                                                            self.failing_query)

        return str
