import sys
import os
from datetime import datetime
import StringIO

import vocabulary

# FIXME: this is really, really, really ugly
#parent_directory, _ = os.path.split(os.path.dirname(os.path.realpath(__file__)))
#sys.path.append(os.path.join(parent_directory, "./lib/NetApp/"))

#from NaServer import NaServer, NaElement
import pytz
import requests
import lxml.etree

_DEBUG = False

ONTAP_MAJORVERSION = 1
ONTAP_MINORVERSION = 0
OCUM_API_URL = '/apis/XMLrequest'
XMLNS = 'http://www.netapp.com/filer/admin'
XMLNS_VERSION = "%d.%d" % (ONTAP_MAJORVERSION, ONTAP_MINORVERSION)

DEFAULT_APP_NAME = "netapp-ocum-events"
LOCAL_TIMEZONE = "Europe/Zurich"

class Server():
    """
    The Server is a stateless, connectionless configuration container
    for a netapp monitoring system. There is no need for closing it. All
    API calls are made on event access etc.

    It implements a subset of the official NetApp API related to events.

    See docstrings for `__init__` for more information on instantiation.

    **Examples:**

    Start a server::

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

    class EventLog():
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

            api_call = NaElement('event-iter')

            api_call.child_add_string('event-id', id)

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

            api_call = NaElement('event-iter')

            if greater_than_id is not None:
                api_call.child_add_string("greater-than-id", str(greater_than_id))

            if time_range is not None:
                start_time, end_time = time_range
                interval = NaElement('event-timestamp-range')
                interval.child_add_string('start-time', start_time)
                interval.child_add_string('end-time', end_time)

                wrapper = NaElement('time-range')
                wrapper.child_add(interval)

                api_call.child_add(wrapper)

            if states is not None:
                event_state_filter_list = NaElement("event-state-filter-list")

                for state in states:
                    event_state = NaElement('event-state', state)
                    event_state_filter_list.child_add(event_state)

                api_call.child_add(event_state_filter_list)

            if severities is not None:
                event_severities = NaElement("event-severities")

                for severity in severities:
                    obj_status = NaElement('obj_status', severity)
                    event_severities.child_add(obj_status)

                api_call.child_add(event_severities)

            if max_records is not None:
                api_call.child_add_string("max-records", max_records)

            api_call.child_add_string('timeout', timeout)

            return self.server._get_events(api_call)

        def __init__(self, server):
            self.server = server


    @property
    def events(self):
        return Server.EventLog(self)

    def __init__(self, hostname, username, password, port=443,
                 transport_type="HTTPS", server_type="OCUM",
                 app_name=DEFAULT_APP_NAME):
        """
        Instantiate a new server connection. Provided details are:

        :param hostname: the hostname of the server (or IP number)
        :param transport_type: HTTP or HTTPS (default is HTTPS)
        :param server_type: only OCUM currently supported
        :param app_name: the name of the calling app, as reported to the
          server
        """

        self.hostname = hostname
        self.auth_tuple = (username, password)
        self.api_url = "https://%s:%d%s" % (hostname, port, OCUM_API_URL)
        self.app_name = app_name

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

            results = self.invoke_elem(api_call)

            if results.results_status() != 'passed':
                raise Exception("API Error: %s" % results.results_reason())
            else:
                raw_events = results.child_get('records').children_get()

                for ev in raw_events:
                    yield Event(ev)

            # is there another page?
            if results.child_get_string('next-tag') is None:
                break
            else:
                next_api_call = NaElement('event-iter')

                next_tag = results.child_get_string('next-tag')

                # According to the specification, we need to preserve
                # all options, but we also need to replace any previous
                # occurrences of 'tag'. However, there is no method for
                # replacing/overwriting children!
                #
                # The work-around is to simply copy everything but the
                # tag-related data to a new query and recur on it.
                for child in api_call.children_get():

                    # Probably an unsupported operation:
                    child_name = child.element['name']

                    if child_name not in ['tag', 'next-tag']:
                        next_api_call.child_add(child)


                # Finally, add a tag to inform the API of where we were:
                next_api_call.child_add_string('tag', next_tag)
                api_call = next_api_call

    def invoke_elem(self, elem):
        """
        Internal convenience method: pass on a call to `invoke_elem()`
        to the server.
        """

        return self.server.invoke_elem(elem)

    def perform_call(self, api_call):
        """
        """

        query_root = vocabulary.netapp(api_call,
                                       xmlns=XMLNS,
                                       version=XMLNS_VERSION,
                                       nmsdk_app=self.app_name)

        request = lxml.etree.tostring(query_root, xml_declaration=True,
                                      encoding="UTF-8")

        if _DEBUG:
            print("Performing request:")
            print(request)

        r = requests.post(self.api_url, verify=False, auth=self.auth_tuple,
                          data=request,
                          headers={'Content-type': 'application/xml'})

        # FIXME: prettify this handling
        r.raise_for_status()

        if _DEBUG:
                print("\nResponse code: %s:\n" % r.status_code)
                print(lxml.etree.tostring(lxml.etree.fromstring(r.content),
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
            num_records = int(response.xpath('/a:netapp/a:results/a:num-records/text()',
                                         namespaces={'a': XMLNS})[0])

            records = response.xpath('/a:netapp/a:results/a:records/*',
                                   namespaces={'a': XMLNS})

            assert num_records == len(records)

            return records

class Event():
    """
    A nicer representation of a logging event. Should only be
    instantiated by the API functions (don't roll your own!).


    """

    def __init__(self, raw_event):

        ## FIXME: extract event-arguments as well, if relevant
        self.about = raw_event.child_get_string('event-about')
        self.category = raw_event.child_get_string('event-category')
        self.condition = raw_event.child_get_string('event-condition')
        self.id = raw_event.child_get_int('event-id')
        self.impact_area = raw_event.child_get_string('event-impact-area')
        self.impact_level = raw_event.child_get_string('event-impact-level')
        self.name = raw_event.child_get_string('event-name')
        self.severity = raw_event.child_get_string('event-severity')
        self.source_name = raw_event.child_get_string('event-source-name')
        self.source_resource_key = raw_event.child_get_string('event-source-resource-key')
        self.source_type = raw_event.child_get_string('event-source-type')
        self.state = raw_event.child_get_string('event-state')
        self.event_type = raw_event.child_get_string('event-type')

        unix_timestamp_localtime = raw_event.child_get_int('event-time')
        self.datetime = datetime.fromtimestamp(unix_timestamp_localtime,
                                               pytz.timezone(LOCAL_TIMEZONE))
        self.timestamp = unix_timestamp_localtime

    def __str__(self):
        datestring = "{:%c}"
        return "[%d] %s: [%s] %s (%s)" % (self.id, datestring.format(self.datetime),
                                          self.severity, self.state, self.name)


class APIError(Exception):
    def __init__(self, message="", errno=None, failing_query=None):
        self.msg = message
        self.errno = errno
        self.failing_query = failing_query


    def __str__(self):
        str = "API Error %s: %s. Offending query: \n %s" % (self.errno,
                                                            self.msg,
                                                            self.failing_query)

        return str
