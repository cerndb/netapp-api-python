import sys
import os
from datetime import datetime

# FIXME: this is really, really, really ugly
parent_directory, _ = os.path.split(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.join(parent_directory, "./lib/NetApp/"))

from NaServer import NaServer, NaElement
import pytz

# HACK WARNING
# This is an override for allowing self-signed server certificates:
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


ONTAP_MAJORVERSION = 1
ONTAP_MINORVERSION = 0

DEFAULT_APP_NAME = "cern-monitoring-experiment"
LOCAL_TIMEZONE = "Europe/Zurich"

class Server():
    """
    The Server is a stateless, connectionless configuration container
    for a netapp monitoring system. There is no need for closing it. All
    API calls are made on event access etc.

    It implements a subset of the official NetApp API related to events.

    See docstrings for __init__ for mor information on instantiation.


    Examples:

    s = Server(...)

    # Return a specific event by its ID:
    event = s.events.by_id(13)

    # Return all events:
    for event in s.events:
        ...

    # Get all events after a specific one:
    for event in s.events.greater_than_id(13):
        ...
    """

    class EventLog():
        """
        An iterator over all the storage engine's events. Will (by
        default) iterate over _all_ events, but may optionally filter
        out events by ID.
        """

        def __iter__(self):
            raw_events = self.server._get_events()

            return (Event(ev) for ev in raw_events)

        def greater_than_id(self, id):
            """
            Return an Iterator over the current event log, but only
            containing events with ID:s higher than id.
            """

            api_call = NaElement("event-iter")
            api_call.child_add_string("greater-than-id", str(id))

            results = self.server.server.invoke_elem(api_call)

            if results.results_status() != 'passed':
                raise Exception("API Error: %s" % results.results_reason())
            else:
                raw_events = results.child_get('records').children_get()

            return (Event(ev) for ev in raw_events)


        def by_id(self, id):
            """
            Return a specific single event with a given ID. Raises an
            error if no such event exists.
            """

            pass

        def __init__(self, server):
            self.server = server


    @property
    def events(self):
        return Server.EventLog(self)

    def __init__(self, hostname, username, password, port=443,
                 transport_type="HTTPS", server_type="OCUM",
                 app_name=DEFAULT_APP_NAME):

        self.server = NaServer(hostname, ONTAP_MAJORVERSION,
                               ONTAP_MINORVERSION)

        self.server.set_server_type(server_type)

        self.server.set_transport_type(transport_type)
        self.server.set_port(port)

        # FIXME: this is a workaround. DO NOT LEAVE ON!
        self.server.set_server_cert_verification(False)

        self.server.set_application_name(app_name)

        ## FIXME: support other auth methods!
        self.server.set_style("LOGIN")
        self.server.set_admin_user(username, password)


    def _get_events(self):
        """
        Internal convenience wrapper function. Will return a list of raw
        child objects describing the entire event log on the server.

        Raises an Exception if the call failed. Good luck interpreting
        the error message -- it is most likely useless.
        """

        results = self.server.invoke('event-iter')

        if results.results_status() != 'passed':
            raise Exception("API Error: %s" % results.results_reason())
        else:
            raw_events = results.child_get('records').children_get()

            return raw_events

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
        self.timestamp = datetime.fromtimestamp(unix_timestamp_localtime,
                                                pytz.timezone(LOCAL_TIMEZONE))

    def __str__(self):
        datestring = "{:%c}"
        return "[%d] %s: [%s] %s (%s)" % (self.id, datestring.format(self.timestamp),
                                          self.severity, self.state, self.name)
