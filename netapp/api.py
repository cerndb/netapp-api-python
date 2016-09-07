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
    for event in s.events.filter(greater_than_id=13):
        ...
    """

    class EventLog():
        """
        An iterator over all the storage engine's events. Will (by
        default) iterate over _all_ events, but may optionally filter
        out events by ID.
        """

        def __iter__(self):

            # Applying an empty filter <==> get everything
            return self.filter()

        def by_id(self, id):
            """
            Return a specific single event with a given ID. Raises an
            error if no such event exists.
            """

            pass

        def filter(self, **kwargs):
            """
            Return an Iterator over items matching the specified filter
            queries, passed in as keyword arguments. Available
            filters/keywords are:

            :param severities: information, warning, error, critical
            :param states: NEW, OBSOLETE etc
            :param greater_than_id: any integer
            :param time_range: tuple of start, end timestamp in local time
              Unix timestamps. Timestamps are _inclusive_.
            :param max_records: paginate results with max_records as the
              maximum number of entries.
            """

            severities = kwargs.get('severities', None)
            states = kwargs.get('states', None)
            greater_than_id = kwargs.get('greater_than_id', None)
            time_range = kwargs.get('time_range', None)
            max_records = kwargs.get('max_records', None)

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

            return self.server._get_events(api_call)

        # Seems to be unsupported in the new API?
        # def filter_long_poll(self, timeout, **kwargs):
        #     """
        #     Like filter(), but long-poll for timeout seconds waiting for
        #     new data matching the query. time_interval is not allowed
        #     and will raise a ValueError.
        #     """

        #     if kwargs.has_key('time_interval'):
        #         raise ValueError("Having time interval constraints on a long "
        #                          "poll doesn't make sense!")

        def __init__(self, server):
            self.server = server


    @property
    def events(self):
        return Server.EventLog(self)

    def __init__(self, hostname, username, password, port=443,
                 transport_type="HTTPS", server_type="OCUM",
                 app_name=DEFAULT_APP_NAME):
        """
        Instantiate a new server connection. Proided details are:
        - hostname: the hostname of the server (or IP number)
        - transport_type: HTTP or HTTPS
        - server_type: only OCUM currently supported
        - app_name: the name of the calling app, as reported to the server
        """

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


    def _get_events(self, api_call):
        """
        Internal convenience wrapper function. Will return a generator
        of events corresponding to the provided query.

        Raises an Exception if the call failed. Good luck interpreting
        the error message -- it is most likely useless.
        """

        results = self.invoke_elem(api_call)

        if results.results_status() != 'passed':
            raise Exception("API Error: %s" % results.results_reason())
        else:
            raw_events = results.child_get('records').children_get()

            for ev in raw_events:
                yield Event(ev)

            # is there another page?
            if results.child_get_string('next-tag') is not None:
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

                # recur on next page
                for event in self._get_events(next_api_call):
                    yield event

    def invoke_elem(self, elem):
        """
        Convenience method: pass on a call to invoke_elem() to the server.
        """

        return self.server.invoke_elem(elem)

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
