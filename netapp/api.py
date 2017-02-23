# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as Intergovernmental Organization
# or submit itself to any jurisdiction.
from __future__ import print_function
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
from collections import namedtuple

import netapp.vocabulary as V

import pytz
import requests
import lxml.etree
import lxml.builder
import six

X = lxml.builder.ElementMaker()

log = logging.getLogger(__name__)

ONTAP_MAJORVERSION = 1
ONTAP_MINORVERSION = 0
OCUM_API_URL = '/apis/XMLrequest'
ONTAP_API_URL = '/servlets/netapp.servlets.admin.XMLrequest_filer'
XMLNS = 'http://www.netapp.com/filer/admin'
XMLNS_VERSION = "%d.%d" % (ONTAP_MAJORVERSION, ONTAP_MINORVERSION)

DEFAULT_APP_NAME = "netapp-api-python"
LOCAL_TIMEZONE = "Europe/Zurich"

"The default connection timeout, in seconds"
DEFAULT_TIMEOUT = 4

"Only consider these data fields for volumes"
VOL_FIELDS = [X('volume-id-attributes',
                *[X(x) for x in
                  ['name', 'uuid', 'junction-path',
                   'containing-aggregate-name',
                   'node']]),
              X('volume-space-attributes',
                *[X(x) for x in ['size-total', 'size-used']]),
              X('volume-autosize-attributes',
                *[X(x) for x in ['is-enabled', 'maximum-size',
                                 'increment-size']]),
              X('volume-state-attributes', X('state')),
              X('volume-export-attributes', X('policy'))]


def _read_bool(s):
    """
    Helper function to read a Boolean value from NetApp's XML data.
    """
    if s == "true":
        return True
    elif s == "false":
        return False
    else:
        raise ValueError(s)


def _int_or_none(s):
    """
    Parse a string into an integer, returning None if the string was empty.

    Raises ValueError otherwise.
    """

    return int(s) if s else None


def _child_get_string(parent, *string_hierarchy):
    """
    Helper function: search parent for its corresponding string value
    with a given key.

    You can search in a hierarchy by giving multiple tags in the right
    order, e.g::

        _child_get_string(parent, "volume-id-attributes", "uuid")
    """
    string_name = "/a:".join(string_hierarchy)

    xpath_query = 'a:%s/text()' % string_name

    matches = parent.xpath(xpath_query,
                           namespaces={'a': XMLNS})

    assert len(matches) < 2, "Should only match at most one string value!"

    return matches[0] if matches else ""


def _child_get_kv_dict(parent, string_name):
    """
    Helper function: search a parent for its corresponding key/value
    dictionary with a given key.

    Structure is:
    <string_name>
      <key-value-pair>
          <key>my-key</key>
          <value>17</value>
      </key-value-pair>
      ...
      <key-value-pair></key-value-pair>
    </string_name>

    which will return: {'key': 17}
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

            for event in self.server._get_paginated(api_call,
                                                    endpoint='OCUM',
                                                    constructor=Event,
                                                    container_tag="records"):
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
                event_states = list(map(V.event_state, states))
                api_call.append(V.event_state_filter_list(*event_states))

            if severities is not None:
                obj_statuses = list(map(V.obj_status, severities))
                api_call.append(V.event_severities(*obj_statuses))

            if max_records is not None:
                api_call.append(V.max_records(str(max_records)))

            if source is not None:
                api_call.append(V.source(str(source)))

            api_call.append(V.timeout(str(timeout)))

            return self.server._get_paginated(api_call, endpoint='OCUM',
                                              constructor=Event,
                                              container_tag="records")

        def __init__(self, server):
            self.server = server

    class VolumeList(object):

        def __init__(self, server):
            self.server = server

        def __iter__(self):
            # Applying an empty filter <==> get everything
            return self.filter()

        def filter(self, max_records=None, **query):
            """
            Usage:
               filter(name='my-volume')

            An empty filter matches every volume.

            It is only possible to match on volume-id-attributes, most
            notably 'uuid', 'junction_path' and 'name'. Underscores will
            be converted to hyphens.

            max_records is (analogous to the same option for
            events.filter) the maximum number of entries to return in
            each fetch operation. Pagination and incremental fetch will
            be done automatically under the hood.
            """

            api_call = X('volume-get-iter',
                         X('desired-attributes',
                           X('volume-attributes',
                               *VOL_FIELDS)))

            if max_records is not None:
                api_call.append(V.max_records(str(max_records)))

            if query:
                attributes = []
                for attribute, value in six.iteritems(query):
                    attributes.append(X(attribute.replace('_', '-'), value))

                api_call.append(X('query',
                                  X('volume-attributes',
                                    X('volume-id-attributes',
                                      *attributes))))

            return self.server._get_paginated(
                api_call,
                endpoint='ONTAP',
                constructor=Volume,
                container_tag="attributes-list")

    class ExportPolicy(object):
        """
        A wrapper object for an export policy with the sole purpose of
        providing lazy access to rules.

        Very internal.
        """
        name = None

        def __init__(self, name, server):
            self.name = name
            self.server = server

        @property
        def rules(self):
            return self.server.export_rules_of(self.name)

    @property
    def events(self):
        """
        A gettable-only property representing all events known to the
        server. Performs eager HTTP fetch on every read. Corresponds to
        an empty filter call.
        """
        return Server.EventLog(self)

    @property
    def volumes(self):
        """
        A gettable-only porperty representing all the volumes on the filer.
        """
        return Server.VolumeList(self)

    def snapshots_of(self, volume_name):
        """
        A generator over names of snapshots of the given volume.

        Might return no elements if there are no snapshots.
        """
        api_call = X('snapshot-get-iter',
                     X('desired-attributes',
                       X('snapshot-info',
                         X('name'))),
                     X('query',
                       X('snapshot-info',
                         X('volume', str(volume_name)))))

        def unpack_name(snapshot_info):
            return _child_get_string(snapshot_info, 'name')

        return self._get_paginated(api_call,
                                   endpoint='ONTAP',
                                   constructor=unpack_name,
                                   container_tag='attributes-list')

    @property
    def export_policies(self):
        """
        Read-only-property: return a list of registered export policy names.
        """
        api_call = X('export-policy-get-iter',
                     X('desired-attributes',
                       X('export-rule-info',
                         X('policy-name'))))

        def unpack_policy(export_rule_info):
            name = _child_get_string(export_rule_info, 'policy-name')
            return Server.ExportPolicy(name=name, server=self)

        return self._get_paginated(api_call,
                                   endpoint='ONTAP',
                                   constructor=unpack_policy,
                                   container_tag='attributes-list')

    def export_rules_of(self, policy_name):
        """
        Return the rules of the policy as a list. Note that order
        matters here!

        Access to the property is lazy, but the list of rules is
        materialised immediately.
        """
        api_call = X('export-rule-get-iter',
                     X('desired-attributes',
                       X('export-rule-info',
                         X('rule-index'),
                         X('client-match'))),
                     X('query',
                       X('export-rule-info',
                         X('policy-name', policy_name))))

        def unpack_rule(export_rule_info):
            index = _child_get_string(export_rule_info, 'rule-index')
            rule = _child_get_string(export_rule_info, 'client-match')
            return index, rule

        results = self._get_paginated(
            api_call,
            endpoint='ONTAP',
            constructor=unpack_rule,
            container_tag='attributes-list')

        return [rule for _index, rule in
                sorted(results, key=lambda x: x[0])]

    def locks_on(self, volume_name):
        """
        Return a list of locks (possibly empty) held on the volume
        volume_name.
        """

        api_call = X('lock-get-iter',
                     X('desired-attributes',
                       X('lock-info',
                         X('volume'),
                         X('lock-state'),
                         X('client-address'))),
                     X('query',
                       X('lock-info',
                         X('volume', volume_name))))

        def unpack_lock(lock_info):
            return Lock(
                volume=_child_get_string(lock_info, 'volume'),
                state=_child_get_string(lock_info, 'lock-state'),
                client_address=_child_get_string(lock_info, 'client-address'))

        return self._get_paginated(api_call,
                                   endpoint='ONTAP',
                                   constructor=unpack_lock,
                                   container_tag='attributes-list')

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
        self.ocum_api_url = "https://%s:%d%s" % (hostname, port, OCUM_API_URL)
        self.ontap_api_url = "https://%s:%d%s" % (hostname,
                                                  port, ONTAP_API_URL)
        self.app_name = app_name
        self.session = requests.Session()
        self.timeout_s = timeout_s

    def close(self):
        """
        Close any open connection to the server. The Server object
        becomes unusable after this.
        """

        self.session.close()

    def _get_paginated(self, api_call, endpoint, constructor,
                       container_tag="records"):
        """
        Internal convenience wrapper function. Will return a generator
        of objects corresponding to the provided query, as constructed
        by the given constructor. May make several queries if the
        results were paginated.

        Raises an APIError if the call failed. Good luck interpreting
        the error message -- it is most likely useless.
        """

        page_left_to_process = True

        if endpoint == 'OCUM':
            api_url = self.ocum_api_url
        elif endpoint == 'ONTAP':
            api_url = self.ontap_api_url
        else:
            raise ValueError(endpoint)

        while page_left_to_process:

            next_tag, raw_events = self.perform_call(api_call, api_url,
                                                     container_tag)

            for ev in raw_events:
                yield constructor(ev)

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

    def perform_call(self, api_call, api_url, container_tag):
        """
        Perform an API call as represented by the provided XML data,
        returning a tuple of next_tag, records, where next_tag is None
        if there were no further pages.

        It is assumed that the response will be a list of entries in
        container_tag::

        <container_tag>
            <data-entry></data-entry>
            <data-entry></data-entry>
        </container_tag>

        Raises an APIError on erroneous API calls.
        """

        query_root = V.netapp(api_call,
                              xmlns=XMLNS,
                              version=XMLNS_VERSION,
                              nmsdk_app=self.app_name)

        request = lxml.etree.tostring(query_root, xml_declaration=True,
                                      encoding="UTF-8")

        log.debug("Performing request: %s" % request)

        r = self.session.post(api_url, verify=False, auth=self.auth_tuple,
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

            records = response.xpath('/a:netapp/a:results/a:{}/*'
                                     .format(container_tag),
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

        self.arguments = _child_get_kv_dict(raw_event, 'event-arguments')

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


class Volume(object):
    """
    A volume in the NetApp storage system.

    Do not roll your own.
    """

    def __init__(self, raw_object):
        self.uuid = _child_get_string(raw_object,
                                      'volume-id-attributes',
                                      'uuid')
        self.name = _child_get_string(raw_object,
                                      'volume-id-attributes',
                                      'name')
        self.active_policy_name = _child_get_string(raw_object,
                                                    'volume-export-attributes',
                                                    'policy')
        self.size_total_bytes = int(_child_get_string(
            raw_object,
            'volume-space-attributes',
            'size-total'))
        self.size_used_bytes = int(_child_get_string(
            raw_object,
            'volume-space-attributes',
            'size-used'))
        self.state = _child_get_string(raw_object,
                                       'volume-state-attributes',
                                       'state')
        self.junction_path = _child_get_string(raw_object,
                                               'volume-id-attributes',
                                               'junction-path')
        self.containing_aggregate_name = _child_get_string(
            raw_object,
            'volume-id-attributes',
            'containing-aggregate-name')
        self.node_name = _child_get_string(raw_object,
                                           'volume-id-attributes',
                                           'node')

        self.autosize_enabled = _read_bool(
            _child_get_string(raw_object,
                              'volume-autosize-attributes',
                              'is-enabled'))

        self.autosize_increment = _int_or_none(_child_get_string(
            raw_object,
            'volume-autosize-attributes',
            'increment-size'))
        self.max_autosize = _int_or_none(_child_get_string(
            raw_object,
            'volume-autosize-attributes',
            'maximum-size'))

    def __str__(self):
        return "<Volume name={}>".format(self.name)

    def __eq__(self, other):
        return(isinstance(other, self.__class__)
               and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)


Lock = namedtuple('Lock', 'volume, state, client_address')


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
