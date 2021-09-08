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
from contextlib import contextmanager

import netapp.vocabulary as V

import pytz
import requests
import lxml.etree
import lxml.builder
import six

X = lxml.builder.ElementMaker()

log = logging.getLogger(__name__)

ONTAP_MAJORVERSION = 1
ONTAP_MINORVERSION = 1.180
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
                   'node', 'owning-vserver-name',
                   'creation-time']]),
              X('volume-space-attributes',
                *[X(x) for x in ['size-total', 'size-used',
                                 'percentage-snapshot-reserve',
                                 'percentage-snapshot-reserve-used']]),
              X('volume-autosize-attributes',
                *[X(x) for x in ['is-enabled', 'maximum-size']]),
              X('volume-state-attributes', X('state')),
              X('volume-export-attributes', X('policy')),
              X('volume-hybrid-cache-attributes', X('caching-policy'))]


def _read_bool(s):
    """
    Helper function to read a Boolean value from NetApp's XML data.
    """
    if s == "true":
        return True
    else:
        return False


def _int_or_none(s):
    """
    Parse a string into an integer, returning None if the string was empty.

    Raises ValueError otherwise.
    """

    return int(s) if s else None


def _child_get_int(parent, *string_hierarchy):
    """
    Perform the same task as _child_get_string, but return an Integer.
    """

    try:
        return int(_child_get_string(parent, *string_hierarchy))
    except (ValueError, TypeError):
        return None


def _child_get_strings(parent, *string_hierarchy):
    """
    Helper function. In a situation where we have::

       <shoes>
          <size>
            <european>42</european>
          </size>
        </shoes>

    Return the strings contained within as a list with::

        _child_get_strings(shoes, 'size', 'european')
        => ["42"]
    """
    string_name = "/a:".join(string_hierarchy)

    query = 'a:%s' % string_name
    matches = parent.findall(query,
                             namespaces={'a': XMLNS})
    return [m.text for m in matches]


def _child_get_string(parent, *string_hierarchy):
    """
    Helper function: search parent for its corresponding string value
    with a given key.

    You can search in a hierarchy by giving multiple tags in the right
    order, e.g::

        _child_get_string(parent, "volume-id-attributes", "uuid")

    This function strictly assumes either 1 or 0 matches (empty string)
    """
    target = "/a:".join(string_hierarchy)
    return parent.findtext("a:{}".format(target),
                           namespaces={'a': XMLNS})


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

        def single(self, volume_name, vserver=None):
            """
            Return a single volume, raising a IndexError if no volume matched.
            """
            if vserver:
                volumes = list(self.filter(name=volume_name, vserver=vserver))
            else:
                volumes = list(self.filter(name=volume_name))
            return volumes[0]

        def make_volume(self, attributes_list):
            name = _child_get_string(attributes_list,
                                     'volume-id-attributes',
                                     'name')
            vserver = _child_get_string(attributes_list,
                                        'volume-id-attributes',
                                        'owning-vserver-name')

            sis_path = "/vol/{}".format(name)
            sis_api_call = X('sis-get-iter',
                             X('desired-attributes',
                               X('sis-status-info',
                                 X('is-compression-enabled'),
                                 X('is-inline-compression-enabled'))),
                             X('query',
                               X('sis-status-info',
                                 X('path', sis_path),
                                 X('vserver', vserver))))

            def extract_compression(attributes_list):
                compression_enabled = _read_bool(_child_get_string(
                    attributes_list,
                    'is-compression-enabled'))

                inline_enabled = _read_bool(_child_get_string(
                    attributes_list,
                    'is-inline-compression-enabled'))

                return compression_enabled, inline_enabled

            result = self.server._get_paginated(
                api_call=sis_api_call,
                endpoint='ONTAP',
                constructor=extract_compression,
                container_tag='attributes-list')

            try:
                compression, inline = next(result)
            except StopIteration:
                # If we didn't get any hits, it meant the entire SIS
                # subsystem was disabled, and thus compression *cannot*
                # be enabled.
                #
                # This is not documented anywhere.
                log.info("SIS was disabled for volume {}. Compression = off"
                         .format(name))
                compression, inline = (False, False)

            return Volume(attributes_list, compression=compression,
                          inline=inline)

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
                constructor=self.make_volume,
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
                         X('name'),
                         X('access-time'),
                         X('total'))),
                     X('query',
                       X('snapshot-info',
                         X('volume', str(volume_name)))))

        def unpack_snapshot(snapshot_info):
            return Snapshot(
                name=_child_get_string(snapshot_info, 'name'),
                creation_time=datetime.fromtimestamp(
                    _child_get_int(snapshot_info, 'access-time'),
                    pytz.timezone(LOCAL_TIMEZONE)),
                size_kbytes=_child_get_int(snapshot_info, 'total'))

        return self._get_paginated(api_call,
                                   endpoint='ONTAP',
                                   constructor=unpack_snapshot,
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
        Return the rules of the policy as a list of index, rule
        tuples. Note that order matters here!

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
            index = _child_get_int(export_rule_info, 'rule-index')
            rule = _child_get_string(export_rule_info, 'client-match')
            return index, rule

        results = self._get_paginated(
            api_call,
            endpoint='ONTAP',
            constructor=unpack_rule,
            container_tag='attributes-list')

        return sorted(results, key=lambda x: x[0])

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

    def create_volume(self, name, size_bytes, aggregate_name,
                      junction_path, export_policy_name=None,
                      percentage_snapshot_reserve=0,
                      compression=True,
                      inline_compression=True,
                      caching_policy=None):
        """
        Create a new volume on the NetApp cluster

        size_bytes is assumed to be in bytes, but if given as a string
        with a suffix ("k", "m", "g" or "t" for kilobytes, megabytes,
        gigabytes and terabytes respectively), use that suffix.
        """
        api_call = X('volume-create',
                     X('volume', name),
                     X('containing-aggr-name', aggregate_name),
                     X('size', str(size_bytes)),
                     X('junction-path', junction_path),
                     X('percentage-snapshot-reserve',
                       str(percentage_snapshot_reserve)))

        if export_policy_name:
            api_call.append(X('export-policy', export_policy_name))

        if caching_policy:
            api_call.append(X('caching-policy', caching_policy))

        self.perform_call(api_call, self.ontap_api_url)

        self.set_compression(volume_name=name, enabled=compression,
                             inline=inline_compression)

    def create_snapshot(self, volume_name, snapshot_name):
        """
        Create a new snapshot (if possible).

        Raises APIError if there is no more room to create snapshots.
        """
        api_call = X('snapshot-create',
                     X('snapshot', snapshot_name),
                     X('volume', volume_name))
        self.perform_call(api_call, self.ontap_api_url)

    def unmount_volume(self, volume_name):
        """
        Unmount a volume
        """
        self.perform_call(X('volume-unmount', X('volume-name', volume_name)),
                          self.ontap_api_url)

    def restrict_volume(self, volume_name):
        """
        Unmount and restrict a volume.
        """

        self.unmount_volume(volume_name)
        self.perform_call(X('volume-restrict', X('name', volume_name)),
                          self.ontap_api_url)

    def clone_volume(self, parent_volume_name, clone_name, junction_path,
                     parent_snapshot=None):
        """
        Clone an existing volume parent_volume_name into clone_name and
        onto junction_path.

        If parent_snapshot is true, use that snapshot as the base for
        the clone.
        """
        api_call = X('volume-clone-create',
                     X('junction-active', 'true'),
                     X('junction-path', junction_path),
                     X('parent-volume', parent_volume_name),
                     X('volume', clone_name))

        if parent_snapshot:
            api_call.append(X('parent-snapshot', parent_snapshot))

        self.perform_call(api_call, self.ontap_api_url)

    def create_export_policy(self, policy_name, rules=None):
        """
        Create a new export policy named policy_name, optionally with
        rules rules (otherwise, use whatever is default).
        """

        api_call = X('export-policy-create',
                     X('policy-name', policy_name),
                     X('return-record', 'true'))

        result = self.perform_call(api_call, self.ontap_api_url)

        name = _child_get_string(result[0], 'export-policy-info',
                                 'policy-name')
        id = _child_get_string(result[0], 'export-policy-info',
                               'policy-id')

        try:
            if rules is not None:
                for index, rule in enumerate(rules, start=1):
                    self.add_export_rule(policy_name, rule, index=index)
        except APIError as e:
            # The rule was probably invalid, roll back
            self.delete_export_policy(policy_name)
            # ...and re-raise the error
            raise e

        return name, id

    def add_export_rule(self, policy_name, rule, index=1):
        """
        Add a new export rule to policy_name.

        If no index is provided, add the rule to the head of the list
        (lowest priority).
        """
        api_call = X('export-rule-create',
                     X('policy-name', policy_name),
                     X('client-match', rule),
                     X('rule-index', str(index)),
                     X('ro-rule',
                       X('security-flavor', "sys")),
                     X('rw-rule',
                       X('security-flavor', "sys")),
                     X('anonymous-user-id', "0"),
                     X('protocol',
                       X('access-protocol', "nfs")),
                     X('super-user-security',
                       X('security-flavor', "sys")))

        self.perform_call(api_call, self.ontap_api_url)

    def remove_export_rule(self, policy_name, index):
        """
        Remove an export rule with a given index and re-number the
        following indices (e.g. decrement by one).
        """
        self.perform_call(X('export-rule-destroy',
                            X('policy-name', policy_name),
                            X('rule-index', str(index))),
                          self.ontap_api_url)

        remaining_rules = self.export_rules_of(policy_name)

        for i, index_rule in enumerate(remaining_rules, start=1):
            rule_index, _rule = index_rule
            self.perform_call(X('export-rule-set-index',
                                X('policy-name', policy_name),
                                X('rule-index', str(rule_index)),
                                X('new-rule-index', str(i))),
                              self.ontap_api_url)

    def delete_export_policy(self, policy_name):
        """
        Delete the export policy named policy_name, and all its rules.
        """

        self.perform_call(X('export-policy-destroy',
                            X('policy-name', policy_name)),
                          self.ontap_api_url)

    def rollback_volume_from_snapshot(self, volume_name, snapshot_name):
        """
        Roll back volume_name to its previous state snapshot_name.
        """
        self.perform_call(X('snapshot-restore-volume',
                            X('volume', volume_name),
                            X('snapshot', snapshot_name)),
                          self.ontap_api_url)

    def break_lock(self, volume_name, client_address):
        """
        Break any locks on volume_name held by client_address.

        Raises an APIError if there was no such lock or no such volume.
        """

        api_call = X('lock-break-iter',
                     X('query',
                       X('lock-info',
                         X('volume', volume_name),
                         X('client-address', client_address))))

        result = self.perform_call(api_call, self.ontap_api_url)
        self.raise_on_non_single_answer(result)

    def set_volume_autosize(self, volume_name, autosize_enabled,
                            max_size_bytes=None):
        """
        Update the autosize properties of volume_name.
        """
        enabled_str = "true" if autosize_enabled else "false"
        if autosize_enabled:
            if not (max_size_bytes):
                raise TypeError("Must provide max_size_bytes"
                                " when enabling autosize!")

        api_call = X('volume-autosize-set',
                     X('volume', volume_name),
                     X('is-enabled', enabled_str))
        if autosize_enabled:
            api_call.append(X('maximum-size', str(max_size_bytes)))
            api_call.append(X('mode','grow'))

        self.perform_call(api_call, self.ontap_api_url)

    def delete_snapshot(self, volume_name, snapshot_name):
        """
        Delete the snapshot named snapshot_name.
        """
        self.perform_call(X('snapshot-delete',
                            X('volume', volume_name),
                            X('snapshot', snapshot_name)),
                          self.ontap_api_url)

    def set_volume_export_policy(self, volume_name, policy_name):
        """
        Set the export policy of a given volume
        """
        self.volume_modify_iter(volume_name,
                                X('volume-attributes',
                                  X('volume-export-attributes',
                                    X('policy', policy_name))))

    def set_volume_snapshot_reserve(self, volume_name, reserve_percent):
        """
        Set a volume's reserved snapshot space (in percent).
        """

        self.volume_modify_iter(volume_name,
                                X('volume-attributes',
                                  X('volume-space-attributes',
                                    X('percentage-snapshot-reserve',
                                      str(reserve_percent)))))

    @property
    def aggregates(self):
        """
        A Generator of named tuples describing aggregates on the cluster.

        If in vserver mode, list that vserver's aggregates. If in
        cluster mode, list the entire cluster's aggregates.
        """

        def unpack_cluster_aggregate(aggregate_info):
            name = _child_get_string(aggregate_info, 'aggregate-name')
            node_names = _child_get_strings(aggregate_info,
                                            'nodes',
                                            'node-name')
            bytes_used = int(_child_get_string(aggregate_info,
                                               'aggr-space-attributes',
                                               'size-used'))
            bytes_available = int(_child_get_string(aggregate_info,
                                                    'aggr-space-attributes',
                                                    'size-available'))
            return Aggregate(name=name, node_names=node_names,
                             bytes_used=bytes_used,
                             bytes_available=bytes_available)

        def unpack_vserver_aggregate(show_aggregates):
            name = _child_get_string(show_aggregates, 'aggregate-name')

            # Fixme: verify that this size is actually in bytes!
            # Documentation doesn't say.
            bytes_available = _child_get_int(show_aggregates, 'available-size')
            return Aggregate(name=name, bytes_available=bytes_available,
                             bytes_used=None,
                             node_names=None)

        if not self.vfiler:
            # Cluster mode
            return self._get_paginated(X('aggr-get-iter',
                                         X('desired-attributes',
                                           X('aggregate-name'),
                                           X('aggregate-space-attributes',
                                             X('size-used'),
                                             X('size-available')),
                                           X('nodes'))),
                                       endpoint='ONTAP',
                                       container_tag='attributes-list',
                                       constructor=unpack_cluster_aggregate)
        else:
            return self._get_paginated(X('vserver-show-aggr-get-iter',
                                         X('desired-attributes',
                                           X('aggregate-name'),
                                           X('available-size')),
                                         X('query',
                                           X('show-aggregates',
                                             X('vserver-name',
                                               self.vfiler)))),
                                       endpoint='ONTAP',
                                       container_tag='attributes-list',
                                       constructor=unpack_vserver_aggregate)

    @property
    def vservers(self):
        """
        A Generator of named tuples describing vservers on the cluster.
        """
        def unpack_vserver(vserver_info):
            aggrs = _child_get_strings(vserver_info, 'aggr-list', 'aggr-name')
            state = _child_get_string(vserver_info, 'state')
            name = _child_get_string(vserver_info, 'vserver-name')
            uuid = _child_get_string(vserver_info, 'uuid')
            return Vserver(name=name, uuid=uuid,
                           aggregate_names=aggrs,
                           state=state)

        return self._get_paginated(X('vserver-get-iter',
                                     X('desired-attributes',
                                       X('aggr-list'),
                                       X('state'),
                                       X('vserver-name'),
                                       X('uuid'))),
                                   endpoint='ONTAP',
                                   container_tag='attributes-list',
                                   constructor=unpack_vserver)

    @property
    def ontap_system_version(self):
        """
        The system version as a string.
        """
        results = self.perform_call(X('system-get-version'),
                                    api_url=self.ontap_api_url)
        return _child_get_string(results, 'results', 'version')

    @property
    def ontapi_version(self):
        """
        Return the ONTAPI version as major.minor.
        """
        results = self.perform_call(X('system-get-ontapi-version'),
                                    api_url=self.ontap_api_url)
        major = _child_get_string(results, 'results', 'major-version')
        minor = _child_get_string(results, 'results', 'minor-version')
        return "{}.{}".format(major, minor)

    @property
    def ocum_version(self):
        """
        The OCUM API version as a string.
        """
        results = self.perform_call(X('system-about'),
                                    api_url=self.ocum_api_url)
        return _child_get_string(results, 'results', 'version')

    @property
    def supported_apis(self):
        """
        Return the list of API names supported by the server.

        Only works on ONTAPI servers.
        """

        results = self.perform_call(X('system-api-list'),
                                    api_url=self.ontap_api_url)
        return _child_get_strings(results,
                                  'results',
                                  'apis',
                                  'system-api-info',
                                  'name')

    @contextmanager
    def with_vserver(self, vserver):
        """
        A Context to temporarily set the vserver/vfiler (it's the same
        thing) during a set of operations, and then reset it to cluster
        mode (or whatever vserver it was configured to use previously)
        again.

        Needed for several operations, among others volume creation.

        Example::

            # Assumption: s is a ONTAPI server object with no vserver set.

            with s.with_vserver("vsrac11"):
                s.create_volume(...) # succeeds
                s.aggregates # fails

            s.create_volume(...) # fails
            s.aggregates # succeeds

        If vserver is none or the empty string, switch to cluster mode.
        """
        old_vfiler = self.vfiler
        log.debug("Temporarily setting vfiler to {}".format(vserver))
        self.vfiler = vserver
        yield
        log.debug("Restoring vfiler to {}".format(old_vfiler))
        self.vfiler = old_vfiler

    def destroy_volume(self, volume_name):
        """
        Permamently delete a volume. Must be offline.
        """
        log.info("Deleting volume {}".format(volume_name))
        result = self.perform_call(X('volume-destroy',
                                     X('name', volume_name)),
                                   self.ontap_api_url)

        for code, message in self.extract_failures(result):
            raise APIError(message=message, errno=code)

    def take_volume_offline(self, volume_name):
        """
        Take a volume offline. Must be unmounted first.

        Warning: will take *all* volumes with that name offline. Make
        sure you are on the correct vserver using with_vserver() or
        similar!
        """
        self.volume_modify_iter(volume_name, X('volume-attributes',
                                               X('volume-state-attributes',
                                                 X('state', 'offline'))))

    def extract_failures(self, result):
        """
        In the case of a call (as returned from `perform_call()`) that
        returns a <failure-list>, extract these as a generator.

        Returns (well, generates):
            a list of tuples of error_code, message, if any. Empty list
            if not.
        """
        errors = result.xpath(('/a:netapp/a:results/'
                               'a:failure-list/*'),
                              namespaces={'a': XMLNS})
        for error in errors:
            code = int(error.find('a:error-code', namespaces={'a': XMLNS}).text)
            message = error.find('a:error-message',
                                 namespaces={'a': XMLNS}).text
            yield (code, message)

    def raise_on_non_single_answer(self, result):
        num_successes = _child_get_int(result, 'results',
                                       'num-succeeded')
        num_failures = _child_get_int(result, 'results',
                                      'num-failed')

        for code, message in self.extract_failures(result):
            raise APIError(message=message, errno=code)

        if not (num_successes == 1 and num_failures == 0):
            raise APIError(message=("Unexpected answer: got"
                                    " {} results, {} failures!"
                                    .format(num_successes, num_failures)))

    def set_compression(self, volume_name, enabled=True, inline=True):
        ERR_ALREADY_ENABLED = 13001

        if not enabled and inline:
            raise ValueError("Inline compression cannot be enabled alone!")

        path = "/vol/{}".format(volume_name)

        sis_enable_call = X('sis-enable',
                            X('path', path))
        sis_set_config_call = X('sis-set-config',
                                X('enable-compression', str(enabled).lower()),
                                X('enable-inline-compression', (str(inline)
                                                                .lower())),
                                X('path', path))

        try:

            self.perform_call(sis_enable_call, self.ontap_api_url)
        except APIError as e:
            if e.errno == ERR_ALREADY_ENABLED:
                log.info("SIS already enabled for {}".format(volume_name))
            else:
                raise e

        self.perform_call(sis_set_config_call, self.ontap_api_url)

    def resize_volume(self, volume_name, new_size):
        """
        Resize the volume. Size follows the same conventions as for
        create_volume.
        """
        resize_call = X('volume-size',
                        X('volume', volume_name),
                        X('new-size', str(new_size)))

        self.perform_call(resize_call, self.ontap_api_url)

    def volume_modify_iter(self, volume_name, *attributes):
        """
        Make a call to volume-modify-iter with the provided attributes.
        """
        api_call = X('volume-modify-iter',
                     X('attributes', *attributes),
                     X('query',
                       X('volume-attributes',
                         X('volume-id-attributes',
                           X('name', volume_name)))))
        result = self.perform_call(api_call, self.ontap_api_url)
        self.raise_on_non_single_answer(result)
        return result

    def set_volume_caching_policy(self, volume_name, policy_name):
        """
        Set a volume's caching policy. Note that this is _different_
        from flexcache policies. The NetApp manual is not exactly clear
        on this, but this is the same attribute as cache-policy when
        creating volumes.
        """

        self.volume_modify_iter(volume_name,
                                X('volume-attributes',
                                  X('volume-hybrid-cache-attributes',
                                    X('caching-policy', policy_name))))

    def __init__(self, hostname, username, password, port=443,
                 transport_type="HTTPS", server_type="OCUM",
                 app_name=DEFAULT_APP_NAME,
                 timeout_s=DEFAULT_TIMEOUT,
                 vserver=""):
        """
        Instantiate a new server connection. Provided details are:

        :param hostname: the hostname of the server (or IP number)
        :param transport_type: HTTP or HTTPS (default is HTTPS)
        :param server_type: only OCUM currently supported
        :param app_name: the name of the calling app, as reported to the
          server
        :param timeout_s: The timeout in seconds for each connection to
          a NetApp filer. Passed as-is to Requests.
        :param vserver: The virtual server to use, if any.
        """

        self.hostname = hostname
        self.auth_tuple = (username, password)
        self.ocum_api_url = "https://%s:%d%s" % (hostname, port, OCUM_API_URL)
        self.ontap_api_url = "https://%s:%d%s" % (hostname,
                                                  port, ONTAP_API_URL)
        self.app_name = app_name
        self.session = requests.Session()
        self.timeout_s = timeout_s
        self.vfiler = vserver

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

        counter = 0
        while page_left_to_process:
            counter += 1
            log.debug("Getting page {}!".format(counter))
            response = self.perform_call(api_call, api_url)
            if container_tag is None:
                return

            num_records = int(
                response.xpath(('/a:netapp/a:results/'
                                'a:num-records/text()'),
                               namespaces={'a': XMLNS})[0])

            records = response.xpath(
                '/a:netapp/a:results/a:{}/*'
                .format(container_tag),
                namespaces={'a': XMLNS})

            assert num_records == len(records)

            for el in records:
                yield constructor(el)

            next_tag = None
            potential_next_tag = response.xpath(
                ('/a:netapp/a:results/'
                 'a:next-tag/text()'),
                namespaces={'a': XMLNS})
            # Is there another page?
            if not potential_next_tag:
                break

            # There was
            next_tag = potential_next_tag[0]
            next_api_call = api_call

            # According to the specification, we need to preserve
            # all options, but we also need to replace any previous
            # occurrences of 'tag'.

            tag_element = next_api_call.find('tag')
            if tag_element is not None:
                next_api_call.remove(tag_element)

            next_api_call.append(V.tag(next_tag))
            api_call = next_api_call

    def perform_call(self, api_call, api_url):
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

        If container_tag is not provided, perform a fire-and-forget call
        that will discard any returned data and not perform any
        extraction.

        Raises an APIError on erroneous API calls. Please note that
        calls performing batch-operations that return errors in a
        <failure-list> will return their results without triggering
        APIErrors, as the call itself has technically (by NetApp's
        definition) succeeded.
        """

        query_root = V.netapp(api_call,
                              xmlns=XMLNS,
                              version=XMLNS_VERSION,
                              nmsdk_app=self.app_name)
        if self.vfiler:
            query_root.attrib['vfiler'] = self.vfiler

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

            errno = int(response.xpath('/a:netapp/a:results/@errno',
                                       namespaces={'a': XMLNS})[0])

            raise APIError(message=reason, errno=errno,
                           failing_query=query_root)

        return response


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

    def __init__(self, raw_object, compression, inline):
        self.compression_enabled = compression
        self.inline_compression = inline
        self.uuid = _child_get_string(raw_object,
                                      'volume-id-attributes',
                                      'uuid')
        self.name = _child_get_string(raw_object,
                                      'volume-id-attributes',
                                      'name')
        self.active_policy_name = _child_get_string(raw_object,
                                                    'volume-export-attributes',
                                                    'policy')
        self.size_total_bytes = _child_get_int(
            raw_object,
            'volume-space-attributes',
            'size-total')
        self.size_used_bytes = _child_get_int(
            raw_object,
            'volume-space-attributes',
            'size-used')
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

        self.max_autosize = _int_or_none(_child_get_string(
            raw_object,
            'volume-autosize-attributes',
            'maximum-size'))
        self.owning_vserver_name = _child_get_string(raw_object,
                                                     'volume-id-attributes',
                                                     'owning-vserver-name')
        creation_timestamp = _child_get_int(raw_object,
                                            'volume-id-attributes',
                                            'creation-time')
        try:
            self.creation_time = datetime.fromtimestamp(
                creation_timestamp,
                pytz.timezone(LOCAL_TIMEZONE))
        except TypeError:
            log.info("Volume {} had no valid creation time!"
                     .format(self.name))
            self.creation_time = None

        self.percentage_snapshot_reserve = _child_get_int(
            raw_object,
            'volume-space-attributes',
            'percentage-snapshot-reserve')

        self.percentage_snapshot_reserve_used = _child_get_int(
            raw_object,
            'volume-space-attributes',
            'percentage-snapshot-reserve-used')

        self.caching_policy = _child_get_string(
            raw_object,
            'volume-hybrid-cache-attributes',
            'caching-policy')

    def __str__(self):
        return str("Volume{}".format(self.__dict__))

    def __eq__(self, other):
        return(isinstance(other, self.__class__)
               and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)


Lock = namedtuple('Lock', 'volume, state, client_address')
Aggregate = namedtuple('Aggregate',
                       'name, node_names, bytes_used, bytes_available')
Vserver = namedtuple('Vserver', 'name, state, uuid, aggregate_names')
Snapshot = namedtuple('Snapshot', 'name, creation_time, size_kbytes')


class APIError(Exception):
    """
    An Exception logging an api-related error and its context. Note that
    an APIError typically occurs *after* a successful transfer of a
    command to the API itself.

    Noteworthy properties are errno (error number), msg (error message)
    and failing_query (the XML query that was processed as the error
    occurred, if available).
    """
    def __init__(self, message="", errno="{no error #}", failing_query=None):
        self.msg = message.rstrip(". ")
        self.errno = errno

        try:
            self.failing_query = lxml.etree.tostring(failing_query,
                                                     pretty_print=True)
        except TypeError:
            if failing_query:
                self.failing_query = str(failing_query)
            else:
                self.failing_query = None

    def __str__(self):
        if self.failing_query:
            offq = ". Offending query: \n {}".format(self.failing_query)
        else:
            offq = ""

        str = "API Error {}: {}{}".format(self.errno, self.msg, offq)

        return str
