# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as Intergovernmental Organization
# or submit itself to any jurisdiction.

"""
This is an XML vocabulary for generating queries against NetApp's API.

It has intentionally been kept very close to the original
representation, and syntactic sugar or wrapping has been kept to a
minimum to avoid confusion in the event of API changes.

**Examples**

The following Python code::

    netapp(event_iter(event_id("17"),
                      timetout("4")),
           xmlns="http//my-namespace.com/xml",
           version="1.0"
           nmsdk_app="test-app")

Will produce this XML::

    <netapp xmlns="http://my-namespace.com/xml" version="1.0"
            nmsdk_app="test-app">
        <event-iter>
            <event-id>17</event-id>
            <timeout>4</timeout>
        </event-iter>
    </netapp>

"""

import lxml.builder
import lxml.etree

E = lxml.builder.ElementMaker()


netapp = E.netapp
"The root element for any query."

timeout = E.timeout
"The timeout for an operation: ``<timeout>x</timeout>``."

source = E.source
"Resource or group."

tag = E.tag
"The tag from the last call, if processing paginated data."


def _value_element(tag_name, tag_value):
    """
    Helper function: generate ``<tag_name>tag_value</tag_name>``.
    """
    elm = lxml.etree.Element(tag_name)
    elm.text = str(tag_value)
    return elm


def _with_children(tag_name, children=[]):
    """
    Helper function: create a tag and append a set of ready-made child tags.
    """
    elm = lxml.etree.Element(tag_name)
    for child in children:
        elm.append(child)
    return elm


def event_iter(*children):
    """
    Generate an ``<event-iter>[children]</event-iter>`` tag.
    """
    return _with_children('event-iter', children)


def greater_than_id(id):
    """
    Generate an ``<greater-than-id>id</greater-than-id>`` tag.
    """
    return _value_element('greater-than-id', id)


def event_severities(*severities):
    """
    One or more event severities, returns something like::

        <event-severity>
            <obj-status>critical</obj-status>
            <obj-status>important</obj-status>
        </event-severity>

    Children needs to be ``<obj-status>``:es containing Severity strings.

    Cannot be empty.
    """
    return _with_children('event-severities', severities)


def obj_status(status):
    """
    The contents of an ``<event-severity>`` tag, a String representing a
    status.

    Example values:

    - ``<obj-status>CRITICAL</obj-status>``
    - ``<obj-status>WARNING</obj-status>``
    """
    return _value_element('obj-status', status)


def time_range(timestamp_range):
    """
    The outer wrapper (!) for an <event-timestamp-range> element.

    Example::

        <time-range>
            <event-timestamp-range>
                <end-time>17</end-time>
                <start-time>23</start-time>
            </event-timestamp-range>
        </time-range>
    """
    return _with_children('time-range', [timestamp_range])


def event_timestamp_range(start_time, end_time):
    """
    The inner wrapper tag for an event time range. See time_range for example!
    """
    return _with_children('event-timestamp-range', (start_time, end_time))


def start_time(start_timestamp):
    """
    A container tag for a starting timestamp. See time_range for examples.
    """
    return _value_element('start-time', start_timestamp)


def end_time(end_timestamp):
    """
    A container tag for an ending timestamp. See time_range for examples.
    """
    return _value_element('end-time', end_timestamp)


def event_state_filter_list(*states):
    """
    A container for at least one ``<event-state>`` tag to filter on.
    """
    return _with_children('event-state-filter-list', states)


def event_state(state_description):
    """
    Value container for an event state: ``<event-state>NEW</event-state>``.
    """
    return _value_element('event-state', state_description)


def max_records(max_number_records):
    """
    Value container for a maximum number of records option:
    ``<max-records>17</max-records>``.
    """
    return _value_element('max-records', max_number_records)


def event_id(id):
    """
    Value container for an event ID:
    ``<event-id>17</event-id>``.
    """
    return _value_element('event-id', id)
