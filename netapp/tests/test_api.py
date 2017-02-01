# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as Intergovernmental Organization
# or submit itself to any jurisdiction.

import os

import netapp.api
from datetime import datetime
import pytest


def is_env_setup():
    return ('NETAPP_HOST' in os.environ
            and 'NETAPP_USERNAME' in os.environ
            and 'NETAPP_PASSWORD' in os.environ)


pytestmark = pytest.mark.skipif(not is_env_setup(),
                                reason=("NETAPP_{HOST, USERNAME, PASSWORD} "
                                        " not set up. Skipping tests requiring"
                                        " a server!"))


@pytest.fixture
def server():
    server_host = os.environ['NETAPP_HOST']
    server_username = os.environ['NETAPP_USERNAME']
    server_password = os.environ['NETAPP_PASSWORD']

    return netapp.api.Server(hostname=server_host, username=server_username,
                             password=server_password)


def test_list_all_events(server):
    found_anything = False
    for event in server.events:
        found_anything = True
        assert event

    assert found_anything


def test_list_events_after(server):
    found_anything = False
    for event in server.events.filter(greater_than_id=0):
        found_anything = True
        assert event

    assert found_anything


def test_list_events_all_filters(server):
    for event in server.events.filter(time_range=(1470045486, 1473252714),
                                      severities=['warning', 'information'],
                                      states=['new']):
        print(event)


def test_list_events_after_last_event(server):
    last_event = None
    for event in server.events:
        last_event = event

    last_time = last_event.timestamp + 1
    now = datetime.now().strftime('%s')

    for event in server.events.filter(time_range=(last_time, now)):
        print(event)
        assert False


def test_pagination_same_as_without(server):
    paginated_events = server.events.filter(max_records=2)
    unpaginated_events = server.events.filter()

    assert all(map(lambda x, y: x.id == y.id, paginated_events,
                   unpaginated_events))


def test_no_filter_same_as_all(server):
    plain_events = server.events
    events_from_filter = server.events.filter()
    assert all(map(lambda x, y: x.id == y.id, plain_events,
                   events_from_filter))


def test_severity_warning_only_warnings(server):
    for event in server.events.filter(severities=['warning']):
        assert event.severity == 'warning'


def test_invalid_severity_filter_throws_exception(server):
    with pytest.raises(Exception):
        for event in server.events.filter(severities=['fnord']):
            print(event)
            assert False


def test_get_known_event_id(server):
    known_event = None

    for event in server.events:
        known_event = event
        break

    single_event = server.events.single_by_id(known_event.id)

    assert single_event
    assert single_event.id == known_event.id
    assert single_event.name == known_event.name


def test_get_nonexistent_event_id(server):
    last_event_id = 0

    for event in server.events:
        last_event_id = event.id

    with pytest.raises(KeyError):
        server.events.single_by_id(last_event_id + 1)
