import os

import netapp.api
from datetime import datetime
import pytest

server_host = os.environ['NETAPP_HOST']
server_username = os.environ['NETAPP_USERNAME']
server_password = os.environ['NETAPP_PASSWORD']


def _connect_server():
    return netapp.api.Server(hostname=server_host, username=server_username,
                             password=server_password)


def test_list_all_events():
    s = _connect_server()

    found_anything = False
    for event in s.events:
        found_anything = True
        assert event

    assert found_anything

def test_list_events_after():
    s = _connect_server()

    found_anything = False
    for event in s.events.filter(greater_than_id=0):
        found_anything = True
        assert event

    assert found_anything


def test_list_events_all_filters():
    s = _connect_server()
    for event in s.events.filter(time_range=(1470045486, 1473252714),
                                 severities=['warning', 'information'],
                                 states=['new']):
        print(event)

def test_list_events_after_last_event():
    s =_connect_server()

    last_event = None
    for event in s.events:
        last_event = event

    last_time = last_event.timestamp + 1
    now = datetime.now().strftime('%s')


    for event in s.events.filter(time_range=(last_time, now)):
        print event
        assert False

def test_pagination_same_as_without():
    s = _connect_server()

    paginated_events = s.events.filter(max_records=2)
    unpaginated_events = s.events.filter()

    assert all(map(lambda x, y: x.id == y.id, paginated_events, unpaginated_events))

def test_no_filter_same_as_all():
    s = _connect_server()

    plain_events = s.events
    events_from_filter = s.events.filter()
    assert all(map(lambda x, y: x.id == y.id, plain_events, events_from_filter))

def test_severity_warning_only_warnings():
    s = _connect_server()

    for event in s.events.filter(severities=['warning']):
        assert event.severity == 'warning'

def test_invalid_severity_filter_throws_exception():
    s = _connect_server()

    with pytest.raises(Exception):
        for event in s.events.filter(severities=['fnord']):
            print event
            assert False

def test_get_known_event_id():
    s = _connect_server()

    known_event = None

    for event in s.events:
        known_event = event
        break

    single_event = s.events.single_by_id(known_event.id)

    assert single_event
    assert single_event.id == known_event.id
    assert single_event.name == known_event.name

def test_get_nonexistent_event_id():
    s = _connect_server()

    last_event_id = 0

    for event in s.events:
        last_event_id = event.id

    with pytest.raises(KeyError):
        s.events.single_by_id(last_event_id + 1)
