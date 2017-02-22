# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as Intergovernmental Organization
# or submit itself to any jurisdiction.

import os

import netapp.api
from datetime import datetime
import pytest


def is_ocum_env_setup():
    return ('NETAPP_HOST' in os.environ
            and 'NETAPP_USERNAME' in os.environ
            and 'NETAPP_PASSWORD' in os.environ)


def is_ontap_env_setup():
    return ('ONTAP_HOST' in os.environ
            and 'ONTAP_USERNAME' in os.environ
            and 'ONTAP_PASSWORD' in os.environ)


requires_ocum = pytest.mark.skipif(not is_ocum_env_setup(),
                                   reason=("NETAPP_{HOST, USERNAME, PASSWORD} "
                                           " not set up. Skipping tests"
                                           " requiring a server!"))

requires_ontap = pytest.mark.skipif(not is_ontap_env_setup(),
                                    reason=("ONTAP_{HOST, USERNAME, PASSWORD} "
                                            " not set up. Skipping tests"
                                            " requiring a server!"))


@pytest.fixture
def ocum_server():
    server_host = os.environ['NETAPP_HOST']
    server_username = os.environ['NETAPP_USERNAME']
    server_password = os.environ['NETAPP_PASSWORD']

    return netapp.api.Server(hostname=server_host, username=server_username,
                             password=server_password)


@pytest.fixture
def ontap_server():
    server_host = os.environ['ONTAP_HOST']
    server_username = os.environ['ONTAP_USERNAME']
    server_password = os.environ['ONTAP_PASSWORD']

    return netapp.api.Server(hostname=server_host, username=server_username,
                             password=server_password)


@requires_ocum
def test_list_all_events(ocum_server):
    found_anything = False
    for event in ocum_server.events:
        found_anything = True
        assert event

    assert found_anything


@requires_ocum
def test_list_events_after(ocum_server):
    found_anything = False
    for event in ocum_server.events.filter(greater_than_id=0):
        found_anything = True
        assert event

    assert found_anything


@requires_ocum
def test_list_events_all_filters(ocum_server):
    for event in ocum_server.events.filter(
            time_range=(1470045486, 1473252714),
            severities=['warning', 'information'],
            states=['new']):
        print(event)


@requires_ocum
def test_list_events_after_last_event(ocum_server):
    last_event = None
    for event in ocum_server.events:
        last_event = event

    last_time = last_event.timestamp + 1
    now = datetime.now().strftime('%s')

    for event in ocum_server.events.filter(time_range=(last_time, now)):
        print(event)
        assert False


@requires_ocum
def test_pagination_same_as_without(ocum_server):
    paginated_events = ocum_server.events.filter(max_records=2)
    unpaginated_events = ocum_server.events.filter()

    assert all(map(lambda x, y: x.id == y.id, paginated_events,
                   unpaginated_events))


@requires_ocum
def test_no_filter_same_as_all(ocum_server):
    plain_events = ocum_server.events
    events_from_filter = ocum_server.events.filter()
    assert all(map(lambda x, y: x.id == y.id, plain_events,
                   events_from_filter))


@requires_ocum
def test_severity_warning_only_warnings(ocum_server):
    for event in ocum_server.events.filter(severities=['warning']):
        assert event.severity == 'warning'


@requires_ocum
def test_invalid_severity_filter_throws_exception(ocum_server):
    with pytest.raises(Exception):
        for event in ocum_server.events.filter(severities=['fnord']):
            pass


@requires_ocum
def test_get_known_event_id(ocum_server):
    known_event = None

    for event in ocum_server.events:
        known_event = event
        break

    single_event = ocum_server.events.single_by_id(known_event.id)

    assert single_event
    assert single_event.id == known_event.id
    assert single_event.name == known_event.name


@requires_ocum
def test_get_nonexistent_event_id(ocum_server):
    last_event_id = 0

    for event in ocum_server.events:
        last_event_id = event.id

    with pytest.raises(KeyError):
        ocum_server.events.single_by_id(last_event_id + 1)


@requires_ontap
def test_get_all_volumes(ontap_server):
    for volume in ontap_server.volumes:
        assert volume.name


@requires_ontap
def test_filter_volumes_paginated(ontap_server):
    unpaginated = set([x.uuid for x in ontap_server.volumes])
    paginated = set([x.uuid for x in
                     ontap_server.volumes.filter(max_records=2)])

    assert unpaginated == paginated


@requires_ontap
def test_filter_name(ontap_server):
    first = list(ontap_server.volumes)[0]

    filtered_result = list(ontap_server.volumes.filter(name=first.name))
    assert len(filtered_result) == 1
    assert filtered_result[0].uuid == first.uuid


@requires_ontap
def test_filter_uuid(ontap_server):
    first = list(ontap_server.volumes)[0]

    filtered_result = list(ontap_server.volumes.filter(uuid=first.uuid))
    assert len(filtered_result) == 1
    assert filtered_result[0].uuid == first.uuid


@requires_ontap
def test_get_snapshots(ontap_server):
    snapshots = []
    for vol in ontap_server.volumes:
        snapshots += list(ontap_server.snapshots_of(vol.name))

    assert snapshots


@requires_ontap
def test_get_export_policies(ontap_server):
    assert 'default' in [x.name for x in ontap_server.export_policies]


@requires_ontap
def test_get_export_rules(ontap_server):
    for policy in ontap_server.export_policies:
        # Rules can be empty, but we need to force them to be realised
        assert policy.rules or policy.rules == []


@requires_ontap
def test_get_locks(ontap_server):
    for volume in ontap_server.volumes:
        locks = list(ontap_server.locks_on(volume.name))
        assert locks or locks == []
