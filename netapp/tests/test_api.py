# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as Intergovernmental Organization
# or submit itself to any jurisdiction.

import os
import re
import uuid

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
            and 'ONTAP_PASSWORD' in os.environ
            and 'ONTAP_VSERVER' in os.environ)


requires_ocum = pytest.mark.skipif(not is_ocum_env_setup(),
                                   reason=("NETAPP_{HOST, USERNAME, PASSWORD} "
                                           " not set up. Skipping tests"
                                           " requiring a server!"))

requires_ontap = pytest.mark.skipif(not is_ontap_env_setup(),
                                    reason=("ONTAP_{HOST, USERNAME, PASSWORD} "
                                            " not set up. Skipping tests"
                                            " requiring a server!"))

run_id = str(uuid.uuid4()).replace("-", "_")


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
    unpaginated_events = list(ocum_server.events.filter())

    # Make sure we fetch at least two pages:
    page_size = len(unpaginated_events)/2 - 1
    paginated_events = ocum_server.events.filter(max_records=page_size)

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
def test_filter_name_vserver(ontap_server):
    first = list(ontap_server.volumes)[0]

    filtered_result = list(ontap_server.volumes.filter(
        name=first.name,
        owning_vserver_name=first.owning_vserver_name))
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


@requires_ontap
def test_get_aggregates(ontap_server):
    aggrs = list(ontap_server.aggregates)
    assert aggrs


@requires_ontap
def test_get_version(ontap_server):
    assert ontap_server.ontapi_version


@requires_ontap
def test_api_names(ontap_server):
    assert 'vserver-get-iter' in ontap_server.supported_apis
    assert 'system-get-version' in ontap_server.supported_apis


@requires_ontap
def test_create_destroy_volume(ontap_server):
    for aggr in ontap_server.aggregates:
        if not re.match("^aggr0.*", aggr.name):
            aggregate_name = aggr.name
            break

    assert aggregate_name

    volume_name = 'test_volume_{}'.format(run_id)
    assert list(ontap_server.volumes.filter(name=volume_name)) == []
    size_mb = 512

    # Needs vserver mode
    with ontap_server.with_vserver(os.environ['ONTAP_VSERVER']):
        ontap_server.create_volume(name=volume_name,
                                   size_bytes=("{}"
                                               .format(size_mb * 1000 * 1000)),
                                   aggregate_name=aggregate_name,
                                   junction_path=("/test_volume_{}"
                                                  .format(run_id)))
        vol = next(ontap_server.volumes.filter(name=volume_name))
        assert vol.name == volume_name
        assert vol.containing_aggregate_name == aggregate_name
        assert vol.owning_vserver_name == os.environ['ONTAP_VSERVER']
        assert vol.size_total_bytes == size_mb * 1000 * 1000

        ontap_server.unmount_volume(volume_name)
        ontap_server.take_volume_offline(volume_name)
        ontap_server.destroy_volume(volume_name)

    # Make sure the context didn't permanently set the vserver:
    list(ontap_server.aggregates)

    assert list(ontap_server.volumes.filter(name=volume_name)) == []


@requires_ocum
def test_ocum_version(ocum_server):
    assert ocum_server.ocum_version
