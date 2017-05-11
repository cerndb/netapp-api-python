# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as Intergovernmental Organization
# or submit itself to any jurisdiction.
import netapp.api

import os
import re
from contextlib import contextmanager
import logging
from datetime import datetime
from itertools import islice

import pytest
import betamax
import pytz


log = logging.getLogger(__name__)
MAX_EVENTS = 100


def policy_by_name(policies_gen, policy_name):
    for policy in policies_gen:
        if not policy.name == policy_name:
            continue
        else:
            return policy
    raise KeyError(policy_name)


run_id = "42"
count_ocum_server = 0
count_ontap_server = 0
ONTAP_VSERVER = os.environ.get('ONTAP_VSERVER', 'vs1rac11')


def new_volume(server, volume_name=None):
    with server.with_vserver(None):
        # We can't get aggregates in vserver mode, so switch to cluster
        # mode:
        for aggr in server.aggregates:
            if not re.match("^aggr0.*", aggr.name):
                aggregate_name = aggr.name
                break

    size_mb = 100
    if not volume_name:
        volume_name = 'test_volume_{}'.format(run_id)
    log.info("Creating new volume named {}".format(volume_name))
    with server.with_vserver(ONTAP_VSERVER):
        server.create_volume(name=volume_name,
                             size_bytes=("{}"
                                         .format(size_mb * 1000 * 1000)),
                             aggregate_name=aggregate_name,
                             junction_path=("/test_volume_{}"
                                            .format(run_id)))
    return volume_name


def delete_volume(server, volume_name):
    log.info("Delete ephermeral volume {}".format(volume_name))
    with server.with_vserver(ONTAP_VSERVER):
        server.unmount_volume(volume_name)
        server.take_volume_offline(volume_name)
        server.destroy_volume(volume_name)


@contextmanager
def ephermeral_volume(server):
    vol_name = new_volume(server)
    try:
        yield vol_name
    finally:
        delete_volume(server, vol_name)


@pytest.fixture
def ocum_server():
    server_host = os.environ.get('NETAPP_HOST', 'db-51195')
    server_username = os.environ.get('NETAPP_USERNAME', "user-placeholder")
    server_password = os.environ.get('NETAPP_PASSWORD', "password-placeholder")

    s = netapp.api.Server(hostname=server_host, username=server_username,
                          password=server_password, timeout_s=10)

    recorder = betamax.Betamax(s.session)
    yield (recorder, s)


@pytest.fixture
def ontap_server():
    server_host = os.environ.get('ONTAP_HOST', 'dbnasa-cluster-mgmt')
    server_username = os.environ.get('ONTAP_USERNAME', 'user-placeholder')
    server_password = os.environ.get('ONTAP_PASSWORD', 'password-placeholder')

    s = netapp.api.Server(hostname=server_host, username=server_username,
                          password=server_password)

    recorder = betamax.Betamax(s.session)
    yield (recorder, s)


def test_list_all_events(ocum_server):
    recorder, server = ocum_server

    found_anything = False
    with recorder.use_cassette('list_all_events'):
        for event in islice(server.events, MAX_EVENTS):
            found_anything = True
            assert event

    assert found_anything


def test_list_events_after(ocum_server):
    recorder, server = ocum_server

    found_anything = False
    with recorder.use_cassette('list_events_after'):
        for event in islice(server.events.filter(greater_than_id=0),
                            MAX_EVENTS):
            found_anything = True
            assert event

    assert found_anything


def test_list_events_all_filters(ocum_server):
    recorder, server = ocum_server

    with recorder.use_cassette('list_all_filters'):
        list(server.events.filter(
            time_range=(1470045486, 1473252714),
            severities=['warning', 'information'],
            states=['new']))


def test_list_events_after_last_event(ocum_server):
    recorder, server = ocum_server
    last_event = None
    with recorder.use_cassette('after_last',
                               match_requests_on=['method', 'uri']):
        for event in server.events:
            last_event = event

        last_time = last_event.timestamp + 1
        now = datetime.now().strftime('%s')

        for event in server.events.filter(time_range=(last_time, now)):
            assert False


def test_pagination_same_as_without(ocum_server):
    recorder, server = ocum_server

    with recorder.use_cassette('pagination'):
        unpaginated_events = list(server.events.filter())

        # Make sure we fetch at least two pages:
        page_size = len(unpaginated_events)/2 - 1
        paginated_events = server.events.filter(max_records=page_size)

        assert all(map(lambda x, y: x.id == y.id, paginated_events,
                       unpaginated_events))


def test_no_filter_same_as_all(ocum_server):
    recorder, server = ocum_server

    with recorder.use_cassette('no_filter_same_as_all'):
        plain_events = islice(server.events, MAX_EVENTS)
        events_from_filter = islice(server.events.filter(), MAX_EVENTS)
        assert all(map(lambda x, y: x.id == y.id, plain_events,
                       events_from_filter))


def test_severity_warning_only_warnings(ocum_server):
    recorder, server = ocum_server

    with recorder.use_cassette('only_warning'):
        for event in islice(server.events.filter(severities=['warning']),
                            MAX_EVENTS):
            assert event.severity == 'warning'


def test_invalid_severity_filter_throws_exception(ocum_server):
    recorder, server = ocum_server

    with recorder.use_cassette('invalid_severity'):
        with pytest.raises(netapp.api.APIError):
            list(server.events.filter(severities=['fnord']))


def test_get_known_event_id(ocum_server):
    known_event = None
    recorder, server = ocum_server

    with recorder.use_cassette('known_event_id'):
        for event in server.events:
            known_event = event
            break

        single_event = server.events.single_by_id(known_event.id)

        assert single_event
        assert single_event.id == known_event.id
        assert single_event.name == known_event.name


def test_get_nonexistent_event_id(ocum_server):
    last_event_id = 0
    recorder, server = ocum_server

    with recorder.use_cassette('nonexistent_event_id'):
        for event in server.events:
            last_event_id = event.id

        with pytest.raises(KeyError):
            server.events.single_by_id(last_event_id + 10)


def test_get_all_volumes(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('all_volumes'):
        for volume in server.volumes:
            assert volume.name


def test_filter_volumes_paginated(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('paginated_volumes'):
        unpaginated = set([x.uuid for x in server.volumes])
        paginated = set([x.uuid for x in
                         server.volumes.filter(max_records=2)])

        assert unpaginated == paginated


def test_filter_name_vserver(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('filter_name_vserver'):
        first = list(server.volumes)[0]

        filtered_result = list(server.volumes.filter(
            name=first.name,
            owning_vserver_name=first.owning_vserver_name))
        assert len(filtered_result) == 1
        assert filtered_result[0].uuid == first.uuid


def test_filter_uuid(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('filter_uuid'):
        first = list(server.volumes)[0]

        filtered_result = list(server.volumes.filter(uuid=first.uuid))
        assert len(filtered_result) == 1
        assert filtered_result[0].uuid == first.uuid


def test_get_snapshots(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('snapshots'):
        snapshots = []
        for vol in server.volumes:
            snapshots += list(server.snapshots_of(vol.name))

    assert snapshots


def test_get_export_policies(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('export_policies'):
        assert 'default' in [x.name for x in server.export_policies]


def test_get_export_rules(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('export_rules'):
        for policy in server.export_policies:
            # Rules can be empty, but we need to force them to be realised
            assert policy.rules or policy.rules == []


def test_get_locks(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('get_locks'):
        for volume in server.volumes:
            locks = list(server.locks_on(volume.name))
            assert locks or locks == []


def test_get_aggregates(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('list_aggrs'):
        aggrs = list(server.aggregates)
        assert aggrs


def test_get_version(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('get_version'):
        assert server.ontapi_version


def test_api_names(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('supported_apis',
                               match_requests_on=['method', 'uri']):
        assert 'vserver-get-iter' in server.supported_apis
        assert 'system-get-version' in server.supported_apis


def test_create_destroy_volume(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('destroy_volume'):
        for aggr in server.aggregates:
            if not re.match("^aggr0.*", aggr.name):
                aggregate_name = aggr.name
                break

        assert aggregate_name

        volume_name = 'test_volume_{}'.format(run_id)
        assert list(server.volumes.filter(name=volume_name)) == []
        size_mb = 512

        # Needs vserver mode
        with server.with_vserver(ONTAP_VSERVER):
            server.create_volume(name=volume_name,
                                 size_bytes=("{}"
                                             .format(size_mb * 1000 * 1000)),
                                 aggregate_name=aggregate_name,
                                 junction_path=("/test_volume_{}"
                                                .format(run_id)))
            vol = next(server.volumes.filter(name=volume_name))
            assert vol.name == volume_name
            assert vol.containing_aggregate_name == aggregate_name
            assert vol.owning_vserver_name == ONTAP_VSERVER
            assert vol.size_total_bytes == size_mb * 1000 * 1000
            assert vol.percentage_snapshot_reserve == 0
            assert vol.percentage_snapshot_reserve_used == 0


            delete_volume(server, volume_name)

        # Make sure the context didn't permanently set the vserver:
        list(server.aggregates)

        assert list(server.volumes.filter(name=volume_name)) == []


def test_ocum_version(ocum_server):
    recorder, server = ocum_server

    with recorder.use_cassette('ocum_version'):
        assert server.ocum_version


def test_create_with_policy(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('create_with_policy'):
        for aggr in server.aggregates:
            if not re.match("^aggr0.*", aggr.name):
                aggregate_name = aggr.name
                break

        # Pick the first non-empty policy, or, if none, the last policy
        for policy in server.export_policies:
            policy_name = policy.name
            if policy.rules:
                break

        volume_name = 'test_volume_{}'.format(run_id)
        size_mb = 100

        with server.with_vserver(ONTAP_VSERVER):
            server.create_volume(name=volume_name,
                                 size_bytes=("{}"
                                             .format(size_mb * 1000 * 1000)),
                                 aggregate_name=aggregate_name,
                                 junction_path=("/test_volume_{}"
                                                .format(run_id)),
                                 export_policy_name=policy_name)
            vol = server.volumes.single(volume_name=volume_name)
            assert vol.name == volume_name
            assert vol.active_policy_name == policy_name
            delete_volume(server, volume_name)


def test_create_snapshot(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('create_snapshot'):
        volume_name = new_volume(server)
        snapshot_name = "test_snap"
        with server.with_vserver(ONTAP_VSERVER):
            server.create_snapshot(volume_name, snapshot_name=snapshot_name)
            snapshots = list(server.snapshots_of(volume_name=volume_name))
            assert snapshot_name in snapshots
            delete_volume(server, volume_name)


def test_restrict_volume(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('restrict_volume'):
        with ephermeral_volume(server) as volume_name:
            with server.with_vserver(ONTAP_VSERVER):
                server.restrict_volume(volume_name)
                vol = server.volumes.single(volume_name=volume_name)
                assert vol.state == 'restricted'


@pytest.mark.xfail(reason="Not supported by license")
def test_clone_volume(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('clone_volume'):
        with ephermeral_volume(server) as volume_name:
            clone_name = 'test_clone_{}'.format(run_id)
            junction_path = "/test_clone_{}".format(run_id)
            with server.with_vserver(ONTAP_VSERVER):
                server.clone_volume(parent_volume_name=volume_name,
                                    clone_name=clone_name,
                                    junction_path=junction_path)
                vol = server.volumes.single(volume_name=volume_name)
                clone = server.volumes.single(volume_name=clone_name)
        delete_volume(server, clone_name)
        assert vol == clone


def test_create_delete_export_policy_with_rules(ontap_server):
    rules = ["127.0.0.1", "10.10.10.1", "10.10.10.12"]
    policy_name = "test_policy_{}".format(run_id)
    recorder, server = ontap_server

    with recorder.use_cassette('create_delete_export_policy'):
        with server.with_vserver(ONTAP_VSERVER):
            server.create_export_policy(policy_name=policy_name, rules=rules)

            policy = policy_by_name(server.export_policies, policy_name)
            assert [r for _id, r in policy.rules] == rules

        # clean up
        with server.with_vserver(ONTAP_VSERVER):
            server.delete_export_policy(policy_name=policy_name)


def test_create_delete_export_policy_without_rules(ontap_server):
    policy_name = "test_policy_no_rules{}".format(run_id)
    recorder, server = ontap_server

    with recorder.use_cassette('create_delete_export_policy_no_rules'):
        with server.with_vserver(ONTAP_VSERVER):
            server.create_export_policy(policy_name=policy_name)

        assert policy_name in [x.name for x in server.export_policies]

        with server.with_vserver(ONTAP_VSERVER):
            server.delete_export_policy(policy_name=policy_name)

        assert policy_name not in [x.name for x in server.export_policies]


def test_add_export_rules_no_index(ontap_server):
    policy_name = "test_policy_add_rules{}".format(run_id)
    rules = ["10.10.10.1", "10.10.10.2", "10.10.10.5", "127.0.0.1"]
    recorder, server = ontap_server

    with recorder.use_cassette('indexless_export_rule_add'):
        with server.with_vserver(ONTAP_VSERVER):
            server.create_export_policy(policy_name=policy_name)
            for rule in rules[::-1]:  # wierd "martian smiley" means "reverse"
                server.add_export_rule(policy_name=policy_name,
                                       rule=rule)

        policy = policy_by_name(server.export_policies, policy_name)
        assert [r for _id, r in policy.rules] == rules

        with server.with_vserver(ONTAP_VSERVER):
            server.delete_export_policy(policy_name=policy_name)


def test_remove_export_rule(ontap_server):
    policy_name = "test_policy_remove_rules{}".format(run_id)
    rules = ["10.10.10.1", "10.10.10.2", "10.10.10.5", "127.0.0.1"]
    recorder, server = ontap_server

    with recorder.use_cassette('remove_export_rule'):
        with server.with_vserver(ONTAP_VSERVER):
            server.create_export_policy(policy_name=policy_name, rules=rules)

        added_policy = policy_by_name(server.export_policies, policy_name)

        indices = [i for i, _r in added_policy.rules]

        # remove rule #3
        with server.with_vserver(ONTAP_VSERVER):
            server.remove_export_rule(policy_name, index=indices[2])

        modified_policy = policy_by_name(server.export_policies, policy_name)

        # Assert correct (updated) ordering
        for idx, rule in enumerate(modified_policy.rules, start=1):
            r_id, r = rule
            assert r_id == idx

        # Remove the third rule from the template set as well
        del rules[2]
        assert [r for _id, r in modified_policy.rules] == rules

        with server.with_vserver(ONTAP_VSERVER):
            server.delete_export_policy(policy_name=policy_name)


@pytest.mark.xfail(reason="Requires license")
def test_rollback_from_snapshot(ontap_server):
    snapshot_name = "test_snap"
    with ephermeral_volume(ontap_server) as volume_name:
        with ontap_server.with_vserver(ONTAP_VSERVER):
            ontap_server.create_snapshot(volume_name,
                                         snapshot_name=snapshot_name)

            ontap_server.rollback_volume_from_snapshot(
                volume_name=volume_name,
                snapshot_name=snapshot_name)


@pytest.mark.xfail(reason="Requires license")
def test_clone_from_snapshot(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('clone_from_snapshot'):
        with ephermeral_volume(server) as volume_name:
            snapshot_name = "test_snap"
            clone_name = 'test_clone_{}'.format(run_id)
            junction_path = "/test_clone_{}".format(run_id)
            with server.with_vserver(ONTAP_VSERVER):
                server.create_snapshot(volume_name,
                                       snapshot_name=snapshot_name)

                server.clone_volume(parent_volume_name=volume_name,
                                    clone_name=clone_name,
                                    junction_path=junction_path,
                                    parent_snapshot=snapshot_name)
                vol = server.volumes.single(volume_name=volume_name)
                clone = server.volumes.single(volume_name=clone_name)
                delete_volume(server, clone_name)
                assert vol == clone


def test_ephermeral_volume(ontap_server):
    recorder, server = ontap_server
    with recorder.use_cassette('ephermeral_volume'):
        with pytest.raises(Exception):
            with ephermeral_volume(server) as vn:
                assert vn in [v.name for v in server.volumes]
                name = vn
                raise Exception

        assert name not in [v.name for v in server.volumes]


def test_set_autosize_enable(ontap_server):
    max_size_kb = 100 * 1000 * 1000
    increment_kb = 10000
    recorder, server = ontap_server

    # This makes the same request repeatedly, it seems, so we need
    # to record it in new_episodes mode.
    with recorder.use_cassette('vol_autosize_enable'):
        with ephermeral_volume(server) as vn:
            with server.with_vserver(ONTAP_VSERVER):
                server.set_volume_autosize(
                    volume_name=vn,
                    max_size_bytes=max_size_kb * 1000,
                    increment_bytes=increment_kb * 1000,
                    autosize_enabled=True)
                vol = server.volumes.single(volume_name=vn)
                assert vol.autosize_enabled
                # Sometimes there is apparently some rounding
                # But not a factor 10 too much, we can assume:
                assert vol.autosize_increment >= increment_kb * 1000
                assert vol.autosize_increment <= increment_kb * 1000 * 10
                assert vol.max_autosize >= max_size_kb * 1000
                assert vol.max_autosize <= max_size_kb * 1000 * 10


def test_set_autosize_disable(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('vol_autosize_disable'):
        with ephermeral_volume(server) as vn:
            with server.with_vserver(ONTAP_VSERVER):
                server.set_volume_autosize(
                    volume_name=vn,
                    max_size_bytes=1,
                    increment_bytes=1,
                    autosize_enabled=False)
            vol = server.volumes.single(volume_name=vn)
            assert not vol.autosize_enabled


def test_set_autosize_invalid_call(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('vol_autosize_invalid_call'):
        with pytest.raises(TypeError):
            server.set_volume_autosize(volume_name="bork",
                                       autosize_enabled=True)


def test_set_export_policy(ontap_server):
    policy_name = "test_policy_set_export_policy{}".format(run_id)
    recorder, server = ontap_server

    with recorder.use_cassette('set_export_policy'):
        with ephermeral_volume(server) as vn:
            with server.with_vserver(ONTAP_VSERVER):
                server.create_export_policy(policy_name=policy_name)
                server.set_volume_export_policy(volume_name=vn,
                                                policy_name=policy_name)
                vol = server.volumes.single(volume_name=vn)
                assert vol.active_policy_name == policy_name

        # Must be done after the volume is deleted:
        with server.with_vserver(ONTAP_VSERVER):
            server.delete_export_policy(policy_name)


def test_delete_snapshot(ontap_server):
    snapshot_name = "test_snap"
    recorder, server = ontap_server

    with recorder.use_cassette('delete_snapshot'):
        with ephermeral_volume(server) as vn:
            with server.with_vserver(ONTAP_VSERVER):
                server.create_snapshot(vn, snapshot_name=snapshot_name)
                server.delete_snapshot(vn, snapshot_name=snapshot_name)
            assert snapshot_name not in server.snapshots_of(volume_name=vn)


def test_get_vservers(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('list_vservers'):
        vservers = list(server.vservers)
        assert vservers


def test_ontap_system_version(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('ontap_system_version'):
        assert server.ontap_system_version


def test_destroy_nonexistent_volume(ontap_server):
    volume_name = run_id
    recorder, server = ontap_server

    with recorder.use_cassette('destroy_nonexistent'):
        with server.with_vserver(ONTAP_VSERVER):
            with pytest.raises(netapp.api.APIError):
                server.destroy_volume(volume_name=volume_name)


def test_create_existent_volume(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('ontap_create_existent'):
        with server.with_vserver(ONTAP_VSERVER):
            with ephermeral_volume(server) as vn:
                with pytest.raises(netapp.api.APIError):
                    new_volume(server, volume_name=vn)


def test_offline_nonexistent_volume(ontap_server):
    volume_name = run_id
    recorder, server = ontap_server

    with recorder.use_cassette('offline_nonexistent'):
        with server.with_vserver(ONTAP_VSERVER):
            with pytest.raises(netapp.api.APIError):
                server.take_volume_offline(volume_name=volume_name)


def test_set_nonexistent_export_policy(ontap_server):
    policy_name = "test_policy_nonexistent_export_policy{}".format(run_id)
    recorder, server = ontap_server

    with recorder.use_cassette('set_nonexistent_export_policy'):
        with ephermeral_volume(server) as vn:
            with server.with_vserver(ONTAP_VSERVER):
                with pytest.raises(netapp.api.APIError):
                    server.set_volume_export_policy(volume_name=vn,
                                                    policy_name=policy_name)


def test_break_locks_nonexistent(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('ontap_break_nonexistent_locks'):
        with ephermeral_volume(server) as vn:
            with server.with_vserver(ONTAP_VSERVER):
                with pytest.raises(netapp.api.APIError):
                    server.break_lock(volume_name=vn,
                                      client_address="made-up-client")


def test_volume_read_compression_status(ontap_server):
    recorder, server = ontap_server
    with recorder.use_cassette('read_compression_status'):
        for volume in server.volumes:
            assert hasattr(volume, 'compression_enabled')
            assert hasattr(volume, 'inline_compression')
            return


def test_volume_set_get_compression_status(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('set_read_compression_status'):
        with ephermeral_volume(server) as volume_name:
            vol = server.volumes.single(volume_name=volume_name)
            with server.with_vserver(vol.owning_vserver_name):
                server.set_compression(vol.name, enabled=True, inline=True)
                vol_updated = server.volumes.single(volume_name=volume_name)
                assert vol_updated.compression_enabled is True
                assert vol_updated.inline_compression is True


def test_volume_compression_on_by_default(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('compression_default_status'):
        with ephermeral_volume(server) as volume_name:
            vol = server.volumes.single(volume_name=volume_name)
            assert vol.compression_enabled
            assert vol.inline_compression


def test_volume_compression_disable(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('compression_disable'):
        with ephermeral_volume(server) as volume_name:
            with server.with_vserver(ONTAP_VSERVER):
                server.set_compression(volume_name, enabled=False,
                                       inline=False)
                vol = server.volumes.single(volume_name=volume_name)
                assert not vol.compression_enabled
                assert not vol.inline_compression

                server.set_compression(volume_name, enabled=False,
                                       inline=False)


def test_volume_has_ctime(ontap_server):

    recorder, server = ontap_server

    with recorder.use_cassette('volume_has_ctime'):
        with ephermeral_volume(server) as volume_name:
            vol = server.volumes.single(volume_name=volume_name)
            assert hasattr(vol, 'creation_time')
            now = datetime.now(tz=pytz.timezone(netapp.api.LOCAL_TIMEZONE))
            assert vol.creation_time <= now
            # Due to recorded data, we cannot reliably give a relative lower
            # bound.


def test_create_different_snapshot_space(ontap_server):
    recorder, server = ontap_server
    vol_name = 'test_snapshotspace_volume'

    with recorder.use_cassette('create_volume_with_snapshot_space'):
        for aggr in server.aggregates:
            if not re.match("^aggr0.*", aggr.name):
                aggregate_name = aggr.name
                break

        try:
            with server.with_vserver(ONTAP_VSERVER):
                server.create_volume(name=vol_name,
                                     size_bytes=100000000,
                                     aggregate_name=aggregate_name,
                                     junction_path="/{}".format(vol_name),
                                     percentage_snapshot_reserve=20)

                vol = server.volumes.single(volume_name=vol_name)
                assert vol.percentage_snapshot_reserve == 20

        finally:
            delete_volume(server, vol_name)


def test_set_snapshot_reserve(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('set_snapshot_reserve'):
        with ephermeral_volume(server) as volume_name:
            with server.with_vserver(ONTAP_VSERVER):
                server.set_volume_snapshot_reserve(volume_name=volume_name,
                                                   reserve_percent=37)
            vol = server.volumes.single(volume_name=volume_name)
            assert vol.percentage_snapshot_reserve == 37


def test_get_cache_policy(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('get_cache_policy'):
        with ephermeral_volume(server) as volume_name:
            vol = server.volumes.single(volume_name=volume_name)
            assert (vol.caching_policy == 'default' or vol.caching_policy
                    is None)


def test_get_aggregates_vfiler_mode(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('get_aggregates_vfiler_mode'):
        with server.with_vserver(ONTAP_VSERVER):
            aggregates = list(server.aggregates)
            assert aggregates


def test_create_policy_with_invalid_rules_not_created(ontap_server):
    recorder, server = ontap_server

    with recorder.use_cassette('create_invalid_export_policy_rule'):
        with server.with_vserver(ONTAP_VSERVER):
            invalid_rules = ["burk@spenat@gurka++1242"]
            policy_name = "test_policy_asdf_hjkl_14"
            with pytest.raises(netapp.api.APIError):
                server.create_export_policy(policy_name, rules=invalid_rules)
            assert policy_name not in server.export_policies
