import os

import netapp.api

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
    for event in s.events.greater_than_id(0):
        found_anything = True
        assert event

    assert found_anything


def test_list_events_after_nonexistent_event():
    s = _connect_server()
    for event in s.events.greater_than_id(1):
        print(event)

def test_list_events_after_last_event():
    pass

def test_list_events_after_middle_event():
    pass

def test_event_get_specific():
    pass

def test_event_get_nonexistent():
    pass

def test_invalid_credentials():
    pass
