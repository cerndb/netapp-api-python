import os

import netapp.api

import pytest

server_host = os.environ['NETAPP_HOST']
server_port = os.environ['NETAPP_PORT']
server_username = os.environ['NETAPP_USERNAME']
server_password = os.environ['NETAPP_PASSWORD']


def test_list_all_events():
    pass

def test_list_events_after():
    pass
