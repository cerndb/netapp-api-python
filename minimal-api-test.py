#!/usr/bin/env python2.7

import os

import netapp.api
from netapp import vocabulary as xml
#import netapp.vocalbulary

import lxml.etree

OCUM_API_URL = '/apis/XMLrequest'

XMLNS = 'http://www.netapp.com/filer/admin'
XMLNS_VERSION = "1.0"

if __name__ == '__main__':
    netapp.api._DEBUG = False
    server_host = os.environ['NETAPP_HOST']
    server_username = os.environ['NETAPP_USERNAME']
    server_password = os.environ['NETAPP_PASSWORD']

    s = netapp.api.Server(hostname=server_host, username=server_username,
                         password=server_password)

    event_iter = xml.event_iter(
        xml.timeout("4"),
        xml.greater_than_id("0"),
        xml.max_records("5")
    )


    for event in s._get_events(event_iter):
        print(event)
