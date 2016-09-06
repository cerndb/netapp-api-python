import sys

# FIXME: this is really, really, really ugly
sys.path.append("./lib/NetApp/")

from NaServer import *

ONTAP_MAJORVERSION = 1
ONTAP_MINORVERSION = 0

APP_NAME = "test!!"

class Server():

    class EventLog():
        """
        An iterator of all the storage engine's events. Will (by
        default) iterate over _all_ events, but may optionally filter
        out events by ID.
        """

        def __iter__(self):
            events = self.server.invoke('event-iter')
            if events.results_status() == "failed":
                raise Exception("Error: " + events.results_reason())
            else:
                return events.__iter__()

        def above_id(self, id):
            """
            Return an Iterator over the current event log, but only
            containing events with ID:s higher than id.
            """

            pass

        def by_id(self, id):
            """
            Return a specific event with a given ID. Raises an error if
            no such event exists.
            """

            pass

        def __init__(self, server):
            self.server = server.server


    @property
    def events(self):
        return Server.EventLog(self)

    def __init__(self, hostname, port=443, transport_type="HTTPS",
                 server_type="OCUM"):
        self.server = NaServer(hostname, ONTAP_MAJORVERSION,
                               ONTAP_MINORVERSION)

        self.server.set_style("LOGIN")
        self.server.set_transport_type(transport_type)
        self.server.set_server_type(server_type)
        self.server.set_port(port)

        # FIXME: this is a workaround. DO NOT LEAVE ON!
        self.server.set_server_cert_verification(False)

        self.server.set_application_name(APP_NAME)


class Event():
    pass
