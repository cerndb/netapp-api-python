import sys

# FIXME: this is really, really, really ugly
sys.path.append("./lib/NetApp/")

import NaServer, NaElement, DfmErrno, NaErrno


class Server():

    class EventLog():
        """
        An iterator of all the storage engine's events. Will (by
        default) iterate over _all_ events, but may optionally filter
        out events by ID.
        """

        def __iter__(self):
            pass

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

    @property
    def events(self):
        return EventLog(self)

    @property.setter
    def events(self):
        raise Exception("Can't set the event log!")



class Event():
    pass
