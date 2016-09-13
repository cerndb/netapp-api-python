import netapp.vocabulary as v

import lxml.etree


def test_complex_query():
    api_call = v.event_iter(
        v.event_id(17),
        v.event_severities(v.obj_status("CRITICAL"),
                           v.obj_status("ERROR"),
                           v.obj_status("WARNING")),
        v.event_state_filter_list(v.event_state("NEW"),
                                  v.event_state("OBSOLETE")),
        v.greater_than_id(17),
        v.max_records(2),
        v.tag("bork_bork"),
        v.time_range(v.event_timestamp_range(v.start_time(17),
                                             v.end_time(29))),
        v.timeout("16"))

    root_wrapper = v.netapp(api_call,
                            xmlns="ns-value",
                            version="1.0",
                            nmsdk_app="test-app")

    str_rep = lxml.etree.tostring(root_wrapper, pretty_print=True)

    print str_rep

    re_parsed_rep = lxml.etree.fromstring(str_rep)

    tag = re_parsed_rep.xpath("/a:netapp/a:event-iter/a:tag/text()",
                              namespaces={"a": "ns-value"})[0]

    assert tag == "bork_bork"

    timeout = re_parsed_rep.xpath("/a:netapp/a:event-iter/a:timeout/text()",
                                  namespaces={"a": "ns-value"})[0]

    assert timeout == "16"

    severities = re_parsed_rep.xpath(("/a:netapp/a:event-iter/"
                                     "a:event-severities/"
                                      "a:obj-status/text()"),
                                     namespaces={"a": "ns-value"})
    print severities

    assert "CRITICAL" in severities
    assert "WARNING" in severities
    assert "ERROR" in severities

    states = re_parsed_rep.xpath(("/a:netapp/a:event-iter/"
                                  "a:event-state-filter-list/"
                                  "a:event-state/text()"),
                                 namespaces={"a": "ns-value"})
    print states

    assert "NEW" in states
    assert "OBSOLETE" in states

    greater_than_id = re_parsed_rep.xpath(("/a:netapp/a:event-iter/"
                                           "a:greater-than-id/text()"),
                                          namespaces={"a": "ns-value"})[0]

    assert greater_than_id == "17"

    max_records = re_parsed_rep.xpath(("/a:netapp/a:event-iter/"
                                       "a:max-records/text()"),
                                      namespaces={"a": "ns-value"})[0]

    assert max_records == "2"

    time_start = re_parsed_rep.xpath(("/a:netapp/a:event-iter/"
                                      "a:time-range/a:event-timestamp-range/"
                                      "a:start-time/text()"),
                                     namespaces={"a": "ns-value"})[0]
    assert time_start == "17"

    time_end = re_parsed_rep.xpath(("/a:netapp/a:event-iter/"
                                    "a:time-range/a:event-timestamp-range/"
                                    "a:end-time/text()"),
                                   namespaces={"a": "ns-value"})[0]

    assert time_end == "29"
