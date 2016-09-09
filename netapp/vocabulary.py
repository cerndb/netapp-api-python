"""
This is an XML vocabulary for generating queries against NetApp's API.
"""

import lxml.builder
import lxml.etree

E = lxml.builder.ElementMaker()


netapp = E.netapp
"The root element for any query."

timeout = E.timeout
"The timeout for an operation: <timeout>x</timeout>."

def _value_element(tag_name, tag_value):
    """
    Helper function: generate <tag_name>tag_value</tag_name>.
    """
    elm = lxml.etree.Element(tag_name)
    elm.text = str(tag_value)
    return elm

def event_iter(*children):
    """
    Generate an <event-iter>[children]</event-iter> tag.
    """
    elm = lxml.etree.Element('event-iter')
    for child in children:
        elm.append(child)
    return elm

def greater_than_id(id):
    """
    Generate an <greater-than-id>id</greater-than-id> tag.
    """
    return _value_element('greater-than-id', id)
