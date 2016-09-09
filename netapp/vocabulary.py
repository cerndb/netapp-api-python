import lxml.builder
import lxml.etree

E = lxml.builder.ElementMaker()


netapp = E.netapp
timeout = E.timeout

def value_element(tag_name, tag_value):
    elm = lxml.etree.Element(tag_name)
    elm.text = str(tag_value)
    return elm

def event_iter(*children):
    elm = lxml.etree.Element('event-iter')
    for child in children:
        elm.append(child)
    return elm

def greater_than_id(v):
    return value_element('greater-than-id', v)
