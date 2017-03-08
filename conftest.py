import os
import base64

import betamax
from betamax_serializers import pretty_json

betamax.Betamax.register_serializer(pretty_json.PrettyJSONSerializer)

netapp_username = os.environ.get('NETAPP_USERNAME', "user-placeholder")
netapp_password = os.environ.get('NETAPP_PASSWORD', "password-placeholder")
ontap_username = os.environ.get('ONTAP_USERNAME', 'user-placeholder')
ontap_password = os.environ.get('ONTAP_PASSWORD', 'password-placeholder')

with betamax.Betamax.configure() as config:
    config.cassette_library_dir = 'netapp/tests/cassettes'
    config.default_cassette_options['serialize_with'] = 'prettyjson'
    config.define_cassette_placeholder('<OCUM-AUTH>',
                                       base64.b64encode(
                                           ('{0}:{1}'
                                            .format(netapp_username,
                                                    netapp_password)
                                            .encode('utf-8'))))
    # Replace the base64-encoded username:password string in the
    # basicauth headers with a placeholder to avoid exposing cleartext
    # passwords in checked-in content.
    config.define_cassette_placeholder('<ONTAP-AUTH>',
                                       base64.b64encode(
                                           ('{0}:{1}'
                                            .format(ontap_username,
                                                    ontap_password)
                                            .encode('utf-8'))))
