import io
import json
from unittest.mock import MagicMock

import pytest
import requests

from coworks import TechMicroService
from coworks import utils
from coworks.cli.sfn import TechState


class TechMS(TechMicroService):
    def __init__(self):
        super().__init__(app_name='test')

    def post_params(self, text=None, context=None, files=None):
        return f"post {text}, {context} and {[f.file.name for f in files]}"


client = MagicMock()
utils.BotoSession.client = MagicMock(return_value=client)
s3_object = {'Body': io.BytesIO(b'test'), 'ContentType': 'text/plain'}
client.get_object = MagicMock(return_value=s3_object)


@pytest.mark.wip
def test_arg_params(local_server_factory):
    # normal API call
    local_server = local_server_factory(TechMS())
    data = {'key': 'value'}
    multiple_files = [
        ('text', (None, "hello world")),
        ('context', (None, json.dumps(data), 'application/json')),
        ('files', ('f1.csv', 'some,data,to,send\nanother,row,to,send\n')),
        ('files', ('f2.txt', 'some,data,to,send\nanother,row,to,send\n', 'text/plain')),
        ('files', ('f3.j2', 'bucket/key', 'text/s3')),
    ]
    response = local_server.make_call(requests.post, '/params', files=multiple_files, json=data, timeout=500)
    assert response.status_code == 200
    assert response.text == "post hello world, {'key': 'value'} and ['f1.csv', 'f2.txt', 'f3.j2']"

    # step function call
    form_data = {
        'text': {'content': "hello world"},
        'context': {'content': {'key': 'value'}, 'mime_type': 'application/json'},
        'files': [
            {'filename': 'f1.csv', 'content': 'some,data', 'mime_type': 'text/plain'},
            {'filename': 'f2.txt', 'content': 'content2', 'mime_type': 'text/plain'},
            {'filename': 'f3.j2', 'path': 'bucket/key', 'mime_type': 'text/s3'},
        ],
    }
    data = {'post': '/params', 'form-data': form_data}
    call = TechState.get_call_data(None, data)
    response = TechMS()(call, {})
    assert response['statusCode'] == 200
    assert response['body'] == "post hello world, {'key': 'value'} and ['f1.csv', 'f2.txt', 'f3.j2']"
