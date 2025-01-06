from unittest import mock
from unittest.mock import MagicMock

import pytest

from coworks import TechMicroService
from coworks import entry
from coworks.const import BIZ_BUCKET_HEADER_KEY
from coworks.const import BIZ_KEY_HEADER_KEY
from ..event import get_event


class AsyncMS(TechMicroService):

    @entry
    def get(self):
        return "ok"


@pytest.mark.skip
class TestClass:
    def test_async_store(self, empty_aws_context):
        with mock.patch('boto3.session.Session') as session:
            session.side_effect = lambda: session
            session.client = MagicMock(side_effect=lambda _: session.client)
            app = AsyncMS()
            event = get_event('/', 'get')
            with app.cws_client(event, empty_aws_context) as c:
                event['headers']['InvocationType'.lower()] = 'Event'
                event['headers'][BIZ_BUCKET_HEADER_KEY.lower()] = 'bucket'
                event['headers'][BIZ_KEY_HEADER_KEY.lower()] = 'specific/key'
                app(event, empty_aws_context)
                session.assert_called_once()
                session.client.assert_called_once_with('s3')
                session.client.upload_fileobj.assert_called_once()
