
import os
from unittest import mock
from unittest.mock import Mock

import click

from coworks.cws.client import client


class TestClass:

    def test_default_command(self):
        assert 'run' in client.commands
        assert 'shell' in client.commands
        assert 'routes' in client.commands

    @mock.patch.dict(os.environ, {"FLASK_APP": "complete:app"})
    def test_routes_command(self, monkeypatch, samples_docs_dir):
        mclick = Mock()
        monkeypatch.setattr(click, "echo", mclick)
        client.main(['routes'], 'cws', standalone_mode=False)
        mclick.assert_called()
        assert len(mclick.mock_calls) == 8
        out = [call.args[0].split(' ')[0] for call in mclick.mock_calls]
        assert 'Endpoint' in out
        assert 'admin.get_route' in out
        assert 'get' in out
        assert 'post' in out
