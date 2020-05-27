import os

import requests

from tests.src.coworks.tech_ms import *

EXAMPLE_DIR = os.getenv('EXAMPLE_DIR')


class WithEnvMS(SimpleMS):

    def get(self):
        """Root access."""
        return os.getenv("test")


class TestClass:

    def test_default(self, local_server_factory):
        local_server = local_server_factory(WithEnvMS(), config_path=EXAMPLE_DIR)
        response = local_server.make_call(requests.get, '/')
        assert response.status_code == 200
        assert response.text == 'test environment variable'

    def test_dev_stage(self, local_server_factory):
        local_server = local_server_factory(WithEnvMS(), config_path=EXAMPLE_DIR, stage="dev")
        response = local_server.make_call(requests.get, '/')
        assert response.status_code == 200
        assert response.text == 'test environment variable'

    def test_prod_stage(self, local_server_factory):
        local_server = local_server_factory(WithEnvMS(), config_path=EXAMPLE_DIR, stage="master")
        response = local_server.make_call(requests.get, '/')
        assert response.status_code == 200
        assert response.text == 'prod variable'
