import os
from urllib.parse import urljoin

import requests


class MicroServiceProxy:

    def __init__(self, env_name, config=None, **kwargs):
        self.session = requests.Session()

        if config is not None:
            if hasattr(config, 'get'):
                self.cws_id = config.get(f'{env_name}_CWS_ID')
                self.cws_token = config.get(f'{env_name}_CWS_TOKEN')
                self.cws_stage = config.get(f'{env_name}_CWS_STAGE', 'dev')
            else:
                self.cws_id = getattr(config, f'{env_name}_CWS_ID')
                self.cws_token = getattr(config, f'{env_name}_CWS_TOKEN')
                self.cws_stage = getattr(config, f'{env_name}_CWS_STAGE', 'dev')
        else:
            self.cws_id = os.getenv(f'{env_name}_CWS_ID')
            self.cws_token = os.getenv(f'{env_name}_CWS_TOKEN')
            self.cws_stage = os.getenv(f'{env_name}_CWS_STAGE', 'dev')

        if not self.cws_id:
            raise EnvironmentError(f"Environment variable {env_name}_CWS_ID not defined")
        if not self.cws_token:
            raise EnvironmentError(f"Environment variable {env_name}_CWS_TOKEN not defined")

        self.session.headers.update({
            'authorization': self.cws_token,
            'content-type': 'application/json',
        })
        self.url = f"https://{self.cws_id}.execute-api.eu-west-1.amazonaws.com/{self.cws_stage}/"

    def get(self, path, data=None, response_content_type='json'):
        if path.startswith('/'):
            path = path[1:]
        resp = self.session.get(urljoin(self.url, path), data=data)
        return self.convert(resp, response_content_type)

    # noinspection PyShadowingNames
    def post(self, path, data=None, json=None, headers=None, sync=True, response_content_type='json'):
        if path.startswith('/'):
            path = path[1:]
        headers = {**self.session.headers, **headers} if headers else self.session.headers
        if not sync:
            headers.update({'InvocationType': 'Event'})
        resp = self.session.post(urljoin(self.url, path), data=data, json=json or {}, headers=headers)
        return self.convert(resp, response_content_type)

    @property
    def routes(self):
        return self.get('/admin/route')

    @staticmethod
    def convert(resp, response_content_type):
        if resp.status_code == 200:
            if response_content_type == 'text':
                content = resp.text
            elif response_content_type == 'json':
                content = resp.json()
            else:
                content = resp.content
        else:
            content = resp.reason
        return content, resp.status_code
