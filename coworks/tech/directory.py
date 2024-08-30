import base64
import datetime
import os
import re
import secrets
import string
import typing as t

import boto3
import requests
from Crypto.Cipher import AES
from flask import json
from flask import current_app, request
from werkzeug.exceptions import BadRequest
from werkzeug.exceptions import Forbidden
from werkzeug.exceptions import MethodNotAllowed
from werkzeug.exceptions import NotFound

from coworks import TechMicroService
from coworks import entry
from utils import is_json


class DirectoryMicroService(TechMicroService):
    DOC_MD = """
## Directory service

Microservice to get information on a deployed CoWorks TechMicroServices.

You can also call a microservice by its name or get a code for a temporary authorized call.

See 'samples/directory' to get how to create and deploy it.
"""

    def __init__(self, name="directory", session=None, access_key_var="AWS_USER_ACCESS_KEY_ID",
                 secret_access_key_var="AWS_USER_SECRET_ACCESS_KEY", region_name_var="AWS_REGION_NAME", **kwargs):
        super().__init__(name=name, **kwargs)

        if session is None:
            access_key = os.getenv(access_key_var)
            secret_key = os.getenv(secret_access_key_var)
            region_name = os.getenv(region_name_var)
            session = boto3.Session(access_key, secret_key, region_name=region_name)
        self.api_client = session.client('apigateway')
        self.lambda_client = session.client('lambda')

    @entry
    def post(self, pattern=None, position=None):
        """Get the list of microservices matching a pattern.

        :param pattern: AWS name pattern (ex. ".*directory.*").
        :param position: position for pagination.
        """
        try:
            match_re = re.compile(pattern) if pattern else None
        except re.error as e:
            raise BadRequest(f"Wrong regular expression: {e.msg}")

        def found(a):
            return match_re.search(a['name']) if match_re else True

        boto3_args = {'limit': 100}
        if position:
            boto3_args.update({'position': position})
        try:
            objects = self.api_client.get_rest_apis(**boto3_args)
        except (Exception,) as e:
            raise BadRequest(e.msg)
        api = [*filter(found, objects['items'])]
        position = self._get_position(objects)
        while position and len(api) < 500:
            objects = self.api_client.get_rest_apis(position=position, limit=100)
            api = [*api, *filter(found, objects['items'])]
            position = self._get_position(objects)

        return {
            'items': {a['name']: a['id'] for a in api},
            'position': position if position else ''
        }

    @entry
    def get_aws(self, name):
        """Get AWS information on a microservice.

        :param name: AWS microservice's name.
        """

        def found(apis):
            for a in apis:
                if a['name'] == name:
                    return a

        objects = self.api_client.get_rest_apis(limit=100)
        api = found(objects['items'])
        if api:
            return api
        position = objects['position']
        while position:
            objects = self.api_client.get_rest_apis(position=position, limit=100)
            api = found(objects['items'])
            if api:
                return api
            position = objects['position'] if 'position' in objects else None

        raise NotFound()

    @entry
    def get_stages(self, name):
        """Get microservice's stages for the microservice.

        :param name: AWS microservice's name.
        """
        api = self.get_aws(name)
        api_id = api['id']
        stages = self.api_client.get_stages(restApiId=api_id)
        return {s['stageName']: {**s, 'api_id': api_id} for s in stages['item']}

    @entry
    def get_url(self, name, stage=None, token_var_name='TOKEN'):
        """Get microservice url for a microservice.
        If stage is not defined the latest production version is choosen or 'dev' if no production version.

        :param name: AWS microservice's name.
        :param stage: AWS microservice's stage.
        :param token_var_name: lambda environment variable's name for the token.
        """
        api_id = ""
        stages = self.get_stages(name)
        if stage:
            if stage not in stages:
                raise NotFound()
        else:
            stage = sorted(stages)[-1]
        api_id = stages[stage]['api_id']

        auths = self.api_client.get_authorizers(restApiId=api_id)
        lambda_uri = auths['items'][0]['authorizerUri']
        uris = lambda_uri.split(':')
        lambda_name = f"{uris[-1].split('$')[0]}{stage}"
        lambda_fun = self.lambda_client.get_function(FunctionName=lambda_name)

        url = f"https://{api_id}.execute-api.eu-west-1.amazonaws.com/{stage}"
        token = lambda_fun['Configuration']['Environment']['Variables'].get(token_var_name)
        return {'url': url, 'token': token}

    @entry
    def get_doc(self, name, stage=None):
        """Get microservice documentation.
        If stage is not defined the latest production version is choosen or 'dev' if no production version.

        :param name: AWS microservice's name.
        :param stage: AWS microservice's stage.
        """
        info = self.get_url(name, stage=stage)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'text/html; charset utf-8',
        }
        url = f"{info['url']}/admin"
        res = requests.get(url, headers=headers)
        return res.text, res.status_code, dict(res.headers)

    @entry
    def get_name(self, api_id):
        """Get microservice name from api id.

        :param api_id: AWS API ID.
        """
        return self.api_client.get_rest_api(restApiId=api_id)

    @entry
    def post_call(self, name, stage=None, path='/admin', method='get', token_var_name='TOKEN', data=None):
        """Call a microservice from its name.

        Accept and Content-Type may be defined in header for the microservice call.

        :param name: microservice's name.
        :param stage: stage version.
        If stage is not defined the latest production version is choosen or 'dev' if no production version.
        :param path: entry path (default '/admin').
        :param method: method called (default 'get').
        :param token_var_name: token variable name (default 'TOKEN').
        :param data: query parameters or json body (default {}).
        """
        data = data or {}
        info = self.get_url(name, stage=stage, token_var_name=token_var_name)

        headers = {
            'Accept': request.headers.get('Accept', 'application/json'),
            'Authorization': info['token'],
            'Content-Type': request.headers.get('Content-Type', 'application/json'),
        }

        url = f"{info['url']}{path}"

        current_app.logger.info(f"{method.upper()} on {url}, with {data}")
        if method.upper() == 'GET':
            res = requests.get(url, params=data, headers=headers)
        elif method.upper() == 'POST':
            res = requests.post(url, json=data, headers=headers)
        else:
            raise MethodNotAllowed()

        if not res.ok:
            return res.text, res.status_code

        accept = headers.get('Accept', 'application/json')
        return res.json() if is_json(accept) else res.text, res.status_code, dict(res.headers)

    @entry
    def post_code(self, name, duration=60, **kwargs):
        """Returns a temporary code to call a microservice.

        :param name: microservice's name.
        :param duration: token duration in minutes.
        :param kwargs: call parameters.
        """
        kwargs['name'] = name

        # Adds expiration time
        expiration_time = datetime.datetime.now() + datetime.timedelta(minutes=duration)
        kwargs['expiration_time'] = expiration_time.strftime('%Y-%m-%dT%H:%M:%S%z')

        # Encodes invocation data
        iv = ''.join((secrets.choice(string.ascii_letters) for i in range(AES.block_size)))
        crypto_obj = AES.new(self._pad(os.getenv("TOKEN")).encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
        cipher = crypto_obj.encrypt(self._pad(json.dumps(kwargs), ' ').encode('utf-8'))
        return iv + base64.b64encode(cipher).decode()

    @entry
    def post_decode(self, code=None):
        """Call the microservice defined by the code with GET method."""
        return self._decode(code=code)

    @entry(no_auth=True)
    def post_code_call(self, code=None, data=None):
        """Call the microservice defined by the code with POST method."""
        return self._call(code=code, data=data)

    def _decode(self, code=None):
        data = {}

        # Decodes invocation data
        iv = code[:AES.block_size]
        code = base64.b64decode(code[AES.block_size:])
        crypto_obj = AES.new(self._pad(os.getenv("TOKEN")).encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
        cipher = crypto_obj.decrypt(code).decode('ascii')
        call_data = json.loads(cipher)
        current_app.logger.debug(call_data)

        # Checks expiration time
        expiration_time = datetime.datetime.strptime(call_data.pop('expiration_time'), '%Y-%m-%dT%H:%M:%S')
        now = datetime.datetime.now()
        if now > expiration_time:
            current_app.logger.debug(f"Expired call : {now} > {expiration_time}")
            raise Forbidden("Your link is no more available.")

        # Check path prefix
        if 'path' in call_data and 'path' in data:
            if call_data['path'] not in data['path']:
                current_app.logger.debug(f"{call_data['path']} not in {data.get('path')}")
                raise Forbidden("You cannot access this entry.")

        # Checks mandatory parameters
        return call_data

    def _call(self, code=None, data=None):
        data = data or {}
        if 'name' in data:
            raise BadRequest("Cannot overload name parameters")

        final_data = {**data, **self._decode(code)}
        if 'path' not in final_data:
            raise BadRequest("Missing path parameters")

        # Invokes microservice entry
        return self.post_call(**final_data)

    def _get_position(self, objects):
        return objects['position'] if 'position' in objects else None

    def _pad(self, str: str, padding_char: t.Optional[str] = None) -> str:
        """Pads the string to AES.block_size (pad with padding_char)"""
        padding = (AES.block_size - (len(str) % AES.block_size))
        return str + padding * (padding_char if padding_char is not None else chr(padding))
