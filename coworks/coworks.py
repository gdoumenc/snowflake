from __future__ import annotations

import base64
import inspect
import io
import itertools
import logging
import os
import sys
import traceback
import typing as t
from functools import partial
from functools import reduce
from pathlib import Path

import boto3
from flask import Blueprint as FlaskBlueprint
from flask import Flask
from flask import current_app
from flask import json
from flask.blueprints import BlueprintSetupState
from flask.testing import FlaskClient
from werkzeug.datastructures import ImmutableDict
from werkzeug.datastructures import MultiDict
from werkzeug.datastructures import WWWAuthenticate
from werkzeug.exceptions import Forbidden
from werkzeug.exceptions import Unauthorized
from werkzeug.routing import Rule

from .const import BIZ_BUCKET_HEADER_KEY
from .const import BIZ_KEY_HEADER_KEY
from .globals import request
from .proxy import HTTP_METHODS
from .proxy import create_cws_proxy
from .proxy import get_cws_annotations
from .proxy import is_arg_parameter
from .proxy import is_kwarg_parameter
from .proxy import make_absolute
from .proxy import path_join
from .proxy import trim_underscores
from .utils import get_app_stage
from .wrappers import CoworksMapAdapter
from .wrappers import CoworksRequest
from .wrappers import CoworksResponse

if t.TYPE_CHECKING:  # pragma: no cover
    from flask.app import App
else:
    WSGIEnvironment = t.Any
    StartResponse = t.Any


#
# Decorators
#


def entry(fun: t.Optional[t.Callable] = None, binary_headers: dict[str, str] | None = None,
          stage: str | t.Iterable[str] | None = None, no_auth: bool = False) -> t.Callable:
    """Decorator to create a microservice entry point from function name.

    :param fun: the entry function.
    :param binary_headers: force default content-type.
    :param stage: entry defined only for this stage(s).
    :param no_auth: set authorizer by default.
    """
    if fun is None:
        return partial(entry, binary_headers=binary_headers, stage=stage, no_auth=no_auth)

    def get_path(start):
        name_ = fun.__name__[start:]
        name_ = trim_underscores(name_)  # to allow several functions with d, Falseifferent args
        return name_.replace('_', '/')

    name = fun.__name__.upper()
    for method in HTTP_METHODS:
        if name == method:
            path = ''
            break
        if name.startswith(f'{method}_'):
            path = get_path(len(f'{method}_'))
            break
    else:
        method = 'POST'
        path = get_path(0)

    stage = [] if stage is None else stage

    fun.__CWS_METHOD = method  # type: ignore[attr-defined]
    fun.__CWS_PATH = path  # type: ignore[attr-defined]
    fun.__CWS_BINARY_HEADERS = binary_headers  # type: ignore[attr-defined]
    fun.__CWS_NO_AUTH = no_auth  # type: ignore[attr-defined]
    fun.__CWS_STAGES = stage if isinstance(stage, list) else [stage]  # type: ignore[attr-defined]

    return fun


#
# Classes
#

class CoworksClient(FlaskClient):
    """Creates environment and complete request.
    """

    def __init__(self, *args: t.Any, aws_event=None, aws_context=None, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

        headers: dict = aws_event['headers']
        request_context = aws_event['requestContext']

        method: str = request_context['httpMethod']
        scheme: str = headers.get('x-forwarded-proto', "https")
        host_name: str = headers['x-forwarded-host'] if 'x-forwarded-host' in headers else request_context['domainName']
        entry_path: str = request_context['entryPath']
        stage: str = request_context['stage']
        entry_path_parameters: dict = aws_event['entryPathParameters']
        query_string: dict = MultiDict(aws_event['multiValueQueryStringParameters'])

        is_encoded: bool = aws_event.get('isBase64Encoded', False)
        body = aws_event['body']
        if body and is_encoded:
            body = base64.b64decode(body)

        self.aws_environ = {
            "wsgi.input": io.BytesIO(b""),
            "wsgi.url_scheme": scheme,
            "REQUEST_SCHEME": scheme,
            "REQUEST_METHOD": method,
            "SERVER_NAME": host_name,
            "PATH_INFO": entry_path,

            'aws_event': aws_event,
            'aws_context': aws_context,
            'aws_stage': stage,
            "aws_entry_path": entry_path,
            "aws_entry_path_parameters": entry_path_parameters,
            'aws_query_string': query_string,
            'aws_body': body,
        }

    def open(self, *args, **kwargs) -> CoworksResponse:  # type: ignore[override]
        req = CoworksRequest(self.aws_environ, populate_request=False, shallow=True)
        return t.cast(CoworksResponse, super().open(req))

    def _copy_environ(self, other) -> dict:
        return {**other}


class Blueprint(FlaskBlueprint):
    """ Represents a blueprint, list of routes that will be added to microservice when registered.

    See :ref:`Blueprint <blueprint>` for more information.
    """

    def __init__(self, name: str | None = None, **kwargs):
        """Initialize a blueprint.

        :param kwargs: Other Flask blueprint parameters.
        """
        import_name = self.__class__.__name__.lower()
        super().__init__(name or import_name, import_name, **kwargs)

    def init_app(self, app):
        """".. deprecated:: 0.8.0"""
        ...

    def init_cli(self, app):
        """.. deprecated:: 0.8.0"""
        ...

    @property
    def logger(self) -> logging.Logger:
        return current_app.logger

    def make_setup_state(self, app: App, options: dict, *args) -> BlueprintSetupState:
        """Stores creation state for deferred initialization."""
        state = super().make_setup_state(app, options, *args)
        ms = t.cast(TechMicroService, app)

        # Defer blueprint route initialization.
        if not options.get('hide_routes', False):
            func = partial(TechMicroService.add_coworks_routes, t.cast(TechMicroService, state.app), state)
            ms.deferred_init_routes_functions = itertools.chain(ms.deferred_init_routes_functions, (func,))

        return state


class TechMicroService(Flask):
    """Simple tech microservice.

    See :ref:`tech` for more information.

    .. versionadded:: 0.8.0
       The `stage_prefixed` parameter was added.
    """

    # Maximume content length for writing response content in debug
    size_max_for_debug = 2000

    def __init__(self, name: str | None = None, stage_prefixed: bool = True, **kwargs) -> None:
        """ Initialize a technical microservice.
        :param name: Name used to identify the microservice.
        :param stage_prefixed: if accessed with stage or not.
        :param kwargs: Other Flask parameters.
        """
        self.default_config = ImmutableDict({
            **self.default_config,
        })

        name = name or self.__class__.__name__.lower()
        super().__init__(import_name=name, static_folder=None, **kwargs)

        self.request_class: type[CoworksRequest] = CoworksRequest
        self.response_class: type[CoworksResponse] = CoworksResponse

        self.deferred_init_routes_functions: t.Iterable[t.Callable] = []
        self._cws_app_initialized = False
        self._cws_conf_updated = False
        self.__aws_url_map: dict | None = None
        self.__stage_prefixed = stage_prefixed

        @self.before_request
        def before():
            self._check_token()

    def init_app(self):
        """Called to finalize the application initialization.
        Mainly to get external variables defined specifically for a workspace.

        .. deprecated:: 0.8.0
        """

    def init_cli(self):
        """Called only on cli command.
        Mainly externalized to allow specific import not needed on deployed implementation.

        .. deprecated:: 0.8.0
        """

    def app_context(self):
        """Override to initialize coworks microservice.
        """
        self._init_app()
        return super().app_context()

    def create_url_adapter(self, _request: CoworksRequest | None):  # type: ignore[override]
        if _request and _request.aws_event:
            self.subdomain_matching = True
            return CoworksMapAdapter(_request.environ, self.url_map, self.aws_url_map, self.__stage_prefixed)
        return super().create_url_adapter(_request)

    def cws_client(self, aws_event, aws_context):
        """CoWorks client used by the lambda call.
        """
        return CoworksClient(self, CoworksResponse, use_cookies=True, aws_event=aws_event, aws_context=aws_context)

    @property
    def header_autorizer_token(self):
        """The identity source for which authorization is requested usefull only on local.
        Usually defined in API GAateway Authorizer resource."""
        return request.headers.get('Authorization')

    @property
    def in_lambda_context(self):
        """Defined as a property to be read only."""
        return self._in_lambda_context

    @property
    def aws_url_map(self) -> dict[str, list[Rule]]:
        if self.__aws_url_map is None:
            self.__aws_url_map = {}
            for rule in self.url_map.iter_rules():
                entry_path = rule.rule.replace('<', '{').replace('>', '}')
                if entry_path in self.__aws_url_map:
                    self.__aws_url_map[entry_path].append(rule)
                else:
                    self.__aws_url_map[entry_path] = [rule]
        return self.__aws_url_map

    @property
    def routes(self) -> list[str]:
        """Returns the list of routes defined in the microservice.
        """
        return [k for k in self.aws_url_map]

    def token_authorizer(self, token: str) -> bool | str:
        """Defined the authorization process.

        If the returned value is False, all routes for all stages are denied.
        If the returned value is True, all routes for all stages are accepted.
        If the returned value is a string, then it must be a stage name and all routes are accepted for this stage.

        By default, just using the token authentification process (defined from terraform template).
        """

        if token is None:
            raise Unauthorized(www_authenticate=WWWAuthenticate(auth_type="basic"))

        return token == os.getenv('TOKEN')

    def auto_find_instance_path(self):
        """Instance path may be redefined by an environment variable.
        """
        instance_relative_path = os.getenv('INSTANCE_RELATIVE_PATH', '')
        path = Path(self.root_path) / instance_relative_path
        return path.as_posix()

    def store_response(self, resp: t.Union[dict, bytes], request_headers):
        """Store microservice response in S3 for biz task sequence.

        May be redefined for another storage in asynchronous call.
        """

        bucket = key = ''
        try:
            bucket = request_headers.get(self.config['X-CWS-S3Bucket'].lower())
            key = request_headers.get(self.config['X-CWS-S3Key'].lower())
            if bucket and key:
                content = json.dumps(resp).encode() if isinstance(resp, dict) else resp
                buffer = io.BytesIO(content)
                buffer.seek(0)
                aws_s3_session = boto3.session.Session()
                aws_s3_session.client('s3').upload_fileobj(buffer, bucket, key)
                self.logger.debug(f"Response stored in {bucket}/{key}")
            else:
                self.logger.debug("Cannot store response as no bucket or key")
        except Exception as e:
            self.logger.error(f"Exception when storing response in {bucket}/{key} : {str(e)}")

    def _init_app(self):
        """Finalize the app initialization.
        """
        if not self._cws_app_initialized:
            self._in_lambda_context = False
            self._update_config(in_lambda=False)
            self.add_coworks_routes()
            for fun in self.deferred_init_routes_functions:
                fun()

            if os.getenv("FLASK_RUN_FROM_CLI"):
                self.init_cli()

            self._cws_app_initialized = True

    def __call__(self, environ, start_response):
        """Main microservice entry point.
        """
        self._init_app()
        return self.wsgi_app(environ, start_response)

    def _update_config(self, *, in_lambda: bool):
        if not self._cws_conf_updated:
            # Set predefined environment variables
            self.config['X-CWS-S3Bucket'] = BIZ_BUCKET_HEADER_KEY
            self.config['X-CWS-S3Key'] = BIZ_KEY_HEADER_KEY

            self._cws_conf_updated = True

    def _check_token(self):
        # Get no_auth option for this entry
        no_auth = False
        if request.url_rule:
            view_function = self.view_functions.get(request.url_rule.endpoint, None)
            if view_function:
                no_auth = get_cws_annotations(view_function, '__CWS_NO_AUTH', False)

        # Checks token if authorization needed
        if not no_auth:
            try:
                token = self.header_autorizer_token
                valid = self.token_authorizer(token)
                if not valid:
                    raise Forbidden()
            except Exception as e:
                current_app.logger.exception(e)
                raise Forbidden()

    def full_logger_error(self, error):
        self.logger.error(f"Exception in {self.name} : {error}")
        parts = ["Traceback (most recent call last):\n"]
        parts.extend(traceback.format_stack(limit=15)[:-2])
        parts.extend(traceback.format_exception(*sys.exc_info())[1:])
        trace = reduce(lambda x, y: x + y, parts, "")
        self.logger.error(f"Traceback: {trace}")

    def add_coworks_routes(self, bp_state: BlueprintSetupState | None = None) -> None:
        """ Creates all routes for a microservice.

        :param bp_state: The blueprint state.
        """

        # Adds entrypoints
        stage = get_app_stage()
        scaffold = bp_state.blueprint if bp_state else self
        method_members = inspect.getmembers(scaffold.__class__, lambda x: inspect.isfunction(x))
        methods = [fun for _, fun in method_members if get_cws_annotations(fun, '__CWS_METHOD')]
        for fun in methods:

            # the entry is not defined for this stage
            stages = get_cws_annotations(fun, '__CWS_STAGES')
            if stages and stage not in stages:
                continue

            method = get_cws_annotations(fun, '__CWS_METHOD')
            entry_path = path_join(get_cws_annotations(fun, '__CWS_PATH'))

            # Get parameters
            sig = inspect.signature(fun)
            param_names = list(sig.parameters.keys())[1:]  # skip self parameter
            generic_kwargs = None
            if len(param_names) > 0:
                last_param_name = param_names[-1]
                last_param = sig.parameters[param_names[-1]]
                if last_param.kind == last_param.VAR_KEYWORD:
                    generic_kwargs = last_param_name
                    param_names = param_names[:-1]
            args = [n for n in param_names if is_arg_parameter(sig.parameters[n])]
            for index, arg in enumerate(args):
                entry_path = path_join(entry_path, f"/<{arg}>")
            kwargs = {n: sig.parameters[n] for n in param_names if is_kwarg_parameter(sig.parameters[n])}

            proxy = create_cws_proxy(scaffold, fun, args, kwargs, generic_kwargs)
            proxy.__CWS_BINARY_HEADERS = get_cws_annotations(fun, '__CWS_BINARY_HEADERS')
            proxy.__CWS_NO_AUTH = get_cws_annotations(fun, '__CWS_NO_AUTH')
            fun.__CWS_FROM_BLUEPRINT = bp_state.blueprint.name if bp_state else None

            prefix = f"{bp_state.blueprint.name}." if bp_state else ''
            endpoint = f"{prefix}{fun.__name__}"

            # Creates the entry
            url_prefix: str = bp_state.url_prefix if bp_state else ''
            rule = make_absolute(entry_path, url_prefix)
            for r in self.url_map.iter_rules():
                if r.rule == rule and method in (r.methods or []):
                    raise AssertionError(f"Duplicate route {rule}")
            try:
                self.add_url_rule(rule=rule, view_func=proxy, methods=[method], endpoint=endpoint, strict_slashes=False)
            except AssertionError:
                raise
