from __future__ import annotations

import inspect
import os
import re
import types
import typing as t
from functools import update_wrapper
from inspect import Parameter
from inspect import Signature
from inspect import signature
from pathlib import PurePosixPath
from urllib.parse import parse_qs
from urllib.parse import urlencode
from urllib.parse import urlsplit as urllib_urlsplit
from urllib.parse import urlunsplit as urllib_urlunsplit

from flask import current_app
from flask import json
from flask import make_response
from pydantic import BaseModel
from pydantic import ValidationError
from werkzeug.exceptions import UnprocessableEntity

import dotenv
from .globals import request

if t.TYPE_CHECKING:
    from flask.sansio.scaffold import Scaffold

HTTP_METHODS = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS']

PROJECT_CONFIG_VERSION = 3

DEFAULT_DEV_STAGE = "dev"
DEFAULT_LOCAL_STAGE = "local"
DEFAULT_PROJECT_DIR = "tech"

BIZ_BUCKET_HEADER_KEY: str = 'X-CWS-S3Bucket'
BIZ_KEY_HEADER_KEY: str = 'X-CWS-S3Key'

OPEN_SQUARE_BRACKETED_KWARG_PATTERN = re.compile(r'([a-zA-Z0-9]+)__')
SQUARE_BRACKETED_KWARG_PATTERN = re.compile(r'([a-zA-Z0-9]+)__([a-zA-Z0-9._]+)__')


def create_cws_proxy(scaffold: Scaffold, func, func_args: list[str], func_kwargs: dict,
                     func_generic_kwargs: str | None):
    """Creates the AWS Lambda proxy function.

    :param scaffold: The Flask or Blueprint object.
    :param func: The initial function proxied.
    :param func_args: The declared function args.
    :param func_kwargs: The declared function kwargs.
    :param func_generic_kwargs: The function generic kwargs if defined (usually **kwargs).
    """

    def proxy(**view_args):
        """
        Adds kwargs parameters to the proxied function.

        :param view_args: Request path parameters.
        """

        def check_keyword_expected(param_name):
            """Alerts when more parameters than expected are defined in request."""
            if func_kwargs and param_name not in func_kwargs:
                _err_msg = f"TypeError: got an unexpected keyword argument '{param_name}'"
                raise UnprocessableEntity(_err_msg)

        def as_fun_params(values: dict, flat=True):
            """Set parameters as simple value or list of values if multiple defined.
           :param values: Dict of values.
           :param flat: If true, the list values of lenth 1 is return as single value.
            """
            params: dict[str, t.Any] = {}
            for k, v in values.items():
                k = remove_brackets(k)

                # if the parameter is a sparse fieldsets
                if splitted := SQUARE_BRACKETED_KWARG_PATTERN.fullmatch(k):
                    for kwarg in func_kwargs:
                        if k.startswith(kwarg) and OPEN_SQUARE_BRACKETED_KWARG_PATTERN.fullmatch(kwarg):
                            k = kwarg
                            if k in params:
                                v = {**params[k], **{splitted.group(2): v}}
                            else:
                                v = {splitted.group(2): v}
                            break

                try:
                    check_keyword_expected(k)
                except UnprocessableEntity:
                    if not func_generic_kwargs:
                        raise
                params[k] = v

            # Flatten single value
            if flat:
                params = {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in params.items()}

            return params

        # Get keyword arguments from request parameters or body
        if func_kwargs or func_generic_kwargs:

            # Adds parameters from query parameters
            if request.method == 'GET':
                get_data = request.values.to_dict(False)
                view_args = dict(**view_args, **as_fun_params(get_data))

            # Adds parameters from body
            elif request.method in ['POST', 'PUT', 'PATCH', 'DELETE']:
                try:
                    if request.is_json:
                        if request.data:
                            post_data = request.json
                            if not isinstance(post_data, dict):
                                if len(func_kwargs) != 1:
                                    msg = ("If request payload is not a dict, "
                                           f"there must be only one kwarg {type(post_data)}")
                                    raise UnprocessableEntity(msg)
                                post_data = {next(iter(func_kwargs)): post_data}
                            view_args = {**view_args, **as_fun_params(post_data, False)}
                    elif request.is_multipart:
                        post_data = request.form.to_dict(False)
                        files = request.files.to_dict(False)
                        view_args = {**view_args, **as_fun_params(post_data), **as_fun_params(files)}
                    elif request.is_form_urlencoded:
                        post_data = request.form.to_dict(False)
                        view_args = dict(**view_args, **as_fun_params(post_data))
                    else:
                        post_data = request.values.to_dict(False)
                        view_args = dict(**view_args, **as_fun_params(post_data))
                except Exception as e:
                    raise UnprocessableEntity(str(e))

            else:
                err_msg = f"Keyword arguments are not permitted for {request.method} method."
                raise UnprocessableEntity(err_msg)

        else:
            if not func_args:
                try:
                    if request.content_length:
                        if request.is_json and request.json:
                            err_msg = f"TypeError: got an unexpected arguments (body: {request.json})"
                            raise UnprocessableEntity(err_msg)
                    if request.query_string:
                        err_msg = f"TypeError: got an unexpected arguments (query: {request.query_string!r})"
                        raise UnprocessableEntity(err_msg)
                except Exception as e:
                    current_app.logger.error(f"Should not go here (1) : {str(e)}")
                    current_app.logger.error(f"Should not go here (2) : {request.get_data()}")
                    current_app.logger.error(f"Should not go here (3) : {view_args}")
                    raise

        view_args = as_typed_kwargs(func, view_args)
        result = current_app.ensure_sync(func)(scaffold, **view_args)

        resp = make_response(result) if result is not None else \
            make_response("", 204, {'content-type': 'text/plain'})

        binary_headers = get_cws_annotations(func, "__CWS_BINARY_HEADERS")
        if binary_headers and not request.in_lambda_context:
            resp.headers.update(binary_headers)

        return resp

    return update_wrapper(proxy, func)


def path_join(*args: str) -> str:
    """ Joins given arguments into an entry route.
    Slashes are stripped for each argument.
    """

    reduced = (x.lstrip('/').rstrip('/') for x in args if x)
    return str(PurePosixPath('/').joinpath(*reduced))[1:]


def make_absolute(route: str, url_prefix: str) -> str:
    """Creates an absolute route.
    """
    path = PurePosixPath('/')
    if url_prefix:
        path = path / url_prefix
    if route:
        path = path / route
    return str(path)


def trim_underscores(name: str) -> str:
    """Removes starting and ending _ in name.
    """
    if name:
        while name.startswith('_'):
            name = name[1:]
        while name.endswith('_'):
            name = name[:-1]
    return name


def is_arg_parameter(param: Parameter) -> bool:
    """ Checks if the parameter is an arg (not a kwarg)."""
    return param.default == inspect.Parameter.empty


def is_kwarg_parameter(param: Parameter) -> bool:
    """ Checks if the parameter is an arg (not a kwarg)."""
    return param.default != inspect.Parameter.empty


def remove_brackets(name):
    """Removes brackets.
    Parameter like page[number] is passed to the function as page__number__."""
    return name.replace('[', '__').replace(']', '__')


def as_typed_kwargs(func: t.Callable, kwargs: dict):
    def get_typed_value(name: str, parameter_type, val):
        if isinstance(parameter_type, types.UnionType):
            for arg in t.get_args(parameter_type):
                try:
                    return get_typed_value(name, arg, val)
                except (UnprocessableEntity, ValidationError):
                    raise
                except (TypeError, ValueError):
                    pass
            raise TypeError()
        origin = t.get_origin(parameter_type)
        if origin is t.Union:
            for arg in t.get_args(parameter_type):
                return get_typed_value(name, arg, val)
            raise TypeError()
        if origin is list:
            arg = t.get_args(parameter_type)[0]
            if isinstance(val, list):
                return [arg(v) for v in val]
            return [arg(val)]
        if origin is set:
            arg = t.get_args(parameter_type)[0]
            if isinstance(val, list):
                return {arg(v) for v in val}
            return {arg(val)}
        if origin is None:
            if val is None or not parameter_type or parameter_type is Signature.empty:
                return val
            if isinstance(val, list):
                msg = f"Multiple values for '{name}' query parameters are not allowed"
                raise UnprocessableEntity(msg)
            if issubclass(parameter_type, bool):
                return str_to_bool(val)
            if issubclass(parameter_type, dict):
                if isinstance(val, str):
                    return json.loads(val)
            if issubclass(parameter_type, BaseModel):
                return parameter_type(**json.loads(val))
            return val if isinstance(val, parameter_type) else parameter_type(val)

    typed_kwargs = {**kwargs}
    try:
        parameters = signature(func).parameters
        for name, value in kwargs.items():
            typed_kwargs[name] = get_typed_value(name, t.cast(Parameter, parameters.get(name)).annotation, value)
    except (UnprocessableEntity, ValidationError):
        raise
    except (TypeError, ValueError) as e:
        raise UnprocessableEntity(str(e))
    except (Exception,):
        pass
    return typed_kwargs


def is_json(mt):
    """Checks if a mime type is json.
    """
    return (
            mt == "application/json"
            or isinstance(mt, str)
            and mt.startswith("application/")
            and mt.endswith("+json")
    )


def str_to_bool(val: str) -> bool:
    return val.lower() in ['true', '1', 'yes']


def get_app_stage():
    """Defined only on deployed microservice or should be set manually."""
    return os.getenv('CWS_STAGE', DEFAULT_DEV_STAGE)


def load_dotenv(stage: str):
    values = {}
    for env_filename in get_env_filenames(stage):
        path = dotenv.find_dotenv(env_filename, usecwd=True)
        if path:
            values.update(dotenv.dotenv_values(path))
    return values


def get_env_filenames(stage):
    return [".env", ".flaskenv", f".env.{stage}", f".flaskenv.{stage}"]


def nr_url(path: str = '', query: dict | None = None, merge_query: bool = False):
    """Combines the arguments into a complete URL as a string.

    :param path: the new path .
    :param query: the query parameters.
    :param merge_query: adds the request query parameters.
    """
    if request.aws_event:
        header = request.aws_event['params']['header']
        if 'forwarded' in header:
            forwarded = {val.split('=')[0]: val.split('=')[1] for val in header['forwarded'].split(';')}
            proto = forwarded['proto']
            host = forwarded['host']
            path = header['x-forwarded-path'] + path
        else:
            proto = header.get('x-forwarded-proto', 'https')
            host = header['host']
    else:
        proto, host, *_ = urllib_urlsplit(request.base_url)

    if merge_query and query:
        if request.aws_event:
            query = {**request.aws_event['params']['querystring'], **query}
        else:
            *_, request_query, _ = urllib_urlsplit(request.url)
            query = {**parse_qs(request_query), **query}

    qs = urlencode(query, doseq=True) if query else ''
    return urllib_urlunsplit([proto, host, path, qs, ''])


def get_cws_annotations(func, key, default=None):
    """Entry function is at least annotated by __CWS_METHOD."""
    if getattr(func, '__CWS_METHOD', None):
        return getattr(func, key, default)
    if getattr(func, '__wrapper__', None):
        return get_cws_annotations(func.__wrapper__, key, default)
    return None
