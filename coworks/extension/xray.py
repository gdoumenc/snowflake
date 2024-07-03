from __future__ import annotations

import logging
import os
import traceback
import typing as t
from functools import partial
from functools import update_wrapper

from aws_xray_sdk import global_sdk_config
# from aws_xray_sdk.core import patch_all
from aws_xray_sdk.core.exceptions.exceptions import SegmentNotFoundException
from aws_xray_sdk.core.recorder import TRACING_NAME_KEY
from aws_xray_sdk.ext.flask.middleware import XRayMiddleware

from coworks.globals import request
from coworks.wrappers import CoworksResponse

if t.TYPE_CHECKING:
    from coworks import TechMicroService
    from aws_xray_sdk.core import AWSXRayRecorder

LAMBDA_NAMESPACE = 'lambda'
REQUEST_NAMESPACE = 'flask'
COWORKS_NAMESPACE = 'coworks'


class XRay:
    def __init__(self, app: TechMicroService, recorder: AWSXRayRecorder, name="xray"):
        self._app = app
        self._recorder = recorder
        self._name = name

        if app is not None:
            self.init_app(app)

        # Bug in aws_xray_sdk : service must be defined or exception will be raised in execution if no dynamic_naming
        if not recorder.service:
            recorder.configure(service=COWORKS_NAMESPACE)

    def init_app(self, app):

        # Not available in flask client
        if os.getenv("FLASK_RUN_FROM_CLI"):
            return

        XRayMiddleware(app, self._recorder)
        app.logger.debug(f"Initializing xray extension {self._name}")

        if app.debug:
            logging.getLogger('aws_xray_sdk').setLevel(logging.DEBUG)

        if global_sdk_config.sdk_enabled():
            # Checks XRay is available
            try:
                self._recorder.current_segment()
            except SegmentNotFoundException:
                pass
            else:
                # Captures routes
                # patch_all() # Bug in aws_xray if engine is defined by flask_sqlalchemy
                app.errorhandler(500)(self.capture_exception)
                self.capture_routes()
                return

        self._app.logger.debug("Skipped capture routes because the SDK is currently disabled.")

    @staticmethod
    def capture(recorder):
        # Decorator to trace function calls on XRay.
        def xray_decorator(function):
            def function_captured(*args, **kwargs):
                subsegment = recorder.current_subsegment()
                if subsegment:
                    call_data = {
                        'args': args[1:],
                        'kwargs': kwargs,
                    }
                    subsegment.put_metadata(function.__name__, call_data, COWORKS_NAMESPACE)

                response = function(*args, **kwargs)

                if subsegment:
                    subsegment.put_metadata(f'{function.__name__}.response', response, COWORKS_NAMESPACE)

                return response

            if global_sdk_config.sdk_enabled():
                # Checks XRay is available
                try:
                    recorder.current_segment()
                except SegmentNotFoundException:
                    pass
                else:
                    # Captures function
                    wrapped_fun = update_wrapper(function_captured, function)
                    return recorder.capture(name=function.__name__)(wrapped_fun)

            return function

        return xray_decorator if os.getenv(TRACING_NAME_KEY) else lambda x: x

    def capture_routes(self):
        for rule in self._app.url_map.iter_rules():
            view_function = self._app.view_functions[rule.endpoint]

            def route_captured(_view_function, *args, **kwargs):
                aws_context = request.aws_context
                subsegment = self._recorder.current_subsegment()

                if subsegment:
                    try:
                        subsegment.put_metadata('context', lambda_context_to_json(aws_context), LAMBDA_NAMESPACE)
                        metadata = {
                            'service': self._app.name,
                            'environ': request_environ_to_json(request),
                        }
                        if request.is_json:
                            try:
                                metadata['json'] = request.json
                            except (Exception,):
                                metadata['data'] = request.get_data(cache=False, as_text=True)
                        elif request.is_multipart:
                            metadata['multipart'] = request.form.to_dict(False)
                            metadata['files'] = [*request.files.keys()]
                        elif request.is_form_urlencoded:
                            metadata['form'] = request.form.to_dict(False)
                            metadata['files'] = [*request.files.keys()]
                        else:
                            metadata['values'] = request.values.to_dict(False)
                        subsegment.put_metadata('request', metadata, COWORKS_NAMESPACE)
                    except (Exception,):
                        pass

                try:
                    response: CoworksResponse = _view_function(*args, **kwargs)
                except Exception:
                    if subsegment:
                        metadata = {'traceback': traceback.format_exc()}
                        subsegment.put_metadata('exception', metadata, COWORKS_NAMESPACE)
                    raise

                if subsegment:
                    try:
                        metadata = {
                            'status_code': response.status_code,
                            'headers': response.headers,
                        }
                        if response.status_code >= 300:
                            metadata['error'] = response.get_data(as_text=True)
                        subsegment.put_metadata('response', metadata, COWORKS_NAMESPACE)
                    except (Exception,):
                        pass

                return response

            wrapped_fun = update_wrapper(partial(route_captured, view_function), view_function)
            fun_name = getattr(wrapped_fun, '__name__', 'wrapped_fun')
            self._app.view_functions[rule.endpoint] = self._recorder.capture(name=fun_name)(wrapped_fun)

    def capture_exception(self, e):
        self._app.logger.error(f"Event: {request.aws_event}")
        self._app.logger.error(f"Context: {request.aws_context}")
        try:
            subsegment = self._recorder.current_subsegment()
            if subsegment:
                subsegment.add_error_flag()
                subsegment.put_annotation('service', self._app.name)
                subsegment.add_exception(e, traceback.extract_stack())
        except SegmentNotFoundException:
            self._app.logger.debug("Skipped capture exception because the SDK is currently disabled.")
        except Exception:
            raise


def lambda_context_to_json(context):
    return {
        'function_name': context.function_name,
        'function_version': context.function_version,
        'memory_limit_in_mb': context.memory_limit_in_mb,
        'aws_request_id': context.aws_request_id,
        'remaining_time': context.get_remaining_time_in_millis(),
    }


def request_environ_to_json(_request):
    return {
        'in_lambda_context': _request.in_lambda_context,
        'is_multipart': _request.is_multipart,
        'is_form_urlencoded': _request.is_form_urlencoded,
        'max_content_length': _request.max_content_length,
        'endpoint': _request.endpoint,
        'query_string': _request.args,
        'want_form_data_parsed': _request.want_form_data_parsed,
        'script_root': _request.script_root,
        'url_root': _request.url_root,
    }
