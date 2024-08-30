import base64
import io
import re
import typing as t
import urllib
import urllib.parse
from io import BytesIO

from flask import Request as FlaskRequest
from flask import Response as FlaskResponse
from flask import current_app
from flask import json
from requests_toolbelt.multipart.decoder import MultipartDecoder
from werkzeug.datastructures import ETags
from werkzeug.datastructures import FileStorage
from werkzeug.datastructures import Headers
from werkzeug.datastructures import MultiDict
from werkzeug.exceptions import BadRequest
from werkzeug.exceptions import HTTPException
from werkzeug.exceptions import MethodNotAllowed
from werkzeug.exceptions import NotFound
from werkzeug.routing import MapAdapter

from .utils import is_json


class TokenResponse:
    """AWS authorization response."""

    def __init__(self, allow: str | bool, arn: str):
        """Value may be string when allowed only if match workspace label."""
        self.allow = allow
        self.arn = arn

    @property
    def json(self) -> dict:
        return {
            "principalId": "user",
            "policyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "execute-api:Invoke",
                        "Effect": "Allow" if self.allow else "Deny",
                        "Resource": self.arn
                    }
                ]
            }
        }


class CoworksMapAdapter(MapAdapter):

    def __init__(self, environ, url_map, aws_url_map, stage_prefixed):
        server_name = environ["SERVER_NAME"]
        aws_stage = environ["aws_stage"]
        url_scheme = environ["REQUEST_SCHEME"]
        path_info = environ["PATH_INFO"]
        method = environ["REQUEST_METHOD"]
        script_name = f'/{aws_stage}/' if stage_prefixed else ''
        super().__init__(url_map, server_name=server_name, script_name=script_name, subdomain='',
                         url_scheme=url_scheme, path_info=path_info, default_method=method)
        self.aws_url_map = aws_url_map
        self.aws_entry_path = environ["aws_entry_path"]
        self.aws_entry_path_parameters = environ["aws_entry_path_parameters"]

    def match(self, method=None, return_rule=False, **kwargs):
        try:
            if self.aws_entry_path not in self.aws_url_map:
                raise NotFound()

            rules = self.aws_url_map[self.aws_entry_path]
            for rule in rules:
                if self.default_method in rule.methods:
                    return rule if return_rule else rule.endpoint, self.aws_entry_path_parameters
            else:
                raise MethodNotAllowed()
        except HTTPException:
            raise
        except Exception as e:
            current_app.logger.debug(f"Rule match error {e}")
            raise NotFound()


class CoworksResponse(FlaskResponse):
    """Default mimetype is redefined."""
    default_mimetype = "application/json"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class CoworksRequest(FlaskRequest):

    def __init__(self, environ, **kwargs):
        self.aws_event: dict | None = environ.get('aws_event')
        self.aws_context = environ.get('aws_context')
        self.aws_query_string: MultiDict | None = environ.get('aws_query_string')
        self.aws_body: str | bytes | None = environ.get('aws_body')
        self._in_lambda_context: bool = self.aws_event is not None
        self.__stream: BytesIO | None = None
        self.__form: MultiDict[str, str] | None = None
        self.__files: MultiDict | FileStorage | None = None
        self.__data = self.__json = None

        super().__init__(environ, **kwargs)

        if self.aws_event and self._in_lambda_context:
            self.headers = Headers(self.aws_event.get('headers'))

    @property
    def in_lambda_context(self):
        """Defined as a property to be read only."""
        return self._in_lambda_context

    @property
    def is_json(self) -> bool:
        """If no content type defined in request, default is application/json`.        """
        return not self.mimetype or super().is_json

    @property
    def is_multipart(self) -> bool:
        """Check if the mimetype indicates form-data.
        """
        mt = self.mimetype
        return (
                mt == "multipart/form-data"
        )

    @property
    def is_form_urlencoded(self) -> bool:
        """Check if the mimetype indicates form-data.
        """
        mt = self.mimetype
        return (
                mt == "application/x-www-form-urlencoded"
        )

    @property
    def args(self):
        if not self.in_lambda_context:
            return super().args
        return self.aws_query_string

    @property
    def stream(self):
        if not self.in_lambda_context:
            return super().stream

        if self.__stream is None:
            self._load_stream_form_files()
        return self.__stream

    @property
    def form(self):
        if not self.in_lambda_context:
            return super().form

        if self.__form is None:
            self._load_stream_form_files()
        return self.__form

    @property
    def files(self):
        if not self.in_lambda_context:
            return super().files

        if self.__files is None:
            self._load_stream_form_files()
        return self.__files

    def get_data(self, **kwargs):
        if not self.in_lambda_context:
            return super().get_data(**kwargs)

        if self.__data is None:
            if kwargs.get('as_text', False):
                self.__data = json.dumps(self.aws_body) if self._body_is_dict else self.aws_body
            elif self.aws_body:
                self.__data = self.aws_body if self._body_is_dict else json.loads(self.aws_body)
        return self.__data

    def get_json(self, **kwargs):
        if not self.in_lambda_context:
            return super().get_json(**kwargs)

        if self.__json is None and self.aws_body:
            self.__json = self.aws_body if self._body_is_dict else json.loads(self.aws_body)
        return self.__json

    @property
    def if_match(self):  # No cache
        return ETags()

    @property
    def if_none_match(self):  # No cache
        return ETags()

    @property
    def if_modified_since(self):  # No cache
        return None

    @property
    def _body_is_dict(self) -> bool:
        return isinstance(self.aws_body, dict)

    def _load_stream_form_files(self) -> None:

        # Stream part
        if self.is_multipart and isinstance(self.aws_body, str):
            self.__stream = io.BytesIO(base64.b64decode(self.aws_body))
        elif self.is_form_urlencoded and isinstance(self.aws_body, str):
            self.__stream = io.BytesIO(self.aws_body.encode('ascii'))
        elif is_json(self.mimetype) and isinstance(self.aws_body, bytes):
            self.__stream = io.BytesIO(self.aws_body)
        else:
            raise BadRequest(f'Undefined mime-type for stream body: {self.mimetype} ({type(self.aws_body)})')

        if self.is_multipart:
            self.__files = MultiDict()
            self.__form = MultiDict()
            multipart_data = MultipartDecoder(self.aws_body, self.content_type)
            for part in multipart_data.parts:

                # Files part
                if b'content-disposition' in part.headers:
                    content_disposition = part.headers.get(b'content-disposition').decode("utf-8")
                    filename_regexp = "filename=\"(?P<filename>[^\"]+)\""
                    match = re.search(filename_regexp, content_disposition)
                    filename = match.group('filename') if match else None

                    name_regexp = "name=\"(?P<name>[^\"]+)\""
                    match = re.search(name_regexp, content_disposition)
                    name = match.group('name') if match else None

                    if b'content-type' in part.headers:
                        content_type = part.headers.get(b'content-type').decode("utf-8")
                    else:
                        content_type = None

                    if b'content-length' in part.headers:
                        content_length = part.headers.get(b'content-length').decode("utf-8")
                    else:
                        content_length = None

                    self.__files[filename] = FileStorage(
                        stream=io.BytesIO(part.content),
                        filename=filename,
                        name=name,
                        content_type=content_type,
                        content_length=content_length,
                    )

                # Form part

        elif self.is_form_urlencoded:
            self.__files = MultiDict()
            self.__form = MultiDict(urllib.parse.parse_qs(t.cast(str, self.aws_body)))
