import dataclasses
import json
import os
import re
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import List, Tuple, Union

from .utils import as_list

DEFAULT_PROJECT_DIR = '.'
DEFAULT_WORKSPACE = 'dev'

ENV_FILE_SUFFIX = '.json'
SECRET_ENV_FILE_SUFFIX = '.secret.json'


# class AuthRequest(ChaliceAuthRequest):
#     def __init__(self, auth_type, token, method_arn):
#         self.auth_type = auth_type
#         self.token = token
#         self.method_arn = method_arn
#
#         _, self.workspace, self.method, self.route = method_arn.split('/', 3)
#
#
# class CORSConfig(ChaliceCORSConfig):
#
#     def get_access_control_headers(self):
#         if not self.allow_origin:
#             return {}
#         return super().get_access_control_headers()


@dataclass
class Config:
    """ Configuration class for deployment."""

    workspace: str = DEFAULT_WORKSPACE
    environment_variables_file: Union[str, List[str]] = 'vars.json'
    environment_variables: Union[dict, List[dict]] = None
    auth = None
    # auth: Union[
    #     Callable[[AuthRequest], Union[bool, list, AuthResponse]],
    #     Callable[[CoworksMixin, AuthRequest], Union[bool, list, AuthResponse]],
    # ] = None
    # cors: CORSConfig = CORSConfig(allow_origin='')
    content_type: Tuple[str] = ('multipart/form-data', 'application/json', 'text/plain')

    def is_valid_for(self, workspace: str) -> bool:
        return self.workspace == workspace

    def existing_environment_variables_files(self, project_dir):
        """Returns a list containing the paths to environment variables files that actually exist """

        # store in a dict to allow specific environment variable files to be overloaded
        files = {}

        def add_file(dir):
            for file in environment_variables_file:
                var_file = Path(dir) / file
                if var_file.is_file():
                    files[file] = var_file
                var_secret_file = var_file.with_suffix(SECRET_ENV_FILE_SUFFIX)
                if var_secret_file.is_file():
                    files[var_secret_file] = var_secret_file

        environment_variables_file = as_list(self.environment_variables_file)

        # get default then specific
        add_file('.')
        add_file(project_dir)
        return files.values()

    def load_environment_variables(self, project_dir):
        """Uploads environment variables from the environment variables files and variables."""
        environment_variables = {}

        # Environment variables from files and from config
        for file in self.existing_environment_variables_files(project_dir):
            try:
                with file.open() as f:
                    environment_variables.update(json.loads(f.read()))
            except JSONDecodeError as e:
                raise FileNotFoundError(f"Syntax error in file {file}: {str(e)}.\n")

        if self.environment_variables:
            environment_variables.update(self.environment_variables)

        # Check environment variables name are accepted by AWS
        # Keys start with a letter and are at least two characters.
        # Keys only contain letters, numbers, and the underscore character (_).
        var_name_regexp = re.compile(r'[a-zA-Z][a-zA-Z_]+')
        for key in environment_variables:
            if not var_name_regexp.fullmatch(key):
                raise KeyError(f'Wrong environment variable name: {key}')

        # Set environment variables
        if environment_variables:
            for key, value in environment_variables.items():
                os.environ[key] = str(value)

    def setdefault(self, key, value):
        """Same as for dict."""
        if not hasattr(self, key):
            setattr(self, key, value)

    def asdict(self):
        return dataclasses.asdict(self)


class LocalConfig(Config):
    def __init__(self, workspace='local', **kwargs):
        if 'environment_variables' not in kwargs:
            kwargs['environment_variables'] = {"AWS_XRAY_SDK_ENABLED": False}
        super().__init__(workspace=workspace, **kwargs)

        if self.auth is None:
            def no_check(auth_request):
                """Authorization method always validated."""
                return True

            self.auth = no_check


class DevConfig(Config):
    def __init__(self, workspace=DEFAULT_WORKSPACE, token_var_name='TOKEN', **kwargs):
        super().__init__(workspace=workspace, **kwargs)

        if self.auth is None:
            def check_token(auth_request):
                """Authorization method testing token value in header."""
                valid = (auth_request.token == os.getenv(token_var_name))
                return valid

            self.auth = check_token


class ProdConfig(DevConfig):
    """ Production configuration have workspace's name corresponding to version's name."""

    def __init__(self, environment_variables_file="vars.prod.json", pattern=r"v[1-9]+", token_var_name='TOKEN',
                 **kwargs):
        super().__init__(environment_variables_file=environment_variables_file, token_var_name=token_var_name, **kwargs)
        self.pattern = pattern

    def is_valid_for(self, workspace):
        return re.match(self.pattern, workspace) is not None
