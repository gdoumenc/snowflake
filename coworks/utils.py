from __future__ import annotations

import os

import dotenv
from .const import DEFAULT_DEV_STAGE


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
