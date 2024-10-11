import typing as t

from .coworks import Blueprint
from .coworks import TechMicroService
from .coworks import entry
from .globals import request
from .version import __version__

type StrList = list[str]
type StrSet = set[str]
type StrDict[T] = dict[str, T]

__all__ = (
    'TechMicroService', 'Blueprint', 'entry',
    'StrList', 'StrSet', 'StrDict',
    'request', '__version__',
)


def __getattr__(name: str) -> t.Any:
    if name == "__version__":
        import importlib.metadata
        import warnings

        warnings.warn(
            "The '__version__' attribute is deprecated and will be removed.",
            DeprecationWarning,
            stacklevel=2,
        )
        return importlib.metadata.version("coworks")

    raise AttributeError(name)
