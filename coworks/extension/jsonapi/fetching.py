from collections import defaultdict

import contextlib
import typing as t
from coworks import StrDict
from coworks import StrList
from coworks import StrSet
from coworks import request
from coworks.proxy import nr_url
from jsonapi_pydantic.v1_0 import Link
from jsonapi_pydantic.v1_0 import TopLevel
from pydantic import BaseModel
from pydantic.networks import HttpUrl
from werkzeug.exceptions import UnprocessableEntity
from werkzeug.local import LocalProxy

from .query import Pagination

type FilterType = tuple[str, str | None, StrList | None]


class Filter(BaseModel, t.Iterable[FilterType]):
    value_as_iterator: bool
    attr: str
    comparators: dict[str, t.Any]

    def add_comparator(self, oper, values):
        if oper in self.comparators:
            self.comparators[oper].extend(values)
        else:
            self.comparators[oper] = values

    def __iter__(self):
        """Iter with all values or simply once."""
        for oper, values in self.comparators.items():
            if values is None:
                yield [] if self.value_as_iterator else None

            if not self.value_as_iterator and isinstance(values, t.Iterator) and not isinstance(values, str):
                yield from ((self.attr, oper, value) for value in values)
            else:
                yield self.attr, oper, values

    def __contains__(self, key):
        return key in self.opers()

    def opers(self) -> t.Iterable[str]:
        return self.comparators.keys()

    def values(self, oper) -> t.Iterable[list]:
        return self.comparators.get(oper, [])


class Filters(t.Iterable[Filter]):

    def __init__(self, filters: dict, jsonapi_type: str, none_oper: str, value_as_iterator: bool):
        self.jsonapi_type = jsonapi_type
        self.value_as_iterator = value_as_iterator

        self._params: StrDict[Filter] = {}
        for k in filter(lambda x: x.startswith(jsonapi_type), filters.keys()):
            prefix = k[len(jsonapi_type):]
            criterions = filters.get(k, [])
            for attr, values in criterions:
                # filter operator
                # idea from https://discuss.jsonapi.org/t/share-propose-a-filtering-strategy/257
                if "____" in attr:
                    attr, oper = attr.split('____', 1)
                    oper = oper or none_oper
                else:
                    oper = none_oper

                if prefix:
                    attr = prefix[1:] + '.' + attr

                if attr in self._params:
                    self._params[attr].add_comparator(oper, values)
                else:
                    self._params[attr] = Filter(value_as_iterator=value_as_iterator, attr=attr,
                                                comparators={oper: values})

    def __iter__(self) -> t.Iterator[Filter]:
        yield from (f for f in self._params.values())

    def __contains__(self, key):
        return key in self.keys()

    def keys(self) -> t.Iterable[str]:
        return self._params.keys()

    def get(self, key: str, default=None) -> Filter | None:
        return self._params.get(key, default)


class FetchingContext:

    def __init__(self, include: str | None = None, fields__: StrDict[str] | None = None,
                 filters__: StrDict[str] | None = None, sort: str | None = None,
                 page__number__: int | None = None, page__size__: int | None = None, page__max__: int | None = None):
        self.include: StrSet = set(split_parameter(include)) if include else set()
        self._fields: StrDict[str] = fields__ if fields__ is not None else {}
        self._sort: StrList = list(split_parameter(sort)) if sort else []
        self.page: int = page__number__ or 1
        self.per_page: int = page__size__ or 100
        self.max_per_page: int = page__max__ or 100

        # Creates filters dict defined ad jsonapi type as key and attribute expression as value
        self._filters: dict = defaultdict(list)
        filters__ = filters__ if filters__ is not None else {}
        for k, v in filters__.items():
            try:
                json_type, filter = k.rsplit('.', 1)
                self._filters[json_type].append((filter, v))
            except ValueError:
                msg = f"The filter parameter '{k}' must be of the form 'filter[type.value][oper]'"
                raise ValueError(msg)

        self.connection_manager = contextlib.nullcontext()

    def field_names(self, jsonapi_type) -> set[str]:
        """Returns the field's names that must be returned for a specific jsonapi type."""
        if jsonapi_type not in self._fields:
            return set()

        fields = self._fields[jsonapi_type]
        if isinstance(fields, list):
            if len(fields) != 1:
                msg = f"Wrong field value '{fields}': multiple fields parameter must be a comma-separated"
                raise UnprocessableEntity(msg)
            field = fields[0]
        else:
            field = fields

        return set(split_parameter(field))

    def get_filter_parameters(self, jsonapi_type: str, *, none_oper='eq', value_as_iterator: bool = True) -> Filters:
        """Get all filters parameters starting with the jsonapi model class name."""
        return Filters(self._filters, jsonapi_type, none_oper=none_oper, value_as_iterator=value_as_iterator)

    @staticmethod
    def add_pagination(toplevel: TopLevel, pagination: type[Pagination]):
        if pagination.total > 1:
            links = toplevel.links or {}
            if pagination.has_prev:
                links["prev"] = Link(
                    href=HttpUrl(nr_url(request.path, {"page[number]": pagination.prev_num}, merge_query=True))
                )
            if pagination.has_next:
                links["next"] = Link(
                    href=HttpUrl(nr_url(request.path, {"page[number]": pagination.next_num}, merge_query=True))
                )
            toplevel.links = links

        meta = toplevel.meta or {}
        meta["count"] = pagination.total
        meta["pagination"] = {
            "page": pagination.page,
            "pages": pagination.pages,
            "per_page": pagination.per_page,
        }
        toplevel.meta = meta


def create_fetching_context_proxy(include: str | None = None, fields__: dict | None = None,
                                  filters__: dict | None = None, sort: str | None = None,
                                  page__number__: int | None = None, page__size__: int | None = None,
                                  page__max__: int | None = None):
    context = FetchingContext(include, fields__, filters__, sort, page__number__, page__size__, page__max__)
    setattr(request, 'fetching_context', context)


fetching_context = t.cast(FetchingContext,
                          LocalProxy(lambda: getattr(request, 'fetching_context', 'Not in JsonApi context')))


def split_parameter(param: str) -> t.Iterable[str]:
    return filter(lambda x: x, map(str.strip, param.split(',')))
