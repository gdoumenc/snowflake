import contextlib
import json
import typing as t
from collections import defaultdict
from datetime import datetime

from jsonapi_pydantic.v1_0 import Link
from jsonapi_pydantic.v1_0 import TopLevel
from pydantic.networks import HttpUrl
from sqlalchemy import ColumnOperators
from sqlalchemy import desc
from sqlalchemy import inspect
from sqlalchemy import not_
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.orm import RelationshipProperty
from werkzeug.exceptions import UnprocessableEntity
from werkzeug.local import LocalProxy

from coworks import request
from coworks.utils import nr_url
from coworks.utils import str_to_bool
from .data import JsonApiBaseModel
from .data import JsonApiDataMixin
from .query import Pagination


class FetchingContext:

    def __init__(self, include: str | None = None, fields__: dict[str, str] | None = None,
                 filters__: dict[str, str] | None = None, sort: str | None = None,
                 page__number__: int | None = None, page__size__: int | None = None, page__max__: int | None = None):
        self.include: set[str] = set(map(str.strip, include.split(','))) if include else set()
        self._fields: dict[str, str] = fields__ if fields__ is not None else {}
        self._sort: list[str] = list(map(str.strip, sort.split(','))) if sort else []
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
        return set(map(str.strip, field.split(',')))

    def pydantic_filters(self, base_model: JsonApiBaseModel):
        _base_model_filters: list[bool] = []
        filter_parameters = fetching_context._filters.get(base_model.jsonapi_type, {})
        for key, value in filter_parameters:
            name, key, oper = self.get_decomposed_key(key)
            if not hasattr(base_model, key):
                msg = f"Wrong '{key}' key for '{base_model.jsonapi_type}' in filters parameters"
                raise UnprocessableEntity(msg)
            column = getattr(base_model, key)

            if oper == 'null':
                if str_to_bool(value[0]):
                    _base_model_filters.append(column is None)
                else:
                    _base_model_filters.append(column is not None)
                continue

            _type = base_model.model_fields.get(key).annotation  # type: ignore[union-attr]
            if _type is bool:
                _base_model_filters.append(base_model_filter(column, oper, str_to_bool(value[0])))
            elif _type is int:
                _base_model_filters.append(base_model_filter(column, oper, int(value[0])))
            elif _type is datetime:
                _base_model_filters.append(
                    base_model_filter(datetime.fromisoformat(column), oper, datetime.fromisoformat(value[0]))
                )
            else:
                _base_model_filters.append(base_model_filter(str(column), oper, value[0]))

        return all(_base_model_filters)

    def sql_filters(self, sql_model: t.Type[JsonApiDataMixin]):
        """Returns the list of filters as a SQLAlchemy filter.

        :param sql_model: the SQLAlchemy model (used to get the SQLAlchemy filter)
        """
        _sql_filters: list[ColumnOperators] = []
        if isinstance(sql_model.jsonapi_type, property):
            jsonapi_type: str = sql_model.jsonapi_type.__get__(sql_model)
        else:
            jsonapi_type = t.cast(str, sql_model.jsonapi_type)
        filter_parameters = self._filters.get(jsonapi_type, {})
        for key, value in filter_parameters:

            # If parameters are defined in body payload they may be not defined as list
            if not isinstance(value, list):
                value = [value]

            name, key, oper = self.get_decomposed_key(key)
            if not hasattr(sql_model, key):
                msg = f"Wrong '{key}' property for sql model '{jsonapi_type}' in filters parameters"
                raise UnprocessableEntity(msg)

            column = getattr(sql_model, key)

            if oper == 'null':
                if len(value) != 1:
                    msg = f"Multiple boolean values '{key}' for null test not allowed"
                    raise UnprocessableEntity(msg)
                exp = column.is_(None) if str_to_bool(value[0]) else not_(column.is_(None))
                _sql_filters.append(exp)

            elif isinstance(column.property, ColumnProperty):
                _type = getattr(column, 'type', None)
                if _type:
                    if _type.python_type is bool:
                        _sql_filters.append(*bool_sql_filter(jsonapi_type, key, column, oper, value))
                    elif _type.python_type is str:
                        _sql_filters.append(*str_sql_filter(jsonapi_type, key, column, oper, value))
                    elif _type.python_type is int:
                        _sql_filters.append(*int_sql_filter(jsonapi_type, key, column, oper, value))
                    elif _type.python_type is datetime:
                        _sql_filters.append(*datetime_sql_filter(jsonapi_type, key, column, oper, value))
                else:
                    _sql_filters.append(column.in_(value))
            elif isinstance(column.property, RelationshipProperty):
                if not isinstance(value, list) or len(value) != 1:
                    msg = (f"Wrong '{value}' value for sql model '{jsonapi_type}'"
                           " in filters parameters (should be a simple list).")
                    raise UnprocessableEntity(msg)
                value = json.loads(value[0])
                if not isinstance(value, dict):
                    msg = (f"Wrong '{value}' value for sql model '{jsonapi_type}'"
                           " in filters parameters (should be a dict).")
                    raise UnprocessableEntity(msg)
                for k, v in value.items():
                    condition = column.has(**{k: v})
                    _sql_filters.append(condition)

        return _sql_filters

    def sql_order_by(self, sql_model):
        """Returns a SQLAlchemy order from model using fetching order keys.

        :param sql_model: the SQLAlchemy model (used to get the SQLAlchemy order)."""
        insp = inspect(sql_model)
        _sql_order_by = []
        for key in self._sort:
            asc_sort = False
            if key.startswith('-'):
                asc_sort = True
                key = key[1:]

            # sort on relationship
            if '.' in key:
                key, attr = key.split('.', 1)
                if key in insp.all_orm_descriptors:
                    column = insp.all_orm_descriptors[key]
                    raise UnprocessableEntity("Sort on relationship is not implemented")
                else:
                    raise UnprocessableEntity(f"Undefined sort key {key} on model {sql_model}")

            # sort on column attributes
            elif key in insp.column_attrs:
                column = insp.column_attrs[key]
                if asc_sort:
                    column = desc(column)

            _sql_order_by.append(column)
        return _sql_order_by

    def _add_branch(self, tree, vector, value):
        key = vector[0]

        if len(vector) == 1:
            tree[key] = value
        else:
            sub_tree = tree.get(key, {})
            tree[key] = self._add_branch(sub_tree, vector[1:], value)

        return tree

    @staticmethod
    def add_pagination(toplevel: TopLevel, pagination: Pagination):
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

    def get_decomposed_key(self, key) -> tuple[str | None, str, str | None]:
        name = oper = None

        # filter operator
        # idea from https://discuss.jsonapi.org/t/share-propose-a-filtering-strategy/257
        if "____" in key:
            key, oper = key.split('____', 1)

        # if the key is named (for allowing composition of filters)
        if ':' in key:
            name, key = key.split(':', 1)

        return name, key, oper


def create_fetching_context_proxy(include: str | None = None, fields__: dict | None = None,
                                  filters__: dict | None = None, sort: str | None = None,
                                  page__number__: int | None = None, page__size__: int | None = None,
                                  page__max__: int | None = None):
    context = FetchingContext(include, fields__, filters__, sort, page__number__, page__size__, page__max__)
    setattr(request, 'fetching_context', context)


fetching_context = t.cast(FetchingContext,
                          LocalProxy(lambda: getattr(request, 'fetching_context', 'Not in JsonApi context')))


def base_model_filter(column, oper, value) -> bool:
    """String filter."""
    oper = oper or 'eq'
    if oper == 'eq':
        return column == value
    if oper == 'neq':
        return column != value
    if oper == 'contains':
        return value in column
    msg = f"Undefined operator '{oper}' for string value"
    raise UnprocessableEntity(msg)


def bool_sql_filter(jsonapi_type, key, column, oper, value) -> list[ColumnOperators]:
    """Boolean filter."""
    if len(value) != 1:
        msg = f"Multiple boolean values '{key}' property on model '{jsonapi_type}' is not allowed"
        raise UnprocessableEntity(msg)
    return [column == str_to_bool(value[0])]


def str_sql_filter(jsonapi_type, key, column, oper, value) -> list[ColumnOperators]:
    """String filter."""
    oper = oper or 'eq'
    if oper == 'eq':
        return [column.in_(value)]
    if oper == 'ilike':
        return [column.ilike('%' + str(v) + '%') for v in value]
    if oper == 'contains':
        return [column.contains(str(v)) for v in value]
    msg = f"Undefined operator '{oper}' for string value"
    raise UnprocessableEntity(msg)


def int_sql_filter(jsonapi_type, key, column, oper, value) -> list[ColumnOperators]:
    """Datetime filter."""
    oper = oper or 'eq'
    if oper not in ('eq', 'neq', 'ge', 'gt', 'le', 'lt'):
        msg = f"Undefined operator '{oper}' for integer value"
        raise UnprocessableEntity(msg)
    return [sort_operator(column, oper, int(v)) for v in value]


def datetime_sql_filter(jsonapi_type, key, column, oper, value) -> list[ColumnOperators]:
    """Datetime filter."""
    oper = oper or 'eq'
    if oper not in ('eq', 'neq', 'ge', 'gt', 'le', 'lt'):
        msg = f"Undefined operator '{oper}' for datetime value"
        raise UnprocessableEntity(msg)
    return [sort_operator(column, oper, datetime.fromisoformat(v)) for v in value]


def sort_operator(column: t.Any, oper, value) -> t.Any:
    if oper == 'eq':
        return column == value
    if oper == 'neq':
        return column != value
    if oper == 'ge':
        return column >= value
    if oper == 'gt':
        return column > value
    if oper == 'le':
        return column <= value
    if oper == 'lt':
        return column < value
    msg = f"Undefined operator '{oper}' in sort_operator"
    raise UnprocessableEntity(msg)
