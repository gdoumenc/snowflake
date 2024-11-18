import typing as t
from datetime import datetime

from sqlalchemy import ColumnOperators
from sqlalchemy import desc
from sqlalchemy import inspect
from sqlalchemy import not_
from sqlalchemy.ext.associationproxy import AssociationProxyInstance
from sqlalchemy.ext.associationproxy import association_proxy
from werkzeug.exceptions import UnprocessableEntity

from coworks.utils import to_bool
from .data import JsonApiDataMixin
from .fetching import fetching_context


def sql_filter(sql_model: type[JsonApiDataMixin]):
    """Returns the list of filters as a SQLAlchemy filter.

    :param sql_model: the SQLAlchemy model (used to get the SQLAlchemy filter)
    """
    _sql_filters: list[ColumnOperators] = []
    if isinstance(sql_model.jsonapi_type, property):
        jsonapi_type: str = sql_model.jsonapi_type.__get__(sql_model)
    else:
        jsonapi_type = t.cast(str, sql_model.jsonapi_type)
    for filter in fetching_context.get_filter_parameters(jsonapi_type, value_as_iterator=False):
        for key, oper, value in filter:
            if '.' in key:
                rel_name, col_name = key.split('.', 1)
            else:
                rel_name = None
                col_name = key

            # Column filtering
            if not rel_name:
                if not hasattr(sql_model, col_name):
                    msg = f"Wrong '{col_name}' property for sql model '{jsonapi_type}' in filters parameters"
                    raise UnprocessableEntity(msg)

                # Appends a sql filter criterion on column
                column = getattr(sql_model, col_name)
                _type = getattr(column, 'type', None)
                _sql_filters.append(*typed_filter(_type, column, oper, value))

            # Relationship filtering
            else:
                if '.' in col_name:
                    raise UnprocessableEntity(f"Association proxy of one level only {col_name}")
                if not hasattr(sql_model, rel_name):
                    msg = f"Wrong '{rel_name}' property for sql model '{jsonapi_type}' in filters parameters"
                    raise UnprocessableEntity(msg)

                # Appends a sql filter criterion on an association proxy column
                proxy = AssociationProxyInstance.for_proxy(association_proxy(rel_name, col_name), sql_model, None)
                _type = getattr(proxy.attr[1], 'type', None)
                _sql_filters.append(*typed_filter(_type, proxy, oper, value))

    return _sql_filters


def sql_order_by(sql_model):
    """Returns a SQLAlchemy order from model using fetching order keys.

    :param sql_model: the SQLAlchemy model (used to get the SQLAlchemy order)."""
    insp = inspect(sql_model)
    _sql_order_by = []
    for key in fetching_context._sort:
        column = None
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
                raise UnprocessableEntity(f"Undefined sort key '{key}' on model '{sql_model}'")

        # sort on column attributes
        elif key in insp.column_attrs:
            column = insp.column_attrs[key]
            if asc_sort:
                column = desc(column)

        if column is None:
            raise UnprocessableEntity(f"Undefined sort key '{key}' on model '{sql_model}'")
        _sql_order_by.append(column)
    return _sql_order_by


def typed_filter(_type, column, oper: str | None, value: list) -> list[ColumnOperators]:
    """Creates SQL filter depending on the column type.

    :param _type: Column type.
    :param column: Column name.
    :param oper: SQLAchemy operator.
    :param value: Value.
    """
    if oper == 'null':
        if len(value) != 1:
            raise UnprocessableEntity("Multiple boolean values for null test not allowed")
        return [column.is_(None) if to_bool(value[0]) else not_(column.is_(None))]

    if _type:
        if _type.python_type is bool:
            return bool_filter(column, oper, value)
        if _type.python_type is str:
            return str_filter(column, oper, value)
        elif _type.python_type is int:
            return int_filter(column, oper, value)
        elif _type.python_type is datetime:
            return datetime_filter(column, oper, value)
        elif _type.python_type is list:
            return list_filter(column, oper, value)
    return column.in_(value)


def bool_filter(column, oper, value) -> list[ColumnOperators]:
    """Boolean filter."""
    if len(value) != 1:
        raise UnprocessableEntity("Multiple boolean values is not allowed")
    return [column == to_bool(value[0])]


def str_filter(column, oper, value) -> list[ColumnOperators]:
    """String filter."""
    oper = oper or 'eq'
    if oper == 'eq':
        return [column.in_(value)]
    if oper == 'neq':
        return [not_(column.in_(value))]
    if oper == 'ilike':
        return [column.ilike(v) for v in value]
    if oper == 'nilike':
        return [not_(column.ilike(v)) for v in value]
    if oper == 'contains':
        return [column.contains(str(v)) for v in value]
    if oper == 'ncontains':
        return [not_(column.contains(str(v))) for v in value]
    if oper == 'in':
        return [column.in_([w for v in value for w in str(v).split(',')])]
    if oper == 'nin':
        return [not_(column.in_([w for v in value for w in str(v).split(',')]))]
    msg = f"Undefined operator '{oper}' for string value"
    raise UnprocessableEntity(msg)


def int_filter(column, oper, value) -> list[ColumnOperators]:
    """Integer filter."""
    oper = oper or 'eq'
    if oper == 'in':
        return [column.in_([int(i) for i in v.split(',')]) for v in value]
    else:
        return [sort_operator(column, oper, int(v)) for v in value]


def datetime_filter(column, oper, value) -> list[ColumnOperators]:
    """Datetime filter."""
    oper = oper or 'eq'
    return [sort_operator(column, oper, datetime.fromisoformat(v)) for v in value]


def list_filter(column, oper, value) -> list[ColumnOperators]:
    """List filter."""
    if oper == 'contains':
        return [column.contains(value)]
    if oper == 'ncontains':
        return [not_(column.contains([v])) for v in value]
    msg = f"Undefined operator '{oper}' for list value"
    raise UnprocessableEntity(msg)


def sort_operator(column, oper, value) -> t.Any:
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
    msg = f"Undefined operator '{oper}'"
    raise UnprocessableEntity(msg)
