from datetime import datetime

from werkzeug.exceptions import UnprocessableEntity

from coworks.utils import to_bool
from .data import JsonApiBaseModel
from .fetching import fetching_context


def pydantic_filter(base_model: JsonApiBaseModel):
    _base_model_filters: list[bool] = []
    for filter in fetching_context.get_filter_parameters(base_model.jsonapi_type):
        for key, oper, value in filter:
            if '.' in key:
                _, key = key.split('.', 1)
            if not hasattr(base_model, key):
                msg = f"Wrong '{key}' key for '{base_model.jsonapi_type}' in filters parameters"
                raise UnprocessableEntity(msg)
            column = getattr(base_model, key)

            if oper == 'null':
                if to_bool(value[0]):
                    _base_model_filters.append(column is None)
                else:
                    _base_model_filters.append(column is not None)
                continue

            _type = base_model.model_fields.get(key).annotation  # type: ignore[union-attr]
            if _type is bool:
                _base_model_filters.append(base_model_filter(column, oper, to_bool(value[0])))
            elif _type is int:
                _base_model_filters.append(base_model_filter(column, oper, int(value[0])))
            elif _type is datetime:
                _base_model_filters.append(
                    base_model_filter(datetime.fromisoformat(column), oper, datetime.fromisoformat(value[0]))
                )
            else:
                _base_model_filters.append(base_model_filter(str(column), oper, value[0]))

    return all(_base_model_filters)


def base_model_filter(column, oper, value) -> bool:
    """String filter."""
    oper = oper or 'eq'
    if oper == 'eq':
        return column == value
    if oper == 'neq':
        return column != value
    if oper == 'contains':
        return value in column
    if oper == 'ncontains':
        return value not in column
    msg = f"Undefined operator '{oper}' for string value"
    raise UnprocessableEntity(msg)
