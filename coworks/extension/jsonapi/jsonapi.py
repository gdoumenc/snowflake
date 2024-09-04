import typing as t
from asyncio import iscoroutine
from functools import update_wrapper
from inspect import Parameter
from inspect import signature

from coworks import TechMicroService
from coworks import request
from flask import current_app
from flask import make_response
from jsonapi_pydantic.v1_0 import Error
from jsonapi_pydantic.v1_0 import ErrorLinks
from jsonapi_pydantic.v1_0 import Link
from jsonapi_pydantic.v1_0 import Relationship
from jsonapi_pydantic.v1_0 import Resource
from jsonapi_pydantic.v1_0 import ResourceIdentifier
from jsonapi_pydantic.v1_0 import TopLevel
from pydantic import ValidationError
from pydantic.networks import HttpUrl
from sqlalchemy import ScalarResult
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.exc import NoResultFound
from werkzeug.exceptions import BadRequest
from werkzeug.exceptions import HTTPException
from werkzeug.exceptions import InternalServerError
from werkzeug.exceptions import NotFound

from .data import JsonApiDataMixin
from .data import JsonApiRelationship
from .fetching import create_fetching_context_proxy
from .fetching import fetching_context
from .query import Pagination
from .query import Query
from .query import Scalar


class JsonApiError(Exception):
    """Exception wich will create a JSON:API error."""

    def __init__(self, id_or_error: t.Union[str, Exception, "JsonApiError", list["JsonApiError"]],
                 title: str | None = None, detail: str | None = None, code=None, status=None):
        if isinstance(id_or_error, str):
            code = str(code) or None
            status = str(status) or InternalServerError.code
            try:
                self.errors = [Error(id=id_or_error, code=code, title=title, detail=detail, status=status)]
            except ValidationError as e:
                self.errors = [Error(id=InternalServerError.code, title=str(title), detail=str(e),
                                     status=str(InternalServerError.code))]
        else:
            if isinstance(id_or_error, JsonApiError):
                id_or_error = [id_or_error]
            elif isinstance(id_or_error, Exception):
                self.errors = [Error(id=InternalServerError.code, title=type(id_or_error).__name__,
                                     detail=str(id_or_error), status=str(InternalServerError.code))]
            else:
                self.errors = id_or_error


class JsonApi:
    """Flask's extension implementing JSON:API specification.
    This extension uses the external API of ODOO.

    .. versionchanged:: 0.7.3
        ``env_var_prefix`` parameter may be a dict of bind values.
        GraphQL removed.
    """

    def __init__(self, app=None):
        """
        :param app: Flask application.
        """
        self.app = None

        if app:
            self.init_app(app)

    def init_app(self, app: TechMicroService):
        self.app = app
        handle_http_exception = app.handle_http_exception
        handle_user_exception = app.handle_user_exception

        def _handle_http_exception(e):
            if 'application/vnd.api+json' not in request.headers.getlist('accept'):
                return handle_http_exception(e)

            try:
                return handle_http_exception(e)
            except JsonApiError as e:
                capture_exception(e)
                return _toplevel_error_response(e.errors)
            except HTTPException as e:
                capture_exception(e)
                errors = [Error(id=e.name, title=e.name, detail=e.description, status=e.code)]
                return _toplevel_error_response(errors, status_code=e.code)

        def _handle_user_exception(e):
            if 'application/vnd.api+json' not in request.headers.getlist('accept'):
                return handle_user_exception(e)

            try:
                return handle_user_exception(e)
            except JsonApiError as e:
                capture_exception(e)
                return _toplevel_error_response(e.errors)
            except ValidationError as e:
                capture_exception(e)
                errors = [Error(id="", status=BadRequest.code, code=err['type'],
                                links=ErrorLinks(about=err['url']),  # type: ignore[typeddict-item]
                                title=err['msg'], detail=str(err['loc'])) for err in e.errors()]
                errors.append(Error(id="", status=BadRequest.code, title=e.title, detail=str(e)))
                return _toplevel_error_response(errors, status_code=BadRequest.code)
            except HTTPException as e:
                capture_exception(e)
                errors = [Error(id=e.name, title=e.name, detail=e.description, status=e.code)]
                return _toplevel_error_response(errors, status_code=e.code)
            except Exception as e:
                capture_exception(e)
                errors = [Error(id=e.__class__.__name__, title=e.__class__.__name__, detail=str(e),
                                status=InternalServerError.code)]
                return _toplevel_error_response(errors, status_code=InternalServerError.code)

        def capture_exception(e):
            app.full_logger_error(e)

        app.handle_http_exception = _handle_http_exception
        app.handle_user_exception = _handle_user_exception

        app.after_request(self._change_content_type)

    def _change_content_type(self, response):
        if 'application/vnd.api+json' not in request.headers.getlist('accept'):
            return response

        response.content_type = 'application/vnd.api+json'
        return response


def jsonapi(func):
    """JSON:API decorator.
    Transforms an entry into an SQL entry with result as JSON:API.

    Must have Flask-SQLAlchemy extension installed.
    """

    async def _jsonapi(*args, ensure_one: bool = False, include: str | None = None,
                       fields__: dict | None = None, filters__: dict | None = None, sort: str | None = None,
                       page__number__: int | None = None, page__size__: int | None = None,
                       page__max__: int | None = None,
                       __neorezo__: dict | None = None, **kwargs):
        """

        :param args: entry args.,
                 page: int | None = None, per_page: int | None = None
        :param ensure_one: retrieves only one resource if true (default false).
        :param include:
        :param fields:
        :param filters:
        :param sort:
        :param page:
        :param per_page:
        :param max_per_page:
        :param kwargs: entry kwargs.
        """
        create_fetching_context_proxy(include, fields__, filters__, sort, page__number__, page__size__, page__max__)
        try:
            res = func(*args, **kwargs)
        except Exception as e:
            return current_app.handle_user_exception(e)

        if iscoroutine(res):
            res = await res

        code = 200
        try:
            if isinstance(res, Query):
                _toplevel = get_toplevel_from_query(res, ensure_one=ensure_one)
            elif isinstance(res, ScalarResult):
                _toplevel = get_toplevel_from_query(res, ensure_one=True)
            elif isinstance(res, TopLevel):
                _toplevel = res
            else:
                raise InternalServerError(f"{res} is not a Query, ScalarResult or TopLevel instance")
        except NotFound:
            if ensure_one:
                raise
            _toplevel = TopLevel(data=[])
        return _toplevel.model_dump_json(exclude_none=True), code

    # Adds JSON:API query parameters
    sig = signature(_jsonapi)
    # Removes self and kwargs from jsonapi wrapper
    jsonapi_sig = tuple(sig.parameters.values())[1:-1]
    # Splits variadic keyword to add it at end of the signature
    func_sig1 = tuple(p for p in signature(func).parameters.values() if p.kind != Parameter.VAR_KEYWORD)
    func_sig2 = tuple(p for p in signature(func).parameters.values() if p.kind == Parameter.VAR_KEYWORD)
    sig = sig.replace(parameters=func_sig1 + jsonapi_sig + func_sig2)
    update_wrapper(_jsonapi, func)
    setattr(_jsonapi, '__signature__', sig)
    return _jsonapi


def to_ressource_data(jsonapi_data: JsonApiDataMixin, *, included: dict[str, dict], prefix: str | None = None,
                      include: set[str] | None = None, exclude: set[str] | None = None) -> dict[str, t.Any]:
    """Transform a simple structure data into a jsonapi ressource data.

    Beware : included is a dict of type/id key and jsonapi ressource value
    :param jsonapi_data: the data to transform.
    :param included_prefix: the prefix of the included resources (indirect inclusion)
    """
    prefix = prefix or ''
    include = include or set()
    exclude = exclude or set()

    # set resource data from basemodel
    _type = jsonapi_data.jsonapi_type
    attrs, rels = jsonapi_data.jsonapi_attributes(
        include=_remove_prefix(include, prefix), exclude=_remove_prefix(exclude, prefix)
    )
    include = include or set(rels) - exclude
    if 'type' in attrs:
        _type = attrs.pop('type')
    else:
        _type = jsonapi_data.jsonapi_type
    if 'id' in attrs:
        _id = str(attrs.pop('id'))
    else:
        _id = jsonapi_data.jsonapi_id

    # get related resources relationships
    relationships: dict[str, Relationship] = {}
    if prefix:
        to_be_excluded = [k[len(prefix):] for k in exclude if prefix in k]
    else:
        to_be_excluded = [k for k in exclude or [] if '.' not in k]

    # add relationship and included resources if needed
    for key, rel in rels.items():
        if key in to_be_excluded or rel is None:
            continue

        if isinstance(rel, list):
            res_ids = []
            for val in rel:
                res_id = get_resource_identifier(val)
                res_ids.append(res_id)
                _add_to_included(included, key, val, include=include, exclude=exclude, prefix=prefix)
            relationships[key] = Relationship(data=res_ids)
        else:
            res_id = get_resource_identifier(rel)
            _add_to_included(included, key, rel, include=include, exclude=exclude, prefix=prefix)
            relationships[key] = Relationship(data=res_id)

    resource_data = {
        "type": _type,
        "id": _id,
        "lid": None,
        "attributes": attrs,
        "links": get_resource_links(jsonapi_data)
    }

    if relationships:
        resource_data["relationships"] = relationships
    if included:
        resource_data["included"] = included

    return resource_data


def get_toplevel_from_query(query: Query | Scalar, *, ensure_one: bool, include: set[str] | None = None,
                            exclude: set[str] | None = None) -> TopLevel:
    """Returns the Toplevel structure from the query.

    :param query: the fetched query.
    :param ensure_one: whether to ensure that only one Toplevel instance is returned (else raises NotFound)
    :param include: an optional set of included resources
    :param exclude: an optional set of excluded resources
    """
    include = fetching_context.include | (include or set())
    exclude = exclude or set()

    def get_toplevel():
        current_app.logger.debug(str(query))
        if ensure_one:
            try:
                resource: JsonApiDataMixin = query.one()
            except (NoResultFound, MultipleResultsFound):
                raise NotFound("None or more than one resource found and ensure_one parameters was set")
            toplevel = toplevel_from_data(resource, include=include, exclude=exclude)
        else:
            if not isinstance(query, Query):
                raise InternalServerError("The query must be a Query if ensure_one is set to True.")
            pagination: Pagination = query.paginate(page=fetching_context.page, per_page=fetching_context.per_page,
                                                    max_per_page=fetching_context.max_per_page)
            toplevel = toplevel_from_pagination(pagination, include=include, exclude=exclude)
        return toplevel

    # connection manager may be iterable (should be performed asynchronously)
    if isinstance(fetching_context.connection_manager, t.Iterable):
        _toplevels = []
        for connection_manager in fetching_context.connection_manager:
            with connection_manager:
                _toplevels.append(get_toplevel())
        data = [r for tp in _toplevels for r in tp.data]
        included = {i.type + i.id: i for tp in _toplevels if tp.included for i in tp.included}
        if len(_toplevels) == 1:
            meta = _toplevels[0].meta
            links = _toplevels[0].links
        else:
            meta = {"count": len(data)}
            links = {}
        return TopLevel(data=data, included=included.values() if included else None, meta=meta, links=links)

    with fetching_context.connection_manager:
        return get_toplevel()


def toplevel_from_data(res: JsonApiDataMixin, include: set[str], exclude: set[str]) -> TopLevel:
    """Transform a simple structure data into a toplevel jsonapi.

    :param res: the data to transform into a toplevel jsonapi structure.
    :param include: set of included resources.
    :param exclude: set of excluded resources.
    """
    included: dict[str, dict] = {}
    filtered_fields = fetching_context.field_names(res.jsonapi_type) | include
    data = to_ressource_data(res, included=included, include=filtered_fields, exclude=exclude)
    resources = Resource(**data)
    included_resources = [Resource(**i) for i in included.values()]
    return TopLevel(data=resources, included=included_resources if included else None)


def toplevel_from_pagination(pagination: Pagination, include: set[str], exclude: set[str]):
    """Transform an iterable pagination into a toplevel jsonapi.

    :param pagination: the data to transform.
    :param include: set of included resources
    :param exclude: set of excluded resources
    """
    included: dict[str, dict] = {}
    data = []
    for d in t.cast(t.Iterable, pagination):
        filtered_fields = fetching_context.field_names(d.jsonapi_type) | include
        data.append(to_ressource_data(d, included=included, include=filtered_fields, exclude=exclude))
    resources = [Resource(**d) for d in data]
    included_resources = [Resource(**i) for i in included.values()]
    toplevel = TopLevel(data=resources, included=included_resources if included else None)
    fetching_context.add_pagination(toplevel, pagination)
    return toplevel


def get_resource_identifier(rel: JsonApiRelationship):
    """ Adds a relationship in the list of relationships from the related model.
    The relationship may not be complete for circular reference and will be completed after in construction.
    """
    if not isinstance(rel, JsonApiRelationship):
        msg = f"Relationship value must be of type JsonApiRelationship, not {rel.__class__}"
        raise InternalServerError(msg)

    type_ = rel.jsonapi_type
    id_ = rel.jsonapi_id
    return ResourceIdentifier(type=type_, id=id_)


def get_resource_links(jsonapi_basemodel) -> dict:
    """Get the links associated to a ressource (from jsonapi_self_link property).
    
    If jsonapi_self_link is a string then there is only one self link.
    """
    self_link = jsonapi_basemodel.jsonapi_self_link
    if isinstance(self_link, str):
        return {'self': Link(href=HttpUrl(jsonapi_basemodel.jsonapi_self_link))}
    if isinstance(self_link, dict):
        return self_link
    raise InternalServerError("Unexpected jsonapi_self_link value")


def _toplevel_error_response(errors, *, status_code=None):
    toplevel = TopLevel(errors=errors).model_dump_json()
    if status_code is None:
        status_code = max((err.status for err in errors))
    response = make_response(toplevel, status_code)
    response.content_type = 'application/vnd.api+json'
    return response


def _add_to_included(included, key, res: JsonApiRelationship, *, prefix, include, exclude):
    """Adds the resource defined at key to the included list of resource.
    
    :param included: list of included resources to increment (if not already inside).
    :param key: the key where the resource is in the parent resource.
    :param res: the relationship to include.
    :param include: set of included resources
    :param exclude: set of excluded resources
    :param included_prefix: dot separated path in resource.
    """
    res_key = res.jsonapi_type + res.jsonapi_id
    if res_key not in included:
        if res.resource_value:
            included[res_key] = None

            # Creates and includes the resource
            new_prefix = f"{prefix}{key}." if prefix else f"{key}."
            field_names = fetching_context.field_names(res.jsonapi_type)
            if field_names:
                filtered_fields = {new_prefix + n for n in field_names} | include
            else:
                filtered_fields = include
            res_included = to_ressource_data(res.resource_value, included=included, prefix=new_prefix,
                                             include=filtered_fields, exclude=exclude)
            included[res_key] = res_included


def _remove_prefix(set_names: set[str], prefix: str) -> set[str]:
    new_set_names = [i[len(prefix):] for i in set_names if i.startswith(prefix)]
    return {n for n in new_set_names if '.' not in n}
