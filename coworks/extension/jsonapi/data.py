from __future__ import annotations

import typing as t
from math import ceil
from typing import overload

from jsonapi_pydantic.v1_0 import Link
from jsonapi_pydantic.v1_0 import Relationship
from jsonapi_pydantic.v1_0 import Resource
from jsonapi_pydantic.v1_0 import ResourceIdentifier
from pydantic import BaseModel
from pydantic import HttpUrl
from pydantic import field_validator
from werkzeug.exceptions import InternalServerError

from coworks import StrDict
from coworks import StrSet
from coworks.extension import jsonapi


class CursorPagination(BaseModel):
    """Pagination based on a cursor model (total and per_page must be defined)"""
    total: int
    page: int | None
    per_page: int | None

    @field_validator("page")
    def set_page(cls, page):
        return page or 1

    @field_validator("per_page")
    def set_per_page(cls, per_page):
        return per_page or 20

    @property
    def pages(self) -> int:
        if not self.total:
            return 1
        assert self.per_page is not None  # by the validator
        return ceil(self.total / self.per_page)

    @property
    def has_prev(self) -> bool:
        assert self.page is not None  # by the validator
        return self.page > 1

    @property
    def prev_num(self) -> int | None:
        if not self.has_prev:
            return None
        assert self.page is not None  # by the validator
        return self.page - 1

    @property
    def has_next(self) -> bool:
        assert self.page is not None  # by the validator
        return self.page < self.pages

    @property
    def next_num(self) -> int | None:
        if not self.has_next:
            return None
        assert self.page is not None  # by the validator
        return self.page + 1


class JsonApiRelationship:
    """Relationship information for jsonapi.
    The id may be given independently of the value.
    """

    def __init__(self, *, type_: str, id_: str, value: JsonApiDataMixin | None = None):
        self.jsonapi_type = type_
        self.jsonapi_id = id_
        self.value = value

    @property
    def resource_value(self) -> JsonApiDataMixin | None:
        return self.value


class JsonApiDataMixin:
    """Any data structure which may be transformed to JSON:API resource.
    """

    @property
    def jsonapi_type(self) -> str:
        return ''

    @property
    def jsonapi_id(self) -> str:
        return ''

    @property
    def jsonapi_self_link(self):
        return "https://monsite.com/missing_entry"

    def jsonapi_attributes(self, include: StrSet, exclude: StrSet) \
            -> tuple[dict[str, t.Any], StrDict[list[JsonApiRelationship] | JsonApiRelationship]]:
        """Splits the structure in attributes versus relationships.

        :param include: included attributes or relationships
        :param exclude: excluded attributes or relationships
        """
        return {}, {}

    def to_resource(self, *, included: StrDict[Resource] | None = None, include: StrSet | None = None,
                    exclude: StrSet | None = None, prefix: str | None = None) \
            -> tuple[Resource, StrDict[Resource]]:
        """Returns:
         * the data of the toplelevel structure
         * the list of included resources extracted from the data
        Beware : included is a dict of type/id key (jsonapi_type + jsonapi_id) and jsonapi ressource value

        :param include: the set of fields to add in the included resources
        :param exclude: the set of fields to exclude from the included resources
        :param prefix: the prefix of the included resources (indirect inclusion)
        """
        prefix = prefix or ''
        include = include or jsonapi.fetching_context.field_names(self.jsonapi_type)
        exclude = exclude or set()
        included = included or {}

        # set resource data from basemodel
        attrs, rels = self.jsonapi_attributes(
            include=_remove_prefix(include, prefix), exclude=_remove_prefix(exclude, prefix)
        )
        include = include or set(rels) - exclude

        # The type may be defined by the class
        _type = self.jsonapi_type
        if _type:
            if 'type' in attrs:
                attrs['_type'] = attrs.pop('type')
        else:
            _type = attrs.pop('type')

        if 'id' in attrs:
            _id = str(attrs.pop('id'))
        else:
            _id = self.jsonapi_id

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
                    res_id = _get_resource_identifier(val)
                    res_ids.append(res_id)
                    _add_to_included(included, key, val, include=include, exclude=exclude, prefix=prefix)
                relationships[key] = Relationship(data=res_ids)
            else:
                res_id = _get_resource_identifier(rel)
                _add_to_included(included, key, rel, include=include, exclude=exclude, prefix=prefix)
                relationships[key] = Relationship(data=res_id)

        resource_data = {
            "type": _type,
            "id": _id,
            "lid": None,
            "attributes": attrs,
            # "links": _get_resource_links(self)
        }

        if relationships:
            resource_data["relationships"] = relationships

        return Resource(**resource_data), included


class JsonApiBaseModel(BaseModel, JsonApiDataMixin):
    """BaseModel data for JSON:API resource"""

    def jsonapi_attributes(self, include: set[str], exclude: set[str]) \
            -> tuple[dict[str, t.Any], dict[str, list[JsonApiRelationship] | JsonApiRelationship]]:
        attrs: dict[str, t.Any] = {}
        rels: dict[str, list[JsonApiRelationship] | JsonApiRelationship] = {}
        for k in {*self.model_fields.keys(), *self.model_computed_fields.keys()}:
            if (include and k not in include) or k in exclude:
                continue

            v = getattr(self, k)
            if self._is_basemodel(v):
                rels[k] = self.create_relationship(v)
            elif not include or k in include:
                attrs[k] = v
        return attrs, rels

    @overload
    def create_relationship(self, value: JsonApiBaseModel) -> JsonApiRelationship:
        ...

    @overload
    def create_relationship(self, value: list[JsonApiBaseModel]) -> list[JsonApiRelationship]:
        ...

    def create_relationship(self, value):
        if self._is_list_or_set(value):
            return [self.create_relationship(x) for x in value]
        return JsonApiRelationship(type_=value.jsonapi_type, id_=value.jsonapi_id, value=value)

    def _is_basemodel(self, v) -> bool:
        if not v:
            return False
        if isinstance(v, JsonApiBaseModel):
            return True
        if self._is_list_or_set(v) and isinstance(next(iter(v)), JsonApiBaseModel):
            return True
        return False

    def _is_list_or_set(self, v):
        return isinstance(v, list) or isinstance(v, set)


class JsonApiDict(dict, JsonApiDataMixin):
    """Dict data for JSON:API resource"""

    @property
    def jsonapi_type(self) -> str:
        return self['type']

    @property
    def jsonapi_id(self) -> str:
        return str(self['id'])

    def jsonapi_attributes(self, include: set[str], exclude: set[str]) \
            -> tuple[dict[str, t.Any], dict[str, list[JsonApiRelationship] | JsonApiRelationship]]:
        attrs = {k: v for k, v in self.items() if (not include or k in include)}
        return attrs, {}


def _get_resource_identifier(rel: JsonApiRelationship):
    """ Adds a relationship in the list of relationships from the related model.
    The relationship may not be complete for circular reference and will be completed after in construction.
    """
    if not isinstance(rel, JsonApiRelationship):
        msg = f"Relationship value must be of type JsonApiRelationship, not {rel.__class__}"
        raise InternalServerError(msg)

    type_ = rel.jsonapi_type
    id_ = rel.jsonapi_id
    return ResourceIdentifier(type=type_, id=id_)


def _get_resource_links(jsonapi_basemodel) -> dict:
    """Get the links associated to a ressource (from jsonapi_self_link property).

    If jsonapi_self_link is a string then there is only one self link.
    """
    self_link = jsonapi_basemodel.jsonapi_self_link
    if isinstance(self_link, str):
        return {'self': Link(href=HttpUrl(jsonapi_basemodel.jsonapi_self_link))}
    if isinstance(self_link, dict):
        return self_link
    raise InternalServerError("Unexpected jsonapi_self_link value")


def _add_to_included(included: StrDict[Resource], key: str, res: JsonApiRelationship, *, prefix: str, include, exclude):
    """Adds the resource defined at key to the included list of resources.

    :param included: list of included resources to increment (if not already inside).
    :param key: the key where the resource is in the parent resource.
    :param res: the relationship to include.
    :param include: set of included resources
    :param exclude: set of excluded resources
    :param included_prefix: dot separated path in resource.
    """
    res_key = _included_key(res)

    # Adds only if not already added in the included set
    if res_key not in included:
        if res.resource_value:
            included[res_key] = None

            # Creates and includes the resource
            new_prefix = f"{prefix}{key}." if prefix else f"{key}."
            field_names = jsonapi.fetching_context.field_names(res.jsonapi_type)
            if field_names:
                filtered_fields = {new_prefix + n for n in field_names} | include
            else:
                filtered_fields = include
            res_included, incl = res.resource_value.to_resource(included=included, prefix=new_prefix,
                                                                include=filtered_fields, exclude=exclude)
            included[res_key] = res_included
            included.update(incl)


def _included_key(res: JsonApiRelationship) -> str:
    return res.jsonapi_type + res.jsonapi_id


def _remove_prefix(set_names: set[str], prefix: str) -> set[str]:
    new_set_names = [i[len(prefix):] for i in set_names if i.startswith(prefix)]
    return {n for n in new_set_names if '.' not in n}
