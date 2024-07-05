import os
import typing as t
import xmlrpc.client

import requests
from aws_xray_sdk.core import xray_recorder
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from werkzeug.exceptions import BadRequest
from werkzeug.exceptions import NotFound

from coworks import TechMicroService
from coworks.extension.xray import XRay
from .jsonapi import JsonApiDataMixin
from .jsonapi import JsonApiDict
from .jsonapi.data import CursorPagination


class OdooConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    url: str
    dbname: str
    user: str
    passwd: str
    const: dict[str, t.Any] = Field(default_factory=dict)  # Used to store properties of this configuration

    @classmethod
    def from_env_var_prefix(cls, env_var_prefix):
        env_url_var_name = f"{env_var_prefix}_URL"
        env_dbname_var_name = f"{env_var_prefix}_DBNAME"
        env_user_var_name = f"{env_var_prefix}_USER"
        env_passwd_var_name = f"{env_var_prefix}_PASSWD"
        return cls.get_config(env_url_var_name, env_dbname_var_name, env_user_var_name, env_passwd_var_name)

    @classmethod
    def get_config(cls, env_url_var_name: str, env_dbname_var_name: str, env_user_var_name: str,
                   env_passwd_var_name: str):
        url = os.getenv(env_url_var_name)
        if not url:
            raise RuntimeError(f'{env_url_var_name} not defined in environment.')
        dbname = os.getenv(env_dbname_var_name)
        if not dbname:
            raise RuntimeError(f'{env_dbname_var_name} not defined in environment.')
        user = os.getenv(env_user_var_name)
        if not user:
            raise RuntimeError(f'{env_user_var_name} not defined in environment.')
        passwd = os.getenv(env_passwd_var_name)
        if not passwd:
            raise RuntimeError(f'{env_passwd_var_name} not defined in environment.')

        return OdooConfig(url=url, dbname=dbname, user=user, passwd=passwd)


class OdooPagination(CursorPagination):
    """Odoo pagination using limit value and odoo request."""
    max_per_page: int | None = None
    query: t.Any | None = None

    @field_validator("max_per_page")
    def set_max_per_page(cls, max_per_page):
        return max_per_page or 100

    def __iter__(self):
        if self.query:
            return iter(self.query.odoo_execute_kw(self.params))
        raise StopIteration()

    @property
    def params(self):
        assert self.page is not None  # by the validator
        assert self.per_page is not None  # by the validator
        if self.page < 1:
            self.page = 1
        return {
            'limit': self.per_page,
            'offset': (self.page - 1) * self.per_page,
        }


class OdooQuery(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    odoo: "Odoo"
    model: str
    method: str
    order: str | None = None
    fields: list[str] | None = None
    domain: list[tuple[str, str, t.Any]] | None
    bind_key: str | None = None  # key to access the configuration defined in binds

    def paginate(self, *, page=None, per_page=None, max_per_page=None) -> OdooPagination:
        pagination = OdooPagination(query=self, page=page, per_page=per_page, max_per_page=max_per_page, total=0)
        res = self.odoo.odoo_execute_kw(self.model, "search_count", [self.domain], bind_key=self.bind_key)
        pagination.total = res
        return pagination

    def all(self, limit=2) -> list[JsonApiDataMixin]:
        """Mainly used only to get one resource so set limit to 2."""
        params = {
            'limit': limit,
            'offset': 0,
        }
        return self.odoo_execute_kw(params)

    def one(self) -> list[JsonApiDataMixin]:
        params = {
            'limit': 1,
            'offset': 0,
        }
        return self.odoo_execute_kw(params)[0]

    def odoo_execute_kw(self, params):
        res = self.odoo.odoo_execute_kw(self.model, self.method, [self.domain], params, bind_key=self.bind_key)
        return [self.odoo.get_basemodel(self.model, rec)(**rec) for rec in res]  # type: ignore[operator]


class Odoo:
    """Flask's extension for Odoo.
    This extension uses the external API of ODOO.

    At creation: a default configuration must be done or a binding dict oj configurations.
    At execution: if no binding key is provided, the default configuration is used.

    .. versionchanged:: 0.7.3
        ``env_var_prefix`` parameter may be a dict of bind values.
        GraphQL removed.
    """

    def __init__(self, app: TechMicroService | None = None,
                 config: OdooConfig | None = None, binds: dict[str | None, OdooConfig] | None = None):

        """
        :param app: Flask application.
        :param config: default configuration if no bind given in execution.
        :param binds: list of configuration binds.
        """

        self.app = None

        self.binds = binds if binds else {}
        if config:
            self.binds[None] = config

        # jsonapi basemodel association from an odoo model and odoo record
        self.get_basemodel = lambda model, record: JsonApiDict

        if app:
            self.init_app(app)

    def init_app(self, app):
        self.app = app

    @XRay.capture(xray_recorder)
    def query(self, model: str, method: str = "search_read", *, fields: list[str] | None = None,
              order: str | None = None, domain: list[tuple[str, str, t.Any]] | None = None,
              bind_key: str | None = None):
        return OdooQuery(odoo=self, model=model, method=method, fields=fields, order=order, domain=domain,
                         bind_key=bind_key)

    @XRay.capture(xray_recorder)
    def kw(self, model: str, method: str = "search_read", *, id: int | None = None, fields: list[str] | None = None,
           order: str | None = None, domain: list[tuple[str, str, t.Any]] | None = None, limit: int = 100,
           page_size: int | None = None, page: int = 0, ensure_one: bool = False, bind_key: str | None = None):
        """Searches with API for records based on the args.
        
        See also: https://www.odoo.com/documentation/14.0/developer/reference/addons/orm.html#odoo.models.Model.search
        @param model: python as a dot separated class name.
        @param method : API method name maybe search_read, search_count
        @param id : id for one element
        @param fields: record fields for result.
        @param order: oder of result.
        @param domain: domain for records.
        @param limit: maximum number of records to return from odoo (default: all).
        @param page_size: pagination done by the microservice.
        @param page: current page searched.
        @param ensure_one: raise error if result is not one (404) and only one (400) object.
        @param bind_key: bind configuration to be used.

        .. versionchanged:: 0.7.3
            Added the ``bind`` parameter.
       """
        if id and domain:
            raise BadRequest("Domain and Id parameters cannot be defined tin same time")
        if id:
            filters = [[('id', '=', id)]]
        else:
            filters = [domain] if domain else [[]]

        params: dict[str, t.Any] = {}
        if order:
            params.update({'order': order})
        if fields:
            params.update({'fields': fields})
        if limit:
            params.update({'limit': limit})
        if page:
            page_size = page_size or limit
            params.update({'offset': (page - 1) * page_size})

        res = self.odoo_execute_kw(model, method, filters, params, bind_key=bind_key)

        if method == 'search_count':
            return res

        if len(res) == 0:
            raise NotFound("No element found.")
        if ensure_one:
            if len(res) != 1:
                raise NotFound("More than one element found and ensure_one parameters was set")
            return res[0]

        return {"ids": [rec['id'] for rec in res], "values": res}

    @XRay.capture(xray_recorder)
    def create(self, model: str, data: list[dict] | None = None, bind_key: str | None = None) -> int:
        """Creates new records for the model.

        See also: https://www.odoo.com/documentation/14.0/developer/reference/addons/orm.html#odoo.models.Model.create
        @param model: python as a dot separated class name.
        @param data: fields, as a list of dictionaries, to initialize and the value to set on them.
        @param bind_key: bind configuration to be used.
        See also:
        https://www.odoo.com/documentation/14.0/developer/reference/addons/orm.html#odoo.models.Model.with_context
        """
        return self.odoo_execute_kw(model, "create", data, bind_key=bind_key)

    @XRay.capture(xray_recorder)
    def write(self, model: str, id: int, data: dict | None = None, bind_key: str | None = None) -> list:
        """Updates one record with the provided values.
        See also: https://www.odoo.com/documentation/14.0/developer/reference/addons/orm.html#odoo.models.Model.write
        @param model: python as a dot separated class name.
        @param id: id of the record.
        @param data: fields to update and the value to set on them.
        @param bind_key: bind configuration to be used.
        """
        return self.odoo_execute_kw(model, "write", [[id], data], bind_key=bind_key)

    @XRay.capture(xray_recorder)
    def delete_(self, model: str, id: int, bind_key: str | None = None) -> list:
        """delete the record.
        See also: https://www.odoo.com/documentation/14.0/developer/reference/addons/orm.html#odoo.models.Model.unlink
        @param model: python as a dot separated class name.
        @param id: id of the record.
        @param bind_key: bind configuration to be used.
        """
        return self.odoo_execute_kw(model, "unlink", [[id]], bind_key=bind_key)

    def get_pdf(self, invoice_id: int, access_token: str, bind_key: str | None = None) -> bytes:
        """Returns the invoice as a PDF invoice.

        @param invoice_id: invoice id.
        @param access_token: access token of this invoice.
        @param bind_key: bind configuration to be used.
        """
        config = self.binds[bind_key]

        url = f"{config.url}/my/invoices/{invoice_id}/?report_type=pdf&download=true&access_token={access_token}"
        resp = requests.get(url)
        if resp.ok:
            return resp.content
        raise NotFound

    def odoo_execute_kw(self, model, method, *args, bind_key: str | None = None, **kwargs):
        """Standard externalm API entries.
        See also: https://www.odoo.com/documentation/15.0/developer/misc/api/odoo.html
        """
        config = self.binds[bind_key]

        if '__uid' not in config.const:
            common = xmlrpc.client.ServerProxy(f'{config.url}/xmlrpc/2/common')
            connected_uid = common.authenticate(config.dbname, config.user, config.passwd, {})
            if not connected_uid:
                raise ConnectionError()
            config.const['__uid'] = connected_uid

        models = xmlrpc.client.ServerProxy(f'{config.url}/xmlrpc/2/object')
        return models.execute_kw(config.dbname, config.const['__uid'], config.passwd, model, method, *args, **kwargs)
