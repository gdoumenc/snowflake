import inspect
import os
import sys
import typing as t
from collections import defaultdict
from inspect import Parameter
from textwrap import dedent

import markdown
from flask import abort
from flask import current_app
from flask import render_template_string
from jinja2 import Environment
from jinja2 import PackageLoader
from jinja2 import select_autoescape
from jsonapi_pydantic.v1_0 import Resource
from jsonapi_pydantic.v1_0 import TopLevel
from pydantic import BaseModel
from werkzeug.exceptions import NotFound

from coworks import Blueprint
from coworks import entry
from coworks.proxy import get_cws_annotations


class Admin(Blueprint):
    """Administration blueprint to get details on the microservice containing it.
    """

    def __init__(self, name: str = 'admin', models: t.Optional[list[type[BaseModel]]] = None, **kwargs):
        super().__init__(name=name, **kwargs)
        self.models = {model.__name__: model for model in models} if models else {}

    @entry(no_auth=True, no_cors=True)
    def get(self):
        """Returns the markdown documentation associated to this microservice.
        """
        md = getattr(current_app, 'doc_md', None)
        if not md:
            md = getattr(current_app.__class__, 'DOC_MD', None)
        if not md and current_app.__class__.__doc__:
            md = current_app.__class__.__doc__.replace('\n', ' ').strip()
        content = markdown.markdown(md, extensions=['fenced_code']) if md else ""

        routes = dict(sorted(self.get_route(blueprint="__all__").items()))
        bottom = render_template_string(self.routes_template, routes=routes)

        headers = {
            "Content-Type": 'text/html; charset=utf-8'
        }
        content = self.header_template + '<hr/>' + content + '<hr/>' + bottom + '\n'
        return content, 200, headers

    @entry(no_auth=True, no_cors=True)
    # @jsonapi(type)
    def get__schema(self, model):
        """Returns the JSON schemas of the model.

        :param model: Model's name.
       """
        if model in self.models:
            schema = self.models[model].schema()
            return TopLevel(
                data=Resource(id=model, type="JsonApiSchema", attributes={"schema": schema})).model_dump_json()
        abort(404)

    @entry(stage="dev")
    def get_route(self, prefix=None, blueprint=None):
        """Returns the list of entrypoints with signature.

        :param prefix: Prefix path to limit the number of returned routes.
        :param blueprint: Show named blueprint routes if defined ('__all__' or blueprint name).
        """
        if blueprint and (blueprint != '__all__' and blueprint not in current_app.blueprints):
            raise NotFound(f"Undefined blueprint {blueprint}")

        routes: dict[str, dict[str, str]] = defaultdict(dict)

        for rule in current_app.url_map.iter_rules():

            # Must return only prefixed routes
            if prefix:
                if rule.rule.startswith(prefix):
                    self.add_route_from_rule(routes, rule)
                else:
                    continue

            function_called = current_app.view_functions[rule.endpoint]
            from_blueprint = get_cws_annotations(function_called, '__CWS_FROM_BLUEPRINT')

            # Must return only blueprint routes
            if blueprint:
                if blueprint == '__all__' or from_blueprint == blueprint:
                    self.add_route_from_rule(routes, rule)
                else:
                    continue

            if from_blueprint is None:
                self.add_route_from_rule(routes, rule)

        return routes

    def get_proxy(self):
        """Returns the calling context."""
        env = Environment(
            loader=PackageLoader(sys.modules[__name__].__name__),
            autoescape=select_autoescape(['html', 'xml']))
        env.filters["signature"] = inspect.signature
        env.filters["positional_params"] = positional_params
        env.filters["keyword_params"] = keyword_params

        data = {
            'name': current_app.name,
            'entries': current_app.url_map,
        }
        template = env.get_template("proxy.j2")
        return template.render(**data)

    def add_route_from_rule(self, routes, rule):
        route = {}
        for http_method in rule.methods:
            if http_method not in ['HEAD', 'OPTIONS']:
                function_called = current_app.view_functions[rule.endpoint]
                route[http_method] = {
                    'signature': get_signature(function_called),
                    'endpoint': rule.endpoint,
                    'binary_headers': get_cws_annotations(function_called, '__CWS_BINARY_HEADERS'),
                    'no_auth': get_cws_annotations(function_called, '__CWS_NO_AUTH'),
                    'no_cors': get_cws_annotations(function_called, '__CWS_NO_CORS'),
                }

                from_blueprint = get_cws_annotations(function_called, '__CWS_FROM_BLUEPRINT', None)
                if from_blueprint:
                    route[http_method]['blueprint'] = from_blueprint

                doc = inspect.getdoc(function_called)
                if doc:
                    docstring = doc.replace('\n', ' ').split(':param ')
                    route[http_method]['doc'] = docstring[0].strip()
                    if len(docstring) > 1:
                        # noinspection PyTypeChecker
                        route[http_method]['params'] = docstring[1:]

        routes[rule.rule].update(route)

    @property
    def header_template(self) -> str:
        deployed = os.getenv("CWS_DATETIME", None)
        description = current_app.name
        if deployed:
            lambda_name = os.getenv("CWS_LAMBDA", description)
            description = f"{lambda_name} deployed {deployed}"
        return dedent(f"""<div style=\"display:flex;justify-content:space-between;\">
            <span style=\"font-size:xx-large;font-weight:bold\">{current_app.__class__.__name__}</span>
            <img style=\"margin-bottom:auto;width:100px;\"
            src=\"https://github.com/gdoumenc/coworks/raw/dev/docs/img/coworks.png\"/>
            </div><div style=\"display:flex;flex-direction:row-reverse;font-size:small;margin-top:5px;\">
            {description}</div>""")

    @property
    def routes_template(self) -> str:
        return dedent(
            """<style type="text/css">ul.nobull {list-style-type: none;}</style>
            <ul class="nobull">{% for entry,route in routes.items() %}
                <li>{{ entry }} : <ul>{% for method,info in route.items() %}
                    <li><i>{{ method }}{{ info.signature }}[endpoint: {{info.endpoint}}]</i> : {{ info.doc }}
                    <ul class="nobull">{% for param in info.params %}<li><i>{{ param }}</i>{% endfor %}</ul>
                {% endfor %}</li></ul></li>
            {% endfor %}</ul>"""
        )


def get_signature(func: t.Callable) -> str:
    sig = ""
    params = inspect.signature(func).parameters
    for i, (k, p) in enumerate(params.items()):
        if i == 0:
            continue
        sp = k
        if p.annotation != Parameter.empty:
            sp = f"{sp}:{str(p.annotation)}"
        if p.default != Parameter.empty:
            sp = f"{sp}={p.default}"
        sig = f"{sp}" if i == 1 else f"{sig}, {sp}"
    return f"({sig})"


def positional_params(func: t.Callable) -> str:
    res = ''
    params = inspect.signature(func).parameters
    for i, (k, p) in enumerate(params.items()):
        if i == 0 or p.kind not in [Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD,
                                    Parameter.VAR_POSITIONAL]:
            continue
        if p.kind == Parameter.POSITIONAL_OR_KEYWORD and p.default != Parameter.empty:
            continue
        sp = f"'{k}' : {k}"
        res = f"{sp}" if i == 1 else f"{res}, {sp}"
    return res


def keyword_params(func: t.Callable) -> str:
    res = ''
    params = inspect.signature(func).parameters
    for i, (k, p) in enumerate(params.items()):
        if i == 0 or p.kind not in [Parameter.KEYWORD_ONLY, Parameter.POSITIONAL_OR_KEYWORD, Parameter.VAR_KEYWORD]:
            continue
        if p.kind == Parameter.POSITIONAL_OR_KEYWORD and p.default == Parameter.empty:
            continue
        sp = f"'{k}' : {k}"
        res = f"{sp}" if i == 1 else f"{res}, {sp}"
    return res
