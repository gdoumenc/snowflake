import pathlib
import click
from abc import abstractmethod
from dataclasses import dataclass
from typing import List

from jinja2 import Environment, PackageLoader, select_autoescape, TemplateNotFound

from coworks import TechMicroService
from coworks.config import CORSConfig
from coworks.cws.command import CwsCommand

DEFAULT_STEP = 'update'


class WriterError(Exception):
    ...


class CwsWriter(CwsCommand):

    def __init__(self, app=None, *, name):
        super().__init__(app, name=name)

    @property
    def options(self):
        return (
            click.option('--output', default=None),
            click.option('--step', default=DEFAULT_STEP),
            click.option('--config', default=None),
            click.option('--debug/--no-debug', default=False, help='Print debug logs to stderr.')
        )

    def _execute(self, **kwargs):
        self._export_header(**kwargs)
        self._export_content(**kwargs)

    def _export_header(self, **kwargs):
        ...

    @abstractmethod
    def _export_content(self, **kwargs):
        """ Main export function.
        :param kwargs: Environment parameters for export.
        :return: None.

        Abstract method which must be redefined in any subclass. The content should be written in self.output.
        """

    def _format(self, content):
        return content


class CwsTemplateWriter(CwsWriter):

    def __init__(self, app=None, *, name='export', data=None, template_filenames=None, env=None):
        super().__init__(app, name=name)
        self.data = data or {}
        self.template_filenames = template_filenames or self.default_template_filenames
        self.env = env or Environment(
            loader=PackageLoader("coworks.cws.writer"),
            autoescape=select_autoescape(['html', 'xml'])
        )

    @property
    @abstractmethod
    def default_template_filenames(self):
        ...

    def _export_content(self, *, project_dir, module, service, workspace, step, variables=None, **kwargs):
        module_path = module.split('.')

        export_config = kwargs['config']
        if export_config:
            common_export_config = next((config for config in export_config if config.get("workspace") is None))
            for c in export_config:
                if c.get('workspace') is not None:
                    for key, value in common_export_config.items():
                        if key not in c:
                            c[key] = value

        data = {
            'writer': self,
            'project_dir': project_dir,
            'module': module,
            'module_path': pathlib.PurePath(*module_path),
            'module_dir': pathlib.PurePath(*module_path[:-1]),
            'module_file': module_path[-1],
            'handler': service,
            'app': self.app,
            'ms_name': self.app.ms_name,
            'variables': variables,
            'deploy_services': list(kwargs.get('deploy_services', [])),
            'step': step,
            'app_config': next((app_config for app_config in self.app.configs if app_config.workspace == workspace)),
            'export_config': export_config,
        }

        data.update(self.data)
        try:
            for template_filename in self.template_filenames:
                template = self.env.get_template(template_filename)
                print(self._format(template.render(**data)), file=self.output)
        except TemplateNotFound as e:
            raise WriterError(f"Cannot find template {str(e)}")
        except Exception as e:
            raise WriterError(e)


UID_SEP = '_'


@dataclass
class TerraformEntry:
    app: TechMicroService
    parent_uid: str
    path: str
    methods: List[str]
    cors: CORSConfig

    @property
    def uid(self):
        def remove_brackets(path):
            return f"{path.replace('{', '').replace('}', '')}"

        if self.path is None:
            return UID_SEP

        last = remove_brackets(self.path)
        return f"{self.parent_uid}{UID_SEP}{last}" if self.parent_uid else last

    @property
    def is_root(self):
        return self.path is None

    @property
    def parent_is_root(self):
        return self.parent_uid == UID_SEP

    def __repr__(self):
        return f"{self.uid}:{self.methods}"


class CwsTerraformWriter(CwsTemplateWriter):

    def __init__(self, app=None, *, name='terraform', data=None, **kwargs):

        data = data or {
            'layer_zip_file': 'layer.zip',
        }
        super().__init__(app, name=name, data=data, **kwargs)

    @property
    def default_template_filenames(self):
        return ['terraform.j2']

    def _export_header(self, **kwargs):
        print("// Do NOT edit this file as it is auto-generated by cws\n", file=self.output)

    @property
    def entries(self):
        """Returns the list of flatten path (prev, last, keys)."""
        all_pathes_id = {}

        def add_entry(previous, last, meth):
            entry = TerraformEntry(self.app, previous, last, meth, self.app.config.cors)
            uid = entry.uid
            if uid not in all_pathes_id:
                all_pathes_id[uid] = entry
            if all_pathes_id[uid].methods is None:
                all_pathes_id[uid].methods = meth
            return uid

        for route, methods in self.app.routes.items():
            previous_uid = UID_SEP
            splited_route = route[1:].split('/')

            # special root case
            if splited_route == ['']:
                add_entry(None, None, methods.keys())
                continue

            # creates intermediate resources
            last_path = splited_route[-1:][0]
            for prev in splited_route[:-1]:
                previous_uid = add_entry(previous_uid, prev, None)

            # set entryes keys for last entry
            add_entry(previous_uid, last_path, methods.keys())

        return all_pathes_id


class CwsTerraformStagingWriter(CwsTerraformWriter):
    def __init__(self, app=None, *, name='terraform-staging', data=None, **kwargs):
        super().__init__(app, name=name, data=data, **kwargs)

    @property
    def default_template_filenames(self):
        return ['terraform_staging.j2']
