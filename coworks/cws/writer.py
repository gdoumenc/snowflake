import inspect
import pathlib
import sys

import click
from jinja2 import Environment, PackageLoader, select_autoescape, TemplateNotFound

from .command import CwsCommand
from .error import CwsCommandError


class CwsWriterError(CwsCommandError):
    ...


class CwsWriter(CwsCommand):

    def __init__(self, app=None, name='export'):
        super().__init__(app, name=name)

    @property
    def options(self):
        return [
            click.option('-o', '--output'),
            click.option('--debug/--no-debug', default=False, help='Print debug logs to stderr.')
        ]

    def _execute(self, **options):
        self._export_header(**options)
        self._export_content(**options)

    def _export_header(self, **options):
        print("// Do NOT edit this file as it is auto-generated by cws\n", file=self.output, flush=True)

    def _export_content(self, **options):
        """ Main export function.
        :param options: Command options.

        Abstract method which must be redefined in any subclass. The content should be written in self.output.
        """
        if options['debug']:
            print(f"Print content in {self.output.name}")


class CwsTemplateWriter(CwsWriter):
    """Writer with  jinja templating."""

    def __init__(self, app=None, *, name='export', data=None, template=None, env=None):
        super().__init__(app, name=name)
        self.data = data or {}
        self.template_filenames = template or []
        self.env = env or Environment(
            loader=PackageLoader(sys.modules[__name__].__name__),
            autoescape=select_autoescape(['html', 'xml']))

    @property
    def options(self):
        return [
            *super().options,
            click.option('--template', '-t', multiple=True),
        ]

    def _export_content(self, *, project_dir, module, service, workspace, template, **options):
        super()._export_content(**options)

        module_path = module.split('.')
        template_filenames = template or self.template_filenames

        # Get parameters for execution
        try:
            config = next(
                (app_config for app_config in self.app.configs if app_config.workspace == workspace)
            )
        except StopIteration:
            raise CwsCommandError("A workspace is mandatory in the python configuration for deploying.\n")

        environment_variable_files = [p.as_posix() for p in
                                      config.existing_environment_variables_files(project_dir)]
        data = {
            'writer': self,
            'project_dir': project_dir,
            'source_file': pathlib.PurePath(project_dir, *module_path),
            'module': module,
            'module_path': pathlib.PurePath(*module_path),
            'module_dir': pathlib.PurePath(*module_path[:-1]),
            'module_file': module_path[-1],
            'handler': service,
            'app': self.app,
            'ms_name': self.app.name,
            'workspace': workspace,
            'app_config': config,
            'environment_variables': config.environment_variables,
            'environment_variable_files': environment_variable_files,
            'sfn_name': options.get('sfn_name'),
            'account_number': options.get('account_number'),
            'description': inspect.getdoc(self.app) or "",
            **options
        }
        data.update(self.data)
        try:
            for template_filename in template_filenames:
                template = self.env.get_template(template_filename)
                print(template.render(**data), file=self.output, flush=True)
        except TemplateNotFound as e:
            raise CwsWriterError(f"Cannot find template {str(e)}")
        except Exception as e:
            raise CwsWriterError(e)
