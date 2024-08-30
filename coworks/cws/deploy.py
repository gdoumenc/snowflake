import base64
import hashlib
import importlib
import inspect
import os
import subprocess
import sys
import sysconfig
import tempfile
import typing as t
from datetime import datetime
from functools import cached_property
from functools import partial
from pathlib import Path
from shutil import ExecError
from shutil import copyfile
from shutil import copytree
from shutil import ignore_patterns
from shutil import make_archive
from subprocess import CompletedProcess

import boto3
import click
from flask.cli import pass_script_info
from flask.cli import with_appcontext
from jinja2 import BaseLoader
from jinja2 import Environment
from jinja2 import PackageLoader
from jinja2 import select_autoescape
from pydantic import BaseModel
from pydantic import ConfigDict
from werkzeug.routing import Rule

from coworks import aws
from coworks.proxy import get_cws_annotations
from coworks.utils import load_dotenv
from .command import CwsCommand
from .utils import progressbar
from .utils import show_stage_banner
from .utils import show_terraform_banner

UID_SEP = '_'


class TerraformContext:

    def __init__(self, info, ctx):
        self.app = info.load_app()
        self.ctx = ctx

        # Transform flask app import path into module import path
        if info.app_import_path and '/' in info.app_import_path:
            msg = f"Cannot deploy or destroy a project with handler not on project folder : {info.app_import_path}.\n"
            msg += f"Add option -p {'/'.join(info.app_import_path.split('/')[:-1])} to resolve this."""
            raise ModuleNotFoundError()
        self.app_import_path = info.app_import_path.replace(':', '.') if info.app_import_path else "app.app"


class TerraformResource(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    parent_uid: str | None
    path: str | None
    rules: list[Rule] | None = None

    @cached_property
    def uid(self) -> str:

        if self.is_root:
            return ''

        assert self.path is not None, "TerraformResource Path cannot be None"
        uid = self.path.replace('{', '').replace('}', '')

        if self.parent_is_root:
            return uid

        # avoid too long id for ressources
        parent_uid: str = self.parent_uid if len(self.parent_uid) < 80 else str(id(self.parent_uid))  # type: ignore
        return f"{parent_uid}{UID_SEP}{uid}" if self.path else parent_uid

    @cached_property
    def is_root(self) -> bool:
        return self.path is None

    @cached_property
    def parent_is_root(self) -> bool:
        return self.parent_uid == ''

    @cached_property
    def no_cors_methods(self) -> t.Iterator[set[str] | None]:
        if not self.rules:
            return iter(())
        return (rule.methods for rule in self.rules if getattr(rule, 'cws_no_cors', None))

    def __repr__(self):
        return f"{self.uid}:{self.rules}"


class Terraform:
    """Terraform class to manage local terraform commands."""
    TIMEOUT = 600

    def __init__(self, backend: 'TerraformBackend', terraform_dir, workspace):
        root_context = backend.terraform_context.ctx.find_root()
        self.terraform_context = backend.terraform_context
        self.app_context = backend.terraform_context
        self.bar = backend.bar
        self.terraform_dir = terraform_dir
        self.refresh = backend.terraform_refresh
        self.stage = root_context.params['stage']
        self.workspace = workspace
        self.working_dir = Path(root_context.params.get('project_dir'))

    def init(self):
        self._execute(['init', '-input=false'])

    def apply(self) -> None:
        """Executes terraform apply command."""
        try:
            cmd = ['apply', '-auto-approve', '-parallelism=1']
            if not self.refresh:
                cmd.append('-refresh=false')
            self._execute(cmd)
        except ExecError:
            # on cloud parallelism options is not available
            cmd = ['apply', '-auto-approve']
            if not self.refresh:
                cmd.append('-refresh=false')
            self._execute(cmd)

    def output(self):
        values = self._execute(['output']).stdout
        return values.decode("utf-8").strip()

    @property
    def api_resources(self):
        """Returns the list of flatten path (prev_uid, last, rule)."""
        resources: dict[str, TerraformResource] = {}

        def add_rule(previous: str, path: str, rule_: Rule | None):
            """Add a method rule in a resource."""
            # todo : may use now aws_url_map
            path = None if path is None else path.replace('<', '{').replace('>', '}')
            resource = TerraformResource(parent_uid=previous, path=path)
            if rule_:
                view_function = self.app_context.app.view_functions.get(rule_.endpoint)
                setattr(rule_, 'cws_binary_headers', get_cws_annotations(view_function, '__CWS_BINARY_HEADERS'))
                setattr(rule_, 'cws_no_auth', get_cws_annotations(view_function, '__CWS_NO_AUTH'))
                setattr(rule_, 'cws_no_cors', get_cws_annotations(view_function, '__CWS_NO_CORS'))

            # Creates terraform ressources if it doesn't exist.
            uid = resource.uid
            if uid not in resources:
                resources[uid] = resource

            resource = resources[uid]
            if rule_:
                if resources[uid].rules is None:
                    resources[uid].rules = [rule_]
                else:
                    resources[uid].rules.append(rule_)  # type: ignore[union-attr]
            return uid

        for rule in self.app_context.app.url_map.iter_rules():
            route = rule.rule
            previous_uid = ''
            if route.startswith('/'):
                route = route[1:]
            splited_route = route.split('/')

            # special root case
            if splited_route == ['']:
                add_rule('', '', rule)
                continue

            # creates intermediate resources
            for prev in splited_route[:-1]:
                previous_uid = add_rule(previous_uid, prev, None)

            # set entry keys for last entry
            add_rule(previous_uid, splited_route[-1:][0], rule)

        return resources

    @property
    def logger(self):
        return self.app_context.app.logger

    @property
    def template_loader(self) -> BaseLoader:
        package_name = sys.modules[__name__].__package__
        assert package_name is not None
        return PackageLoader(package_name)

    @property
    def jinja_env(self) -> Environment:
        return Environment(loader=self.template_loader, autoescape=select_autoescape(['html', 'xml']),
                           trim_blocks=True, lstrip_blocks=True)

    def get_context_data(self, **options) -> dict:
        # Microservice context data
        app = self.app_context.app
        tf_key = options.pop('tf_key', None) or app.name.replace('_', '/')
        data = {
            'api_resources': self.api_resources,
            'app': app,
            'app_import_path': self.app_context.app_import_path,
            'debug': app.debug,
            'description': inspect.getdoc(app) or "",
            'environment_variables': load_dotenv(self.stage),
            'ms_name': app.name,
            'now': datetime.now().isoformat(),
            'tf_key': tf_key,
            'stage': self.stage,
            'workspace': self.workspace,
            **options
        }

        # AWS context data
        profile_name = options.get('profile_name')
        if profile_name:
            aws_session = boto3.Session(profile_name=profile_name)
            aws_region = aws_session.region_name
            data['aws_region'] = aws_region
            aws_account = aws_session.client("sts").get_caller_identity()["Account"]
            data['aws_account'] = aws_account

        return data

    def _execute(self, cmd_args: list[str]) -> CompletedProcess:
        self.logger.debug(f"Terraform arguments : ['-chdir={self.terraform_dir} {' '.join(cmd_args)}]")
        p = subprocess.run(["terraform", *cmd_args], capture_output=True, cwd=self.terraform_dir,
                           timeout=self.TIMEOUT)
        if p.returncode != 0:
            msg = p.stderr.decode('utf-8')
            if not msg:
                msg = p.stdout.decode('utf-8')
            raise ExecError(msg)
        return p

    def generate_file(self, template_filename, output_filename, **options) -> None:
        """Generates stage terraform files."""
        template = self.jinja_env.get_template(template_filename)
        with (self.terraform_dir / output_filename).open("w") as f:
            data = self.get_context_data(**options)
            f.write(template.render(**data))
            self.logger.debug(f"Terraform file {self.terraform_dir / output_filename} generated")


class TerraformBackend:
    """Terraform class to manage remote deployements."""

    def __init__(self, terraform_context, bar, **options):
        """The remote terraform class correspond to the terraform command interface for workspaces.
        """
        self.terraform_context = terraform_context
        self.app = terraform_context.app
        self.bar = bar

        # Creates terraform dir if needed
        self.terraform_dir = Path(options['terraform_dir'])
        self.stage = terraform_context.ctx.find_root().params['stage']

        self.terraform_class = Terraform
        self.terraform_refresh = options['terraform_refresh']
        self._api_terraform = self._stage_terraform = None

    @property
    def api_terraform(self):
        if self._api_terraform is None:
            self.app.logger.debug(f"Create common terraform instance using {self.terraform_class}")
            self.terraform_dir.mkdir(exist_ok=True)
            self._api_terraform = self.terraform_class(self, terraform_dir=self.terraform_dir, workspace="common")
        return self._api_terraform

    @property
    def stage_terraform(self):
        if self._stage_terraform is None:
            self.app.logger.debug(f"Create {self.stage} terraform instance using {self.terraform_class}")
            terraform_dir = Path(f"{self.terraform_dir}_{self.stage}")
            terraform_dir.mkdir(exist_ok=True)
            self._stage_terraform = self.terraform_class(self, terraform_dir=terraform_dir, workspace=self.stage)
        return self._stage_terraform

    def process_terraform(self, command_template, terraform_init=True, **options):
        root_command_params = self.terraform_context.ctx.find_root().params

        # Set default options calculated value
        options['key'] = options.get('key') or f"{self.app.name}/archive.zip"

        # Transfert zip file to S3
        self.bar.update(msg="Copy source files on S3")
        b64sha256 = self.copy_sources_to_s3(**options)
        options['source_code_hash'] = b64sha256

        self.bar.update(msg="Generates terraform files")

        # Generates common terraform files
        if terraform_init:
            output_filename = "terraform.tf"
            self.app.logger.debug('Generate terraform global files')
            self.api_terraform.generate_file("terraform.j2", output_filename, **root_command_params, **options)
            self.stage_terraform.generate_file("terraform.j2", output_filename, **root_command_params, **options)

        # Generates terraform files and copy environment variable files in terraform working dir for provisionning
        output_filename = f"{self.app.name}.tech.tf"
        self.app.logger.debug(f'Generate terraform {output_filename} files')
        self.api_terraform.generate_file(command_template, output_filename, **root_command_params, **options)
        self.stage_terraform.generate_file(command_template, output_filename, **root_command_params, **options)

        # Stops process if dry
        if options['dry']:
            self.bar.update(msg="Nothing deployed and destroyed (dry mode)")
            return

        if terraform_init:
            self.app.logger.debug("Init terraform")
            self.api_terraform.init()
            assert self._stage_terraform is not None
            self._stage_terraform.init()

        # Apply to api terraform
        self.bar.update(msg="Create or update API")
        self.api_terraform.apply()
        if options.get('api'):
            return

        # Apply to stage terraform
        self.bar.update(msg=f"Deploy staged lambda ({self.stage})")
        self.stage_terraform.apply()

        self.delete_sources_on_s3(**options)

        # Traces output
        self.bar.terminate()
        click.echo()
        echo_output(self.api_terraform)
        click.secho("🎉 CoWorks Microservice deployed", fg="green")

    def copy_sources_to_s3(self, dry, **options):
        module_name = options.get('module_name') or []
        bucket = options.get('bucket')
        key = options.get('key')
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Defines ignore file paternsctx
            ignore = options.get('ignore') or ['.*', 'terraform*']
            if ignore and not isinstance(ignore, list):
                if type(ignore) is tuple:
                    ignore = [*ignore]
                else:
                    ignore = [ignore]
            ignore = [*ignore, '*.pyc', '__pycache__']
            ignore = [*ignore, 'Pipfile*', 'requirements.txt']
            ignore = [*ignore, '*cws.yml', 'env_variables*']
            full_ignore_patterns = partial(ignore_patterns, *ignore)

            # Creates archive
            project_dir = self.terraform_context.ctx.find_root().params.get('project_dir')
            full_project_dir = Path(project_dir).resolve()
            try:
                if tmp_path.relative_to(full_project_dir):
                    msg = f"Cannot deploy a project defined in tmp folder (project dir id {full_project_dir})"
                    raise click.exceptions.UsageError(msg)
            except (Exception,):
                pass

            tmp_sources_path = Path(tmp_path) / 'filtered_dir'
            self.app.logger.debug(f"Copy source file in {tmp_sources_path}")
            copytree(project_dir, tmp_sources_path, ignore=full_ignore_patterns())

            for name in module_name:
                if name.endswith(".py"):
                    file_path = Path(sysconfig.get_path('purelib')) / name
                    copyfile(file_path, tmp_sources_path / name)
                else:
                    mod = importlib.import_module(name)
                    assert mod.__file__ is not None
                    module_path = Path(mod.__file__).resolve().parent
                    copytree(module_path, tmp_sources_path / name, ignore=full_ignore_patterns())
            module_archive = make_archive(str(tmp_path / 'sources'), 'zip', tmp_sources_path)

            # Uploads archive on S3
            with open(module_archive, 'rb') as archive:
                size = int(os.path.getsize(module_archive) / 1000)
                b64sha256 = base64.b64encode(hashlib.sha256(archive.read()).digest()).decode()
                if dry:
                    self.bar.update(msg=f"Nothing copied (dry mode, {size} Kb)")
                else:
                    archive.seek(0)
                    try:
                        profile_name = options.get('profile_name')
                        aws_s3_session = aws.AwsS3Session(profile_name=profile_name)
                        aws_s3_session.client.upload_fileobj(archive, bucket, key)
                    except Exception as e:
                        raise e

                    self.app.logger.debug(msg=f"Successfully uploaded sources at s3://{bucket}/{key}")
                    self.bar.update(msg=f"Sources files copied ({size} Kb)")

            return b64sha256

    def delete_sources_on_s3(self, **options):
        bucket = options.get('bucket')
        key = options.get('key')
        profile_name = options.get('profile_name')
        aws_s3_session = aws.AwsS3Session(profile_name=profile_name)
        aws_s3_session.client.delete_object(Bucket=bucket, Key=key)
        self.bar.update(msg="Sources files deleted on s3")


@click.command("deploy", CwsCommand, short_help="Deploy the CoWorks microservice on AWS Lambda.")
@click.option('--bucket', '-b',
              help="Bucket to upload sources zip file to", required=True)
@click.option('--dry', is_flag=True,
              help="Doesn't perform deploy [Global option only].")
@click.option('--ignore', '-i', multiple=True,
              help="Ignore pattern when copying source to lambda.")
@click.option('--key', '-k',
              help="Sources zip file bucket's name.")
@click.option('--module-name', '-m', multiple=True,
              help="Python module added from current pyenv (module or file.py).")
@click.option('--profile-name', '-pn', required=True,
              help="AWS credential profile.")
@click.option('--binary-types', multiple=True,
              help="Content types defined as binary contents (no encoding).")
@click.option('--json-types', multiple=True,
              help="Add mime types for JSON response.")
@click.option('--layers', '-l', multiple=True, required=True,
              help="Add layer (full arn: aws:lambda:...). Must contains CoWorks at least.")
@click.option('--memory-size', default=128,
              help="Lambda memory size (default 128).")
@click.option('--python', '-p', type=click.Choice(['3.8', '3.9', '3.10', '3.11']), default='3.11',
              help="Python version for the lambda.")
@click.option('--tf-bucket',
              help="Bucket's name if not using cloud terraform.")
@click.option('--tf-key',
              help="Bucket's key if not using cloud terraform (default microservice's name).")
@click.option('--security-groups', multiple=True, default=[],
              help="Security groups to be added [ids].")
@click.option('--subnets', multiple=True, default=[],
              help="Subnets to be added [ids].")
@click.option('--terraform-cloud', '-tc', is_flag=True, default=False,
              help="Use cloud workspaces (default false).")
@click.option('--terraform-dir', '-td', default="terraform",
              help="Terraform files folder (default terraform).")
@click.option('--terraform-organization', '-to',
              help="Terraform organization (needed if using cloud terraform).")
@click.option('--terraform-refresh', '-tr', is_flag=True, default=True,
              help="Forces terraform to refresh the state (default true).")
@click.option('--text-types', multiple=True,
              help="Add mime types for JSON response [at least text/plain, text/html].")
@click.option('--timeout', default=60,
              help="Lambda timeout (default 60s).Only for asynchronous call (API call 30s).")
@click.option('--token-key',
              help="Header token key.")
@click.pass_context
@pass_script_info
@with_appcontext
def deploy_command(info, ctx, **options) -> None:
    """ Deploiement in 2 steps:
        Step 1. Create API and routes integrations
        Step 2. Deploy API and Lambda
    """
    if options.get('terraform_cloud') and not options.get('terraform_organization'):
        raise click.BadParameter('An organization must be defined if using cloud terraform')
    if not options.get('terraform_cloud') and not options.get('tf_bucket'):
        raise click.BadParameter('A bucket must be defined if using terraform on S3 [--tf-bucket option]')

    terraform_context = TerraformContext(info, ctx)
    app = terraform_context.app
    stage = ctx.parent.params['stage']

    app.logger.debug(f"Start deploy command: {options}")
    show_stage_banner(stage)
    cloud = options['terraform_cloud']
    refresh = options['terraform_refresh']
    show_terraform_banner(cloud, refresh)
    with progressbar(label="Deploy microservice", threaded=not app.debug) as bar:  # type: ignore
        terraform_backend_class = options.pop('terraform_class', TerraformBackend)
        app.logger.debug(f"Deploying {app} using {terraform_backend_class}")
        backend = terraform_backend_class(terraform_context, bar, **options)
        backend.process_terraform('deploy.j2', **options)


@click.command("destroy", CwsCommand, short_help="Destroy the CoWorks microservice on AWS Lambda.")
# Zip options (redefined)
@click.option('--bucket', '-b', required=True,
              help="Bucket to upload sources zip file to")
@click.option('--dry', is_flag=True,
              help="Doesn't perform deploy [Global option only].")
@click.option('--key', '-k',
              help="Sources zip file bucket's name.")
@click.option('--profile-name', '-pn', required=True,
              help="AWS credential profile.")
@click.option('--terraform-cloud', is_flag=True, default=False,
              help="Use cloud workspaces (default false).")
@click.option('--terraform-dir', '-td', default="terraform",
              help="Terraform files folder (default terraform).")
@click.option('--terraform-organization', '-to',
              help="Terraform organization needed if using cloud terraform.")
@click.option('--terraform-refresh', '-tr', is_flag=True, default=True,
              help="Forces terraform to refresh the state (default true).")
@click.pass_context
@pass_script_info
@with_appcontext
def destroy_command(info, ctx, **options) -> None:
    """ Destroy by setting counters to 0.
    """
    terraform_context = TerraformContext(info, ctx)
    stage = ctx.parent.params['stage']

    app = terraform_context.app
    app.logger.debug(f"Start destroy command: {options}")
    show_stage_banner(stage)
    cloud = options['terraform_cloud']
    refresh = options['terraform_refresh']
    show_terraform_banner(cloud, refresh)
    with progressbar(label='Destroy microservice', threaded=not app.debug) as bar:  # type: ignore
        terraform_backend_class = options.pop('terraform_class', TerraformBackend)
        app.logger.debug(f'Destroying {app} using {terraform_backend_class}')
        backend = terraform_backend_class(terraform_context, bar, **options)
        backend.process_terraform('destroy.j2', terraform_init=False, **options)
    if not options['dry']:
        click.echo(f"You can now delete the terraform_{options.get('stage')} folder.")


@click.command("deployed", CwsCommand, short_help="Retrieve the microservices deployed for this project.")
@click.option('--terraform-dir', default="terraform",
              help="Terraform folder (default terraform).")
@click.option('--terraform-cloud', is_flag=True, default=False,
              help="Use cloud workspaces (default false).")
@click.option('--terraform-refresh', '-tr', is_flag=True, default=True,
              help="Forces terraform to refresh the state (default true).")
@click.pass_context
@pass_script_info
@with_appcontext
def deployed_command(info, ctx, **options) -> None:
    terraform_context = TerraformContext(info, ctx)
    app = terraform_context.app
    stage = ctx.parent.params['stage']

    app.logger.debug(f"Start destroy command: {options}")
    show_stage_banner(stage)
    cloud = options['terraform_cloud']
    refresh = options['terraform_refresh']
    show_terraform_banner(cloud, refresh)
    with progressbar(label='Retrieving information', threaded=not app.debug) as bar:  # type: ignore
        terraform_backend_class = options.pop('terraform_class', TerraformBackend)
        app.logger.debug(f'Destroying {app} using {terraform_backend_class}')
        backend = terraform_backend_class(terraform_context, bar, **options)
    echo_output(backend.api_terraform)


def echo_output(terraform):
    """Pretty print terraform output.
    """
    rows = terraform.output()
    for row in rows.split('\n'):
        values = row.split('=')
        if len(values) > 1:
            cws_name = values[0].strip()[:-3]  # remove last _id
            api_id = values[1].strip()[1:-1]  # remove quotes
            api_url = f"https://{api_id}.execute-api.eu-west-1.amazonaws.com/"
            click.secho(f"The microservice {cws_name} is deployed at {api_url}", fg='yellow')
