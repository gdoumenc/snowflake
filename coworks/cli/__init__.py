import json
import os
import shutil
import sys
from tempfile import SpooledTemporaryFile

import click
from chalice import BadRequestError
from chalice.cli import CONFIG_VERSION, DEFAULT_STAGE_NAME, DEFAULT_APIGATEWAY_STAGE_NAME
from chalice.cli import chalice_version, get_system_info
from chalice.utils import serialize_to_json
from coworks import BizFactory
from coworks.cli.sfn import StepFunctionWriter
from coworks.cli.writer import Writer, TerraformWriter, WriterError
from coworks.version import __version__

from .factory import CWSFactory


@click.group()
@click.version_option(version=__version__,
                      message=f'%(prog)s %(version)s, chalice {chalice_version}, {get_system_info()}')
@click.option('--project-dir',
              help='The project directory path (absolute or relative).'
                   'Defaults to CWD')
@click.pass_context
def client(ctx, project_dir=None):
    if project_dir is None:
        project_dir = os.getcwd()
    elif not os.path.isabs(project_dir):
        project_dir = os.path.abspath(project_dir)
    ctx.obj['project_dir'] = project_dir


@client.command('init')
@click.option('--force/--no-force', default=False,
              help='Forces project reinitialization.')
@click.pass_context
def init(ctx, force):
    """Init chalice configuration file."""
    project_name = os.path.basename(os.path.normpath(ctx.obj['project_dir']))

    chalice_dir = os.path.join('.chalice')
    if os.path.exists(chalice_dir):
        if force:
            shutil.rmtree(chalice_dir)
            created = False
        else:
            sys.stderr.write(f"Project {project_name} already initialized\n")
            return
    else:
        created = True

    os.makedirs(chalice_dir)
    config = os.path.join('.chalice', 'config.json')
    cfg = {
        'version': CONFIG_VERSION,
        'app_name': project_name,
        'stages': {
            DEFAULT_STAGE_NAME: {
                'api_gateway_stage': DEFAULT_APIGATEWAY_STAGE_NAME,
            }
        }
    }
    with open(config, 'w') as f:
        f.write(serialize_to_json(cfg))

    if created:
        sys.stdout.write(f"Project {project_name} initialized\n")
    else:
        sys.stdout.write(f"Project {project_name} reinitialized\n")


@client.command('run')
@click.option('-m', '--module', default='app',
              help="Filename of your microservice python source file.")
@click.option('-a', '--app', default='app',
              help="Coworks application in the source file.")
@click.option('-h', '--host', default='127.0.0.1')
@click.option('-p', '--port', default=8000, type=click.INT)
@click.option('-s', '--stage', default=DEFAULT_STAGE_NAME, type=click.STRING,
              help="Name of the Chalice stage for the local server to use.")
@click.option('--debug/--no-debug', default=False,
              help='Print debug logs to stderr.')
@click.pass_context
def run(ctx, module, app, host, port, stage, debug):
    """Runs local server."""
    handler = CWSFactory.import_attr(module, app, cwd=ctx.obj['project_dir'])
    handler.run(host=host, port=port, stage=stage, debug=debug, project_dir=ctx.obj['project_dir'])


@client.command('export')
@click.option('-m', '--module', default='app',
              help="Filename of your microservice python source file.")
@click.option('-a', '--app', default='app',
              help="Coworks application in the source file.")
@click.option('-b', '--biz', default=None,
              help="BizMicroservice name.")
@click.option('-f', '--format', default='terraform')
@click.option('-o', '--out')
@click.pass_context
def export(ctx, module, app, biz, format, out):
    """Exports microservice description in other descrioption languages."""
    try:
        handler = export_to_file(module, app, format, out, project_dir=ctx.obj['project_dir'], biz=biz)
        if handler is None:
            sys.exit(1)
    except WriterError:
        sys.exit(1)


@client.command('update')
@click.option('-m', '--module', default='app',
              help="Filename of your microservice python source file.")
@click.option('-a', '--app', default='app',
              help="Coworks application in the source file.")
@click.option('-b', '--biz', required=True, help="BizMicroservice name.")
@click.option('-p', '--profile', default=None)
@click.pass_context
def update(ctx, module, app, biz, profile):
    out = SpooledTemporaryFile(mode='w+')
    handler = export_to_file(module, app, 'sfn', out, project_dir=ctx.obj['project_dir'], biz=biz)
    if handler is None:
        sys.exit(1)
    handler.aws_profile = profile

    out.seek(0)
    if isinstance(handler, BizFactory):
        try:
            sfn_name = f"{module.split('.')[-1]}-{biz}"
            update_sfn(handler, sfn_name, out.read())
        except BadRequestError:
            sys.stderr.write(f"Cannot update undefined step function '{sfn_name}'.\n")
            sys.exit(1)

    else:
        sys.stderr.write(f"Update not defined on {type(handler)}.\n")
        sys.exit(1)


def export_to_file(module, app, _format, out, **kwargs):
    try:
        handler = CWSFactory.import_attr(module, app, cwd=kwargs['project_dir'])
        _writer: Writer = handler.extensions['writers'][_format]
    except (AttributeError, ModuleNotFoundError):
        sys.stderr.write(f"Module '{module}' has no service {app}\n")
        return
    except KeyError:
        sys.stderr.write(f"Format '{_format}' undefined (you haven't add a {_format} writer to {app} )\n")
        return

    _writer.export(output=out, module_name=module, handler_name=app, **kwargs)
    return handler


def update_sfn(handler, biz, src):
    client = handler.sfn_client
    sfn_arn = handler.get_sfn(biz)
    response = client.update_state_machine(stateMachineArn=sfn_arn, definition=src)
    print(response)


def main():
    return client(obj={})
