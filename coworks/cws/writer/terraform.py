from dataclasses import dataclass
from typing import List

from coworks import TechMicroService
from coworks.config import CORSConfig
from .writer import TemplateWriter

UID_SEP = '_'


@dataclass
class Entry:
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


class TerraformWriter(TemplateWriter):

    def __init__(self, app=None, name='terraform', data=None, **kwargs):
        data = data or {
            'layer_zip_file': 'layer.zip',
        }
        super().__init__(app=app, name=name, data=data, **kwargs)

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
            entry = Entry(self.app, previous, last, meth, self.app.config.cors)
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


class TerraformPlanWriter(TerraformWriter):

    def __init__(self, app=None, name='plan', data=None, template_filenames=None, **kwargs):
        template_filenames = template_filenames or ['plan.j2']
        data = data or {'cmd': 'plan'}
        super().__init__(app, name=name, template_filenames=template_filenames, data=data, **kwargs)

    def _export_header(self, **kwargs):
        pass

    def _format(self, content):
        return content.translate(str.maketrans('\n', ' ', '\t\r'))