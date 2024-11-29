import os
from unittest import mock

from flask import json

from coworks import Blueprint
from coworks import TechMicroService
from coworks import entry
from coworks.blueprint.admin_blueprint import Admin


class DocumentedMS(TechMicroService):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.register_blueprint(Admin(), url_prefix="/admin")

    def token_authorizer(self, token):
        return True

    @entry
    def get(self):
        """Root access."""
        return "get"

    @entry
    def post(self):
        """Root access."""
        return "post"

    @entry
    def post_content(self, value, other="none"):
        """Add content."""
        return f"post_content {value}{other}"

    @entry
    def post_contentannotated(self, value: int, other: str = "none"):
        """Add content."""
        return f"post_content {value}{other}"

    @entry(no_auth=True)
    def get_list(self, values: [int]):
        return "ok"


class HiddenBlueprint(Blueprint):

    @entry
    def get(self):
        """Test not in routes."""
        return "ok"


@mock.patch.dict(os.environ, {"TOKEN": "token"})
class TestClass:

    def test_routes(self):
        app = DocumentedMS()
        with app.test_request_context():
            assert '/' in app.routes
            assert '/content/{value}' in app.routes
            assert '/contentannotated/{value}' in app.routes
            assert '/list/{values}' in app.routes

    def test_documentation(self):
        app = DocumentedMS()
        with app.test_client() as c:
            response = c.get('/admin/route?blueprint=test', headers={'Authorization': 'token'})
            assert response.status_code == 404

            response = c.get('/admin/route?blueprint=__all__', headers={'Authorization': 'token'})
            assert response.status_code == 200
            routes = json.loads(response.get_data(as_text=True))
            assert routes["/"]['GET'] == {
                "doc": "Root access.", "signature": "()", "endpoint": "get",
                'binary_headers': None, 'no_auth': False
            }
            assert routes["/"]['POST'] == {
                "doc": "Root access.", "signature": "()", "endpoint": "post",
                'binary_headers': None, 'no_auth': False
            }
            assert routes["/content/<value>"]['POST'] == {
                "doc": "Add content.", "signature": "(value, other=none)", "endpoint": "post_content",
                'binary_headers': None, 'no_auth': False
            }
            assert routes["/contentannotated/<value>"]['POST'] == {
                "doc": "Add content.", "signature": "(value:<class 'int'>, other:<class 'str'>=none)",
                'endpoint': "post_contentannotated",
                'binary_headers': None, 'no_auth': False
            }
            assert routes["/admin/route"]['GET']['signature'] == "(prefix=None, blueprint=None)"
            assert routes["/admin/route"]['GET']['endpoint'] == "admin.get_route"
            assert routes["/list/<values>"]['GET'] == {
                "signature": "(values:[<class 'int'>])", 'endpoint': 'get_list',
                'binary_headers': None, 'no_auth': True
            }

            response = c.get('/admin/route', headers={'Authorization': 'token'})
            assert response.status_code == 200
            routes = json.loads(response.get_data(as_text=True))
            assert "/admin/routes" not in routes

    def test_documentation_with_hidden_blueprints(self):
        app = DocumentedMS()
        app.register_blueprint(HiddenBlueprint(), url_prefix="/hidden", hide_routes=True)
        with app.test_client() as c:
            response = c.get('/admin/route', headers={'Authorization': 'token'})
            assert response.status_code == 200
            routes = json.loads(response.get_data(as_text=True))
            assert '/hidden' not in routes
