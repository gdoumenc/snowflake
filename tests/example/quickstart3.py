from coworks import TechMicroService
from coworks.config import Config, CORSConfig
from coworks.cws.writer import TerraformStagingWriter


class SimpleMicroService(TechMicroService):

    def get(self):
        return f"Simple microservice ready.\n"

    def auth(self, auth_request):
        return auth_request.token == "token"


DEV_CONFIG = Config(
    workspace="dev",
    version="0.0",
    cors=CORSConfig(allow_origin='*'),
    environment_variables_file="config/vars_dev.json",
    layers=["layer"]
)
PROD_CONFIG = Config(
    workspace="prod",
    version="0.0",
    cors=CORSConfig(allow_origin='www.mywebsite.com'),
    environment_variables_file="config/vars_prod.secret.json",
    layers=["layer"]
)

app = SimpleMicroService(app_name='test', configs=[DEV_CONFIG, PROD_CONFIG])
TerraformStagingWriter(app)


if __name__ == '__main__':
    app.run()
