from coworks import TechMicroService

app = TechMicroService(__name__)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>", 200, {'Content-Type': 'text/html; charset=utf-8'}
