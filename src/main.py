from flask import Flask, escape, request
from routes import metricas
from flask_cors import CORS
import os

app = Flask(__name__)
CORS = CORS(app)

app.register_blueprint(metricas.get_blueprint())

if __name__ == '__main__':
    PORT = int(os.environ.get('PORT', 5500))
    app.run(port = PORT, debug = False, host = '0.0.0.0')