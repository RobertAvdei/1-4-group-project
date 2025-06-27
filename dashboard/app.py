import mimetypes

mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('application/json', '.geo.json')
mimetypes.add_type('text/css', '.css')
from flask import Flask, request
from flask import render_template
from queries import fetch_data
app = Flask(__name__)


@app.route("/")
def home():
    return render_template('index.html')

@app.route("/api/data")
def return_data():
    return fetch_data()

if __name__ == "__main__":
    app.run()
