from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/patients")
def patient_list():
    return "<p>200 patients found!</p>"

@app.route("/upload/clients")

# Ask for csv file
# convert
def patient_list():
    return "<p>200 patients found!</p>"