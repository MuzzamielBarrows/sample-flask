from flask import Flask
from flask import render_template

app = Flask(__name__)

def logging():
    print("This logging is working")
    return 5

@app.route("/")
def main():
    a = logging()
    print(a)
    return render_template("index.html")
