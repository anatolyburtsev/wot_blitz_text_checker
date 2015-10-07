from flask import Flask
from flask import request
from flask import render_template
import wg_api

app = Flask(__name__)


@app.route('/hello')
def hello_world():
    return 'Hello World!'


@app.route('/')
def my_form():
    return render_template("my-form.html")


@app.route('/', methods=['POST'])
def my_form_post():

    text = request.form['text']

    #processed_text = text.upper()
    processed_text = "<br>".join(wg_api.check_text_for_user_from_clans(text))
    return processed_text


if __name__ == '__main__':
    #app.debug = True
    app.run(host='0.0.0.0')