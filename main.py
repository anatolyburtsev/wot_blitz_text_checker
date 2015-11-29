# -*- coding: utf-8 -*-
from flask import Flask
from flask import request
from flask import render_template
import wg_api
import sql_result_api
import logic
import config

app = Flask(__name__)


@app.route('/hello')
def hello_world():
    return 'Hello World!'


@app.route('/whenmypost')
def when_my_post():
    return render_template("enter_your_nickname.html")


@app.route('/whenmypost', methods=["POST"])
def when_my_post_post():
    try:
        username = str(request.form["nickname"])
    except UnicodeEncodeError:
        result = u'Ник должен быть написан латинскими буквами'
    else:
        result = logic.when_my_post(username, config.vk_group)
    return result


@app.route('/text4nick')
def my_form():
    return render_template("my-form.html")


@app.route('/text4nick', methods=['POST'])
def my_form_post():
    text = request.form['text']
    processed_text = "<br>".join(wg_api.check_text_for_user_from_clans(text))
    if processed_text == "":
        processed_text = "Nothing Found"
    return processed_text


@app.route("/")
def chart():
    # labels = ["January","February","March","April","May","June","July","August"]
    # values = [10,9,8,7,6,4,7,8]
    # return render_template('chart.html', values=values, labels=labels)
    posts_info = sql_result_api.get_data_from_db()
    labels = [str(x[1]) + ":00 " + x[0] for x in posts_info]
    values = [x[2] for x in posts_info]
    return render_template('chart.html', values=values, labels=labels)


if __name__ == '__main__':
    #app.debug = True
    app.run()
    #app.run(host='0.0.0.0', port=5000)