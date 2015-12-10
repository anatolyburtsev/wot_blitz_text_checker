# -*- coding: utf-8 -*-
from flask import Flask
from flask import render_template
from flask import request

import clan_event_db
import config
import logic
import sql_result_api
import wg_api

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
@app.route("/vk_queue")
def chart():
    # labels = ["January","February","March","April","May","June","July","August"]
    # values = [10,9,8,7,6,4,7,8]
    # return render_template('chart.html', values=values, labels=labels)
    posts_info = sql_result_api.get_data_from_db()
    labels = [str(x[1]) + ":00 " + x[0] for x in posts_info]
    values = [x[2] for x in posts_info]
    return render_template('chart.html', values=values, labels=labels)


@app.route("/kto_tut_samyj_chotkij")
def show_clan_event_data():
    # labels = ["XG","XG-A","XG-T","EQ","CPA","OS_H","PAKU","TOP-A","ACE-S","EXE","3AKOH", "PC","SLM","-NO-","PX_TM","HARDA","BOSS",
    #      "GWARD","AIR","DALE"]
    # values = [0, 73483, 6399, 43888, 0, 184961, 66410, 180025, 6361, 115999, 0, 39648, 0, 0, 32142, 0, 4195, 0, 26642, 0]
    labels,values = clan_event_db.get_clans_data_from_db()
    labels  = [ x if x == "XG" else u"Пидарасы" for x in labels]
    max_value = 1.2 * max(values)
    distance, distance_E100 = clan_event_db.get_distance_between_clan_and_top()
    if distance[0] != "-":
        diff = u"Мы опережаем ближайшего конкурента на: " + str(distance)
    else:
        diff = u"Мы отстаем от лидера на целых: " + str(distance) + u"!!! Давайте поднажмем!!!"
    return render_template('clan_event.html', values = values, labels = labels, max_value = max_value,
                           diff_message=diff, E100_count = distance_E100)


if __name__ == '__main__':
    #app.debug = True
    #app.run(host='0.0.0.0', port=80)
    app.run(host='0.0.0.0', port=5000)