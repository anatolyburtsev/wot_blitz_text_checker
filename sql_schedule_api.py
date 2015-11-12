# -*- coding: utf-8 -*-
import sqlite3
table_name = 'schedule'


def init_db():
    tblcmd = 'create table if not exists ' + table_name + ' (hour int(2) unique, name char(30), hashtag char(30), ' \
                                                          'response char(20), description char(50))'
    curs.execute(tblcmd)


def add_new_rubric(hour, rubric_name, rubrik_hashtag, response, description=""):
    curs.execute('insert into '+table_name + ' values (?,?,?,?,?)', (hour, rubric_name, rubrik_hashtag,
                                                                     response, description))
    #print(curs.rowcount)


def show_db():
    curs.execute('select * from ' + table_name)
    print(curs.fetchall())


def clean_db():
    curs.execute("delete from " + table_name)


def drop_db():
    curs.execute('drop table ' + table_name)


def select_times_by_hashtag(hashtag):
    curs.execute('select hour from ' + table_name + ' where hashtag like \'' + hashtag + '\'')
    times = [x[0] for x in curs.fetchall()]
    return times


def get_all_tags_in_order():
    curs.execute('select hashtag from ' + table_name + ' order by hour')
    tags = [x[0] for x in curs.fetchall()]
    return tags


def get_all_tags_and_time_in_order():
    curs.execute('select hashtag,hour from ' + table_name + ' order by hour')
    tags = curs.fetchall()
    # tags = [x[0] for x in curs.fetchall()]
    return tags


def get_all_unique_tags():
    curs.execute('select distinct hashtag from ' + table_name + ' order by hour')
    tags = [x[0] for x in curs.fetchall()]
    return tags


conn = sqlite3.connect(table_name + '.sql')
curs = conn.cursor()

#clean_db()

# drop_db()
# init_db()
# add_new_rubric(9, "fight results", "epicwin", "Eldar", "fight result")
# add_new_rubric(10, "Music", "Music", "Anna", "blahbldah")
# add_new_rubric(11, "Picture", "humor", "Nataliya", "voen lalala")
# add_new_rubric(12, "Ugadajka", "ugadaika", "Artem", "voen lalala")
# add_new_rubric(14, "Gif", "gif", "Lev", "voen lalala")
# add_new_rubric(16, "Picture", "humor", "Nataliya", "voen lalala")
# add_new_rubric(18, "Humor from chat", "lolchat", "Stas", "voen lalala")
# add_new_rubric(20, "Screenshoter", "screenshoter", "Tolya", "voen lalala")
# add_new_rubric(21, "Picture", "humor", "Andrew", "voen lalala")
# add_new_rubric(22, "fight results", "epicwin", "Eldar", "fight result")
# conn.commit()


# show_db()
#
# print select_times_by_hashtag("drawtag")
# print get_all_tags_in_order()
# print get_all_unique_tags()
# print get_all_tags_and_time_in_order()