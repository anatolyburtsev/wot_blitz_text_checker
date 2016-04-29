# -*- coding: utf-8 -*-

import config
import vk_api
import sqlite3
import system_api


table_name = 'all_posts'
conn = sqlite3.connect(table_name + '.sql')
curs = conn.cursor()


def init_db():
    tblcmd = 'create table if not exists ' + table_name + ' ' \
                                                          '(post_id int(10), ' \
                                                          ' likes int(7), ' \
                                                          'reposts int(7), ' \
                                                          'comments int(7), ' \
                                                          'text char(300), ' \
                                                          'date_unixtime int(11), ' \
                                                          'date datetime, ' \
                                                          'weekday int(1), ' \
                                                          'month int(2), ' \
                                                          'photos int(2), ' \
                                                          'videos int(2), ' \
                                                          'audio int(2), ' \
                                                          'poll int(1),' \
                                                          'link int(1)' \
                                                          ')'
    curs.execute(tblcmd)


def show_db():
    curs.execute('select * from ' + table_name)
    print(curs.fetchall())


def clean_db():
    curs.execute("delete from " + table_name)


def drop_db():
    curs.execute('drop table if exists ' + table_name)


def save_posts(posts):
    # posts = vk_api.get_all_posts(config.vk_group)
    counter = 0
    for post in posts:
        if counter % 100 == 0:
            print "post number:" + str(counter)
        counter += 1
        # print post
        post_id = "http://vk.com/wall-" + str(vk_api.get_group_id_by_url(config.vk_group)) + "_" + str(post["id"])
        likes = post["likes"]["count"]
        reposts = post["reposts"]["count"]
        comments = post["comments"]["count"]
        text = post["text"]
        # author = vk_api.get_ame_by_id(post["from_id"])
        date_unixtime = post["date"]
        date_datetime = system_api.unixtime_to_datetime(date_unixtime)
        weekday = system_api.unixtime_to_weekofday(date_unixtime)
        month = system_api.unixtime_to_month(date_unixtime)
        photos = 0
        videos = 0
        audio = 0
        poll = 0
        link = 0

        if "attachments" in post.keys():
            for attach in post["attachments"]:
                if attach["type"] == "photo" or attach["type"] == "posted_photo":
                    photos += 1
                if attach["type"] == "video":
                    videos += 1
                if attach["type"] == "audio":
                    audio += 1
                if attach["type"] == "poll":
                    poll += 1
                if attach["type"] == "link":
                    link += 1

        # print (post_id, likes, reposts, comments, text, author, date_unixtime,
        #                       date_datetime, weekday, photos, videos, audio, poll, link)
        tblcmd = 'insert into ' + table_name + ' values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        curs.execute(tblcmd, (post_id, likes, reposts, comments, text, date_unixtime,
                              date_datetime, weekday, month, photos, videos, audio, poll, link))
    conn.commit()

    # сдвиг -3 часа
    # select * from all_posts where date like "2016-03-30%";
    # select post_id from all_posts where date like "2016-03% 07:00:%" order by likes desc limit 3;
    # укажи месяц!!!!
    # for i in {14,16,18,20,21,22}; do echo "Часов: ${i}:00"; echo "3 самых популярных поста:"; echo $( sqlite3 all_posts.sql "select post_id from all_posts where date like \"2016-03% $((i - 3)):00:%\" order by likes desc limit 3;"); done


if __name__ == "__main__":
    # clean_db()
    drop_db()
    init_db()
    posts = vk_api.get_all_posts(config.vk_group)
    print "Got all posts."
    save_posts(posts)
