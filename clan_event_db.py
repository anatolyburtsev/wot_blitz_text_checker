import wg_api
import config
import sqlite3
import datetime
import logging
import time
import math

db_name = "event_dec_2015"
conn_db = sqlite3.connect(db_name + ".sqlite3")
curs_db = conn_db.cursor()
clans = ["XG","XG-A","XG-T","EQ","CPA","OS_H","PAKU","TOP-A","ACE-S","EXE","3AKOH", "PC","SLM","-NO-","PX_TM","HARDA","BOSS",
         "GWARD","AIR","DALE"]

# clans = [str(x) for x in clans_with_more_than_50_members ]
clans_db_name = "clans_data"
clans_db_temp_name = clans_db_name + "_temp"


def init_db():
    for tbl_name in ["history", "last", "current"]:
        cmd = 'create table if not exists ' + tbl_name + " (uid int(10), name char(20), clan_id int(10), clan_tag char(6), " \
                                           "dmg int(10), frags int(7), date DATETIME);"
        curs_db.execute(cmd)
    cmd = 'create table ' + clans_db_name + ' (clan_id int(10), clan_tag char(6), dmg int(11));'
    curs_db.execute(cmd)
    cmd = 'create table ' + clans_db_temp_name + ' (clan_id int(10), clan_tag char(6), dmg int(11));'
    curs_db.execute(cmd)
    conn_db.commit()


def init_clans_db():
    for clan_tag in clans:
        clan_id = wg_api.get_clan_id_by_tag(clan_tag)
        cmd = 'insert into ' + clans_db_name + " values (?,?,?);"
        curs_db.execute(cmd, (clan_id, clan_tag, 0))
    conn_db.commit()


def drop_db(tbl_name):
    try:
        curs_db.execute('drop table ' + tbl_name)
    except sqlite3.OperationalError:
        pass


def clean_db(tbl_name):
    try:
        curs_db.execute("delete from " + tbl_name)
    except sqlite3.OperationalError:
        pass


def get_clan_tag_from_db(clan_id):
    assert str(clan_id).isdigit()
    cmd = 'select clan_tag from  ' + clans_db_name + ' where clan_id like ' + str(clan_id) +" ;"
    curs_db.execute(cmd)
    try:
        result = curs_db.fetchone()[0]
    except TypeError:
        logging.error("Problem with clan id: " + str(clan_id))
        raise
    return result


def get_clan_id_from_db(clan_tag):
    assert type(clan_tag) == str or type(clan_tag) == unicode
    cmd = 'select clan_id from  ' + clans_db_name + ' where clan_tag like "' + str(clan_tag) +'" ;'
    curs_db.execute(cmd)
    try:
        result = curs_db.fetchone()[0]
    except TypeError:
        logging.error("Problem with clan tag: " + str(clan_tag))
        raise
    return result


def get_username_by_uid_from_db(uid):
    assert str(uid).isdigit()
    cmd = 'select name from last where uid like {} limit 1;'.format(uid)
    curs_db.execute(cmd)
    try:
        result = curs_db.fetchone()[0]
    except TypeError:
        logging.error("problem with uid: {}".format(uid))
        raise
    return result


def save_user_data_to_table(user_id, username, clan_id, clan_tag, dmg, frags, dt, tbl_name):
    cmd = 'insert into ' + tbl_name + ' values (?,?,?,?,?,?,?);'
    curs_db.execute(cmd, (user_id, username, clan_id, clan_tag, dmg, frags, dt))


def collect_data_for_clan_members(clan_id):
    # if not str(clan_id).isdigit():
    clan_id = get_clan_id_from_db(clan_id)
    data = wg_api.get_data_for_all_user_from_clans([clan_id])
    dt = datetime.datetime.now()
    clan_tag = get_clan_tag_from_db(clan_id)

    for user_id, user_data in data.items():
        username = user_data[0]
        dmg = user_data[1]
        frags = user_data[2]
        save_user_data_to_table(user_id, username, clan_id, clan_tag, dmg, frags, dt, "history")
        save_user_data_to_table(user_id, username, clan_id, clan_tag, dmg, frags, dt, "current")
    conn_db.commit()


def shift_data_from_db_to_db(src_tbl, dst_tbl):
    clean_db(dst_tbl)
    cmd = "insert into " + dst_tbl +" select * from " + src_tbl +";"
    curs_db.execute(cmd)
    clean_db(src_tbl)


def collect_data_for_all_clans(clans_list):
    shift_data_from_db_to_db("current", "last")
    clean_db(clans_db_temp_name)

    for clan_tag in clans_list:
        try:
            collect_data_for_clan_members(clan_tag)
        except Exception as e:
            print ("Error:" + str(e))
            print ("problem with clan_tag:" + str(clan_tag))
    # conn_db.commit()

    cmd = "insert into " + clans_db_temp_name + " select cd.clan_id,cd.clan_tag,cd.dmg+dmg_diff from " + clans_db_name\
          +" as cd join (select current.clan_id,sum(current.dmg-last.dmg) as dmg_diff from current inner join last " \
           "using(uid) group by current.clan_id) using(clan_id);"
    curs_db.execute(cmd)
    shift_data_from_db_to_db(clans_db_temp_name, clans_db_name)
    conn_db.commit()


def get_clans_data_from_db():
    cmd = "select clan_tag,dmg from " + clans_db_name + " order by dmg DESC;"
    curs_db.execute(cmd)
    fetched = curs_db.fetchall()
    return [str(x[0]) for x in fetched],[x[1] for x in fetched]


def is_user_played(nickname, day, hour1, hour2):
    av_dmg_per_fight = 3000
    av_fight_in_minutes = 4
    minutes_at_all = 0
    if day < 10:
        day = "0" + str(day)
    else:
        day = day
    if hour1 < 10:
        hour1 = '0' + str(hour1)
    if hour2 < 10:
        hour2 = '0' + str(hour2)
    if hour2 == 24:
        hours2 = 23
        min2 = 59
    else:
        min2 = 00

    cmd = "select dmg,date from history where name like '{}' and date > '2015-12-{} {}:00:00' and" \
          " date < '2015-12-{} {}:{}:00';".format(nickname, day, hour1, day, hour2, min2 )
    curs_db.execute(cmd)
    fetched = curs_db.fetchall()

    # cur_dmg = fetched[0][0]
    # # print fetched[0][1]
    # for dmg, event_date in fetched:
    #     if dmg > cur_dmg:
    #         minutes_at_all += (dmg-cur_dmg)/av_dmg_per_fight * av_fight_in_minutes
    #         # print "minutes: {}, date: {}".format((dmg-cur_dmg)/av_dmg_per_fight * av_fight_in_minutes, event_date)
    #         #print event_date
    #         cur_dmg = dmg
    # return float(minutes_at_all)/60
    print fetched
    if len(fetched) == 0 or fetched[0] == fetched[-1]:
        return False
    else:
        return True



def get_distance_between_clan_and_top(clan_tag="XG"):
    cmd = "select dmg-(select max(dmg) from clans_data where clan_tag not like '" + clan_tag +\
          "') from clans_data where clan_tag like '" + clan_tag + "';"
    curs_db.execute(cmd)
    result = int(curs_db.fetchone()[0])
    result_E100 = abs(result) / 2300
    if abs(result) > 500000:
        result = divmod(result, 100000)[0]
        result = str(float(result)/10) + "M"
    elif abs(result) > 800:
        result = divmod(result,  100)[0]
        result = str(float(result)/10) + "K"
    return [result, result_E100]


if __name__ == "__main__":
    # drop_db("last")
    # drop_db("history")
    # drop_db("current")
    # drop_db(clans_db_name)
    # drop_db(clans_db_temp_name)
    # init_db()
    # init_clans_db()
    # t = time.time()

    # how_many_hours_play_per_day("DlSCOTEQUE", 10)
    # print " "
    # how_many_hours_play_per_day("DlSCOTEQUE", 11)
    # how_many_hours_play_since_day("DlSCOTEQUE", 1)
    # how_many_hours_play_in_clan_since_day("EQ", 1)
    #print "hours at all: {}".format(how_many_hours_play_in_clan_since_day_group_by_user("EQ",4))
    print is_user_played('NoOneIsPerfect', 10, 00, 23)
    # clans_list = get_clans_data_from_db()[0]
    # collect_data_for_all_clans(clans_list)
