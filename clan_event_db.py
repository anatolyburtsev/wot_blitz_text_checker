import wg_api
import config
import sqlite3
import datetime

db_name = "event_dec_2015"
conn_db = sqlite3.connect(db_name + ".sqlite3")
curs_db = conn_db.cursor()
clans = ["XG","XG-A","XG-T","EQ","CPA","OS_H","PAKU","TOP-A","ACE-S","EXE","3AKOH", "PC","SLM","-NO-","PX_TM","HARDA","BOSS",
         "GWARD","AIR","DALE"]
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
    return curs_db.fetchone()[0]

def get_clan_id_from_db(clan_tag):
    assert type(clan_tag) == str
    cmd = 'select clan_id from  ' + clans_db_name + ' where clan_tag like "' + str(clan_tag) +'" ;'
    curs_db.execute(cmd)
    return curs_db.fetchone()[0]


def save_user_data_to_table(user_id, username, clan_id, clan_tag, dmg, frags, dt, tbl_name):
    cmd = 'insert into ' + tbl_name + ' values (?,?,?,?,?,?,?);'
    curs_db.execute(cmd, (user_id, username, clan_id, clan_tag, dmg, frags, dt))


def collect_data_for_clan_members(clan_id):
    if not str(clan_id).isdigit():
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
        collect_data_for_clan_members(clan_tag)


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
    return [x[0] for x in fetched],[x[1] for x in fetched]


if __name__ == "__main__":
    # drop_db("last")
    # drop_db("history")
    # drop_db("current")
    # drop_db(clans_db_name)
    # drop_db(clans_db_temp_name)
    # init_db()
    # init_clans_db()
    collect_data_for_all_clans(clans)