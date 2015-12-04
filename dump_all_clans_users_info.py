import wg_api
import sqlite3
from dump_all_clans_info import get_all_clans_ids
import datetime

users_table_name = "users"

conn_users = sqlite3.connect(users_table_name + '.sql')
curs_users = conn_users.cursor()


def init_db():
    tblcmd = 'create table if not exists ' + users_table_name + ' (id int(10), name char(30), date DATETIME,' \
                                                                'clan_id int(7), damage int(10), frags int(6))'
    curs_users.execute(tblcmd)
    tblcmd = 'create table if not exists '


def show_db():
    curs_users.execute('select * from ' + users_table_name + ' limit 10;')
    print(curs_users.fetchall())


def clean_db():
    try:
        curs_users.execute("delete from " + users_table_name)
    except sqlite3.OperationalError:
        pass


def drop_db():
    try:
        curs_users.execute('drop table ' + users_table_name)
    except sqlite3.OperationalError:
        pass


def save_user_data(user_id, clan_id):
    # uneffective!
    assert str(user_id).isdigit()
    user_id = str(user_id)
    user_data = wg_api.get_users_data_by_id(user_id)
    nickname = user_data["nickname"]
    dmg = user_data["damage_dealt"]
    frags = user_data["frags"]

    curs_users.execute('insert into ' + users_table_name + ' values (?,?,?,?,?,?)', (user_id, nickname,
                                                                                    datetime.datetime.now(),
                                                                                    clan_id,
                                                                                    dmg,
                                                                                    frags))


def save_all_users_data_from_clan(clan_id, dt):
    assert str(clan_id).isdigit()
    clan_id = str(clan_id)
    users_dict = wg_api.get_data_for_all_user_from_clan(clan_id)
    # {user_id, [nickname, dmg, frags]}
    list_to_save =[]

    for user_id, user_data in users_dict.items():
        nickname = user_data[0]
        dmg = user_data[1]
        frags = user_data[2]
        list_to_save.append((user_id, nickname, dt, clan_id, dmg, frags))
    curs_users.executemany('insert into ' + users_table_name + ' values (?,?,?,?,?,?)', list_to_save)
    # for user_id in wg_api.get_clans_members_list_by_id(clan_id):
    #     save_user_data(user_id, clan_id)


def save_all_users_data():
    dt = datetime.datetime.now()
    for clan_id in get_all_clans_ids():
        try:
            save_all_users_data_from_clan(clan_id, dt)
        except Exception as e:
            print e
        print "clan " + str(clan_id) + " done"
        if clan_id % 50 == 0:
            conn_users.commit()
    conn_users.commit()


def save_user_clan_relationship():
    init_db_cmd = "create table if not exists clans (clan_id int(8) unique, user_id int(10));"
    curs_users.execute(init_db_cmd)

    for clan_id in get_all_clans_ids():
        members = wg_api.get_clans_members_list_by_id(clan_id)
        for user_id in members:
            curs_users.execute("insert into clans values (?,?)", (clan_id, user_id))
    conn_users.commit()


# # clean_db()
# show_db()
if __name__ == "__main__":
    # drop_db()
    # init_db()
    # show_db()
    # save_all_users_data_from_clan(10)
    # show_db()
    # print get_all_clans_ids()
    # clean_db()
    #save_all_users_data()
    save_user_clan_relationship()
# show_db()
