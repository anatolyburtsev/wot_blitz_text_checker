import wg_api
import sqlite3
import shutil
import datetime

clans_table_name = "clans"

conn_clans = sqlite3.connect(clans_table_name + '.sql')
curs_clans = conn_clans.cursor()


def init_db():
    tblcmd = 'create table if not exists ' + clans_table_name + ' (id int(7) unique, tag char(6), size int(3))'
    curs_clans.execute(tblcmd)


def show_db():
    curs_clans.execute('select * from ' + clans_table_name)
    print(curs_clans.fetchall())


def clean_db():
    curs_clans.execute("delete from " + clans_table_name)


def drop_db():
    curs_clans.execute('drop table ' + clans_table_name)


def save_clan_data(clan_data_dict):
    # {id: [tag, number of members], ...}
    conn_clans.commit()
    copy_db_file()
    clean_db()
    for clan_id, clan_data in clan_data_dict.items():
        curs_clans.execute('insert into ' + clans_table_name + ' values (?,?,?)', (clan_id, clan_data[0], clan_data[1]))
    conn_clans.commit()


def copy_db_file():
    my_date = datetime.date.today()
    dst_filename = str(my_date.year) + str(my_date.month) + str(my_date.day)
    shutil.copyfile(clans_table_name + ".sql", dst_filename + ".sql")


def get_all_clans_ids():
    curs_clans.execute('select id from ' + clans_table_name)
    return [x[0] for x in curs_clans.fetchall()]




# # clean_db()
# show_db()
if __name__ == "__main__":
    save_clan_data(wg_api.get_all_clans_list())