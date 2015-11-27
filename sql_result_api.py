import sqlite3
table_name = 'current_result'


def init_db():
    conn = sqlite3.connect(table_name + '.sql')
    curs = conn.cursor()
    tblcmd = 'create table if not exists ' + table_name + ' (tag char(30), hour int(2), post int(3))'
    curs.execute(tblcmd)


def save_to_db(result_list):
    conn = sqlite3.connect(table_name + '.sql')
    curs = conn.cursor()
    tblcmd = 'delete from ' + table_name
    curs.execute(tblcmd)
    tblcmd = 'insert into ' + table_name + ' values (?,?,?)'
    curs.executemany(tblcmd, result_list)
    conn.commit()


def get_data_from_db():
    conn = sqlite3.connect(table_name + '.sql')
    curs = conn.cursor()
    curs.execute('select * from ' + table_name +' order by hour;')
    return curs.fetchall()


# init_db()
#save_to_db([(u'pewtag', 10, 2), (u'drawtag', 12, 4), (u'pewtag', 22, 16)])
# print get_data_from_db()
# if __name__ == "__main__":
#     print (get_data_from_db())