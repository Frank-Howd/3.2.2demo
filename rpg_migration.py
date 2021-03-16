import os
import sqlite3
import psycopg2

pg_dbname = os.environ["PG_DBNAME"]
pg_user = os.environ["PG_USER"]
pg_password = os.environ["PG_PASSWORD"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
extraction_db = "rpg_db.sqlite3"

sl_conn = sqlite3.connect(extraction_db)
sl_curs = sl_conn.cursor()

pg_conn = psycopg2.connect(
    dbname=pg_dbname, user=pg_user, password=pg_password, host=pg_host, port=pg_port
)
pg_curs = pg_conn.cursor()

sl_curs.execute(
    """SELECT name from 
    sqlite_master where type='table' 
    and name not like 'django%'
    and name not like 'sqlite%'
    and name not like 'auth%'
                """
)
table_results = sl_curs.fetchall()
table_names = []
for table in table_results:
    table_names.append(table[0])
table_names[4], table_names[2] = table_names[2], table_names[4]
table_names[3], table_names[7] = table_names[7], table_names[3]


for table in table_names:
    sl_curs.execute("""
    SELECT SQL from sqlite_master where type='table' and tbl_name = '{}'
    and name not like 'django%'
    and name not like 'sqlite%'
    and name not like 'auth%'""".format(table))
    create = sl_curs.fetchone()[0]
    create = create.replace("integer NOT NULL PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    create = create.replace("bool", "integer")
    sl_curs.execute("SELECT name from PRAGMA_TABLE_INFO('%s');" % table)
    col_names = sl_curs.execute("SELECT * FROM %s" % table)
    rows = sl_curs.fetchall()

    try:
        pg_curs.execute("DROP TABLE IF EXISTS %s CASCADE;" % table)
        pg_curs.execute(create)
        for row in rows:
            insert_row = """
                INSERT INTO %s VALUES %s
            """ % (table, row)
            pg_curs.execute(insert_row)
        pg_conn.commit()
        print("Created", table)

    except psycopg2.DatabaseError as e:
        print('Error: %s' % e)
        break

pg_conn.close()
sl_conn.close()
