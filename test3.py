import pymysql.cursors
import sqlite3
import datetime

sqliteDbName = "am_es_cdd02460-c432-4bdc-bca3-34ae3fb006a6.db"
atomSiteURL = "http://10.10.10.10/"

# Connect to the am-2-atom-metadata-migrator Sqlite database
sqliteDb = sqlite3.connect(sqliteDbName)
sqliteCursor = sqliteDb.cursor()

# Connect to the AtoM MySQL database
mysqlConnection = pymysql.connect(
    host="localhost",
    user="atom-user",
    password="ATOMPASSWORD",
    db="atom",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

sqliteCursor.execute("""SELECT * FROM aipfiles""")
allaips = sqliteCursor.fetchall()

for aip in allaips:
    try:

        with mysqlConnection:
            mysqlCursor = mysqlConnection.cursor()

            sql = "SELECT `object_id` FROM digital_object WHERE `name`= %s"
            # find match for aipfile's filename
            mysqlCursor.execute(sql, (aip[3]))
            result = mysqlCursor.fetchall()
            # assign AtoM information object match
            object_id = result[0]["object_id"]

            if object_id is not None:

                print("match found for " + aip[3])

            else:
                print("no match found for " + aip[3])

    except:
        print("linking failure")


mysqlConnection.close()
sqliteDb.close()
