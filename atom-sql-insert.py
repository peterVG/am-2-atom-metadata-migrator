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

            # stage the AIP file metadata to be inserted into AtoM DO description
            esvalues = {
                "aipUUID": aip[1],
                "objectUUID": aip[0],
                "formatName": aip[4],
                "formatVersion": aip[5],
                "formatRegistryKey": aip[6],
                "formatRegistryName": aip[7],
            }

            for key, value in esvalues.items():
                # AtoM stores Archivematica metadata as Properties attached
                # to the Digital Object's parent Information Object.
                # We need to loop through and create a Property for each
                # Archivematica metadata value.
                sql = "INSERT INTO `property` (`object_id`, `name`, `source_culture`) VALUES (%s, %s, %s)"
                mysqlCursor.execute(sql, (object_id, key, "en"))
                property_id = mysqlCursor.lastrowid
                sql = "INSERT INTO `property_i18n` (`value`, `id`, `culture`) VALUES (%s, %s, %s)"
                mysqlCursor.execute(sql, (value, property_id, "en"))

            # only commit if all properties are succesfully updated
            mysqlConnection.commit()
            mysqlCursor.close()

            sql = "SELECT `slug` FROM slug WHERE `object_id`= %s"
            mysqlCursor.execute(sql, (object_id))
            slug = mysqlCursor.fetchone()

            # update am-2-atom link status
            sql = "UPDATE aipfiles SET atomURL = ?, atomSlug = ?, atomLinkStatus = ?, atomLinkDate  WHERE uuid = ?"
            sqliteCursor.execute(
                sql,
                (atomSiteURL, slug, "success", str(datetime.datetime.now()), aip[0]),
            )
            sqliteDb.commit()

    except:
        # update am-2-atom link status
        sql = "UPDATE aipfiles SET atomLinkStatus = ?, atomLinkDate  WHERE uuid = ?"
        sqliteCursor.execute(sql, ("fail", str(datetime.datetime.now())), aip[0])
        sqliteDb.commit()

mysqlConnection.close()
sqliteDb.close()
