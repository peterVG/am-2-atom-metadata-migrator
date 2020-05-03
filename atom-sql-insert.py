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
allaipfiles = sqliteCursor.fetchall()

for aipfile in allaipfiles:
    # skip file if has already been matched
    if aipfile[11] == "success":
        print(aipfile[3] + " already matched")
        continue

    try:
        with mysqlConnection:

            mysqlCursor = mysqlConnection.cursor()
            sql = "SELECT `object_id` FROM digital_object WHERE `name`= %s"
            # find match for aipfile's filename
            mysqlCursor.execute(sql, (aipfile[3]))
            result = mysqlCursor.fetchone()

            if result:
                print("match found for " + aipfile[3])

                # assign AtoM information object match
                object_id = result["object_id"]

                # stage the AIP file metadata to be inserted into AtoM DO description
                esvalues = {
                    "aipUUID": aipfile[1],
                    "objectUUID": aipfile[0],
                    "formatName": aipfile[4],
                    "formatVersion": aipfile[5],
                    "formatRegistryKey": aipfile[6],
                    "formatRegistryName": aipfile[7],
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

                sql = "SELECT `slug` FROM slug WHERE `object_id`= %s"
                mysqlCursor.execute(sql, object_id)
                result = mysqlCursor.fetchone()
                slug = result["slug"]

                mysqlCursor.close()

                # update am-2-atom link status
                sql = "UPDATE aipfiles SET atomURL = ?, atomSlug = ?, atomLinkStatus = ?, atomLinkDate = ? WHERE uuid = ?"
                sqliteCursor.execute(
                    sql,
                    (
                        atomSiteURL,
                        slug,
                        "success",
                        str(datetime.datetime.now()),
                        aipfile[0],
                    ),
                )

                sqliteDb.commit()

            else:
                mysqlCursor.close()

                print("no match found for " + aipfile[3])

                # update am-2-atom link status
                sql = "UPDATE aipfiles SET atomURL = ?, atomLinkStatus = ?, atomLinkDate = ? WHERE uuid = ?"
                sqliteCursor.execute(
                    sql,
                    (atomSiteURL, "no match", str(datetime.datetime.now()), aipfile[0]),
                )

                sqliteDb.commit()

    except:
        mysqlCursor.close()

        print("linking failure")
        # update am-2-atom link status
        sql = "UPDATE aipfiles SET atomURL = ?, atomLinkStatus = ?, atomLinkDate = ? WHERE uuid = ?"
        sqliteCursor.execute(
            sql, (atomSiteURL, "failure", str(datetime.datetime.now()), aipfile[0])
        )
        sqliteDb.commit()

mysqlConnection.close()
sqliteDb.close()
