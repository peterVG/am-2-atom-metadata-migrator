import pymysql.cursors

# Connect to the AtoM MySQL database
mysqlConnection = pymysql.connect(
    host="localhost",
    user="atom-user",
    password="ATOMPASSWORD",
    db="atom",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

with mysqlConnection:
    mysqlCursor = mysqlConnection.cursor()
    sql = "SELECT `object_id` FROM digital_object WHERE `name`= %s"
    # find match for aipfile's filename
    mysqlCursor.execute(sql, "funky_breakbeat_4.wav")
    result = mysqlCursor.fetchone()
    object_id = result["object_id"]
    print(object_id)

    mysqlCursor.close()
    mysqlConnection.close()
