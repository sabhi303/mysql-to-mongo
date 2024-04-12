import mysql.connector 
import pymongo
import decimal
import datetime

def migrate_all(mysqldb_dict, mongodb_host, mongodb_dbname):
    mysqldb = mysql.connector.connect(
        host=mysqldb_dict["mysql_host"],
        database=mysqldb_dict["mysql_database"],
        user=mysqldb_dict["mysql_user"],
        password=mysqldb_dict["mysql_password"]
    )
    table_list_cursor = mysqldb.cursor()
    table_list_cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name;",
        (mysqldb_dict["mysql_database"],)
    )
    tables = table_list_cursor.fetchall()
    myclient = pymongo.MongoClient(mongodb_host)
    mydb = myclient[mongodb_dbname]

    def migrate(db, col):
        mycursor = db.cursor(dictionary=True) 
        mycursor.execute("SELECT * FROM `" + col + "`;")  # Escape 'references' column name
        myresult = mycursor.fetchall()
        print("Table name : "+ col)
        mycol = mydb[col]
        if len(myresult) > 0:
            # Convert decimal.Decimal to Python native types
            for row in myresult:
                for key, value in row.items():
                    if isinstance(value, decimal.Decimal):
                        row[key] = float(value)  # or str(value)
                    elif isinstance(value, datetime.date):
                        row[key] = datetime.datetime.combine(value, datetime.datetime.min.time())
            x = mycol.insert_many(myresult)
            return len(x.inserted_ids)
        else:
            return 0

    for table in tables:
        x = migrate(mysqldb, table[0])
        print("Total inserted rows : " + str(x))


def migrate_single(mysqldb_dict, mongodb_host, mongodb_dbname, table):
    mysqldb = mysql.connector.connect(
        host=mysqldb_dict["mysql_host"],
        database=mysqldb_dict["mysql_database"],
        user=mysqldb_dict["mysql_user"],
        password=mysqldb_dict["mysql_password"]
    )
    mycursor = mysqldb.cursor(dictionary=True) 
    mycursor.execute("SELECT * FROM `" + table + "`;")  # Escape 'references' column name
    myresult = mycursor.fetchall()

    myclient = pymongo.MongoClient(mongodb_host)
    mydb = myclient[mongodb_dbname]
    mycol = mydb[table]
    if len(myresult) > 0:
        # Convert decimal.Decimal to Python native types
        for row in myresult:
            for key, value in row.items():
                if isinstance(value, decimal.Decimal):
                    row[key] = float(value)  # or str(value)
                elif isinstance(value, datetime.date):
                    row[key] = datetime.datetime.combine(value, datetime.datetime.min.time())
        x = mycol.insert_many(myresult)
        print(len(x.inserted_ids))
    else:
        print(0)
