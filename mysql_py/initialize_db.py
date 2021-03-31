#function to make a connection to database
#configure DB_config.txt to change mysql connection details
def connect_db():
    try :
        data = open("DB_config.txt","r").read()
        data = json.loads(data)
    except :
        print("could not read DB_config,txt file")

    try:
        db = mysql.connect(
            host = data["host"],
            user = data["user"],
            password = data["password"],
            database = data["database"]
        )

        return db
    except:
        print("could not access the database")

def create_tables(cursor):
    cursor.execute("SHOW TABLES")
    current_tables = list(cursor)
    current_tables = list(map(lambda x:x[0] , current_tables))
    if "pd_cnts_snsrs" not in current_tables:
        Q1 = '''CREATE TABLE pd_cnts_snsrs(sensor_id INT PRIMARY KEY,
            place VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT)
            '''

        cursor.execute(Q1)
        print("pd_cnts_snsrs table created")
    else:
        print("pd_cnts_snsrs table already exists")

    if "pd_cnts" not in current_tables:
        Q2 = '''CREATE TABLE pd_cnts(
        sensor_id INT ,
        pedestrian_count INT ,
        date_time DATETIME ,
        PRIMARY KEY(sensor_id , date_time))'''

        Q3 = "ALTER TABLE pd_cnts ADD FOREIGN KEY(sensor_id) REFERENCES pd_cnts_snsrs(sensor_id)"
        cursor.execute(Q2)
        cursor.execute(Q3)
        print("pd_cnts table created")
    else :
        print("pd_cnts already exists")

if __name__ == "__main__":
    db = connect_db()
    cursor = db.cursor()
    create_tables(cursor)
    db.commit()
