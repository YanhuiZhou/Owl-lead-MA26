from sodapy import Socrata
import pandas as pd
import mysql.connector as mysql
import json
import time

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

def populate_pd_cnts(cursor , access_token = "RrWl8vIygHuQlviEbWq1TD5zC"):

    client = Socrata("data.melbourne.vic.gov.au",access_token)
    results = client.get("d6mv-s43h")
    results_df = pd.DataFrame.from_records(results)
    sensor_data = results_df[["sensor_id"  , "total_of_directions" , "date_time"]]
    sensors_data_list = [tuple(x) for x in sensor_data.to_numpy()]
    print(sensors_data_list[0:5])
    if sensors_data_list:
        cursor.execute("TRUNCATE pd_cnts")
        Q5 = "INSERT INTO pd_cnts(sensor_id , pedestrian_count , date_time) VALUES (%s, %s , %s)"
        cursor.executemany(Q5 , sensors_data_list)

        print("successfully added new data to pd_cnts table")
        return 1

    print("no new data recieved")

if __name__ == "__main__":
    db = connect_db()
    cursor = db.cursor()
    populate_pd_cnts(cursor)
    db.commit()
