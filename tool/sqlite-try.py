#
# json to sqlite3
import sqlite3
import json
import os
import time
os.chdir('..')
print(os.getcwd())




for map_name in ['v1_daytime', 'v4']:
    try:
        sqliteConnection = sqlite3.connect(
        "../data-dev/data/{}/crawl_records.db".format(map_name))
        cursor = sqliteConnection.cursor()
        cursor.execute('''
            CREATE TABLE crawl_records(
                file_name varchar(100) NOT NULL,
                crawled_at varchar(8) NOT NULL,
                map_rotation varchar(2), 
                ETag varchar(30) NOT NULL, 
                zoom_level INTEGER NOT NULL,
                coord_x INTEGER NOT NULL,
                coord_y INTEGET NOT NULL,
                frozen boolean DEFAULT False,
                deleted boolean DEFAULT False
	        )'''
        )
        cursor.execute('''
            CREATE TABLE map_attributes(
                id INTEGER PRIMARY KEY NOT NULL,
                map_name varchar(30),
                map_savename varchar(30),
                storage_type varchar(6),
                data_path varchar(128),
                last_total_depth INTEGER,
                last_update varchar(8)
            )'''
        )

        cursor.execute('''
            INSERT INTO map_attributes
                (id, map_savename, storage_type, data_path)
            VALUES (?,?,?,?)''',
            (1, map_name, 'local', 'data-production')
        )
        

        with open('{}/{}/update_history.json'.format('../data-production/data', map_name), 'r') as f:
            update_history = json.load(f)
            for file_name in update_history:
                zoom_level, coord_x, coord_y = file_name.split('.')[0].split('_')
                for record in update_history[file_name]:
                    ETag = record['ETag']
                    crawled_at = record['Save_in'].split('/')[4]
                    stored_at = '/'.join(record['Save_in'].split('/')[2:])
                    #print(record)
                    #print(file_name, crawled_at, ETag, zoom_level, coord_x, coord_y, stored_at)
                    #print('-------------------')
                    cursor.execute('''INSERT INTO crawl_records
                    (file_name, crawled_at, ETag, zoom_level, coord_x, coord_y)
                    VALUES (?,?,?,?,?,?)''', 
                    (file_name, crawled_at, ETag, zoom_level, coord_x, coord_y) )
                #time.sleep(0.2)
            sqliteConnection.commit()
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("sqlite3 closed")


'''
try:
    print("{}/data/{}/crawl_records.db".format(CrConf.data_folders, map_v1.map_savename))
    sqliteConnection = sqlite3.connect(
        "{}/data/{}/crawl_records.db".format(CrConf.data_folders, map_v1.map_savename))
    cursor = sqliteConnection.cursor()
    
    print("Database created and Successfully Connected to SQLite")
    sqlite_select_Query = "select sqlite_version();"
    cursor.execute(sqlite_select_Query)
    record = cursor.fetchall()
    print("SQLite Database Version is: ", record)
    cursor.close()
finally:
    if (sqliteConnection):
        sqliteConnection.close()
        print("sqlite3 closed")
'''