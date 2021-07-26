import sqlite3
import os
import logging

# TODO: 是否要单例模式？


class UpdateHistoryDBConn():
    def __init__(self, logger:logging.Logger): # kwargs?
        if logger: self.logger = logger
        else: 
            self.logger = logging.getLogger()
            self.logger.warning('logger not specified, using backup default logger.')
    #----------------DB Operations-----------------
    def prepare(self, data_folderR, map_name, map_savename, storage_typeR, data_folderW): #prepareDBConnection(self):
        # Connects to DB and return its connection,
        # Create DB file path if not exist
        # Create DB Tables and headers on DB file creation.
        # TODO: testcase: data_folderR != data_folderW
        # data_folder_R = {project_root}/{config.data_folders}/data/{map_savename}
        # data_folder_W = {config.data_folders}
        try:
            self.sqliteConnection = sqlite3.connect(
                '{}/crawl_records.sqlite3'.format(data_folderR)) # Its '.' path is set to the path of calling module(crawl.py...).
        except sqlite3.OperationalError:
            self.logger.warning('DB not found, creating its directory: ')
            self.logger.warning('{}/crawl_records.sqlite3'.format(data_folderR))
            if not os.path.exists(data_folderR):
                os.makedirs(data_folderR)
                self.logger.info(
                    'Made directory\t./{}'.format(data_folderR)
                    )
            self.sqliteConnection = sqlite3.connect(
                '{}/crawl_records.sqlite3'.format(data_folderR))
        self.logger.info(
            'Connected to [{}]/crawl_records.sqlite3'.format(data_folderR))

        # init tables with headers.
        cursor = self.sqliteConnection.cursor()
        #    table: crawl_records: init once, appends every crawl.file; updates manually.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_records(
                file_name varchar(40) NOT NULL,
                crawled_at varchar(8) NOT NULL,
                map_rotation varchar(2), 
                ETag varchar(30) NOT NULL, 
                zoom_level INTEGER NOT NULL,
                coord_x INTEGER NOT NULL,
                coord_y INTEGER NOT NULL,
                frozen boolean DEFAULT 0,
                deleted boolean DEFAULT 0
	        )'''
        )
        #    table: map_attributes: init once, never updates (unless manually)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS map_attributes(
                id INTEGER PRIMARY KEY NOT NULL,
                map_name varchar(30),
                map_savename varchar(30),
                storage_type varchar(6),
                data_path varchar(128)
            )''' # This table only 1 row, id can only == 1
        )
        # Init map_attributes, pass if record already exists
        cursor.execute('''
            INSERT OR IGNORE INTO map_attributes
                (id, map_name, map_savename, storage_type, data_path)
            VALUES(?,?,?,?,?)''',
            (1, map_name, map_savename, storage_typeR, data_folderW)
        )

        #    table: last_update: init once, updates every crawl.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS last_update(
                id INTEGER PRIMARY KEY NOT NULL,
                date varchar(8),
                total_depth INTEGER,
                renderer varchar(128)
            )'''
        )

        #return self.sqliteConnection

    def updateDBDates(self, today, total_depth, map_type): 
        # update 'last_update' in DB.last_update .
        cursor = self.sqliteConnection.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO last_update
                (id, date, total_depth, renderer)
            VALUES (?,?,?,?)''',
            (1, 
            today, # '20201203'
            total_depth, # 12
            map_type # 'Overviewer' | 'Mapcrafter'(not implemented)
            )
        )

    def getLatestSavedETag(self, file_name) -> str: 
        # Return the latest saved ETag 
        #    of the given file_name from DB;
        # If the filename has no record then return 'G' 
        #    in order not to collode into hex-like ETags.
        retrieved = self.getLatest('ETag', file_name)
        return 'G' if retrieved is None else retrieved[0]

    def getLatestUpdateDate(self, file_name) -> str:
        # Return the latest update date 
        #    of the given file_name from DB;
        # example: '20201112'
        retrieved = self.getLatest('ETag', file_name) # ETag is not used in this function
        # ... but essential to make a correct function call.
        return '' if retrieved is None else retrieved[1]

    def getLatest(self, item, file_name):
        # Return the latest item from DB
        # If item has no record then return ''.
        cursor = self.sqliteConnection.cursor()
        cursor.execute('''
            SELECT {}, crawled_at
            FROM crawl_records
            WHERE file_name = ? AND not deleted
            ORDER BY crawled_at DESC
            LIMIT 1
        '''.format(item), (file_name,) 
        )
        retrieved = cursor.fetchone() 
        return retrieved

    def getMapStorageType(self):
        # return 'local' or 's3'
        retrieved = self.getMapAttr('map_attributes', 'storage_type')
        # if no record then return config else return record
        return None if retrieved == None else retrieved[0] # TODO: 在数据库无信息返回配置文件的storage_type时给出warning

    def getMapDatapath(self):
        retrieved = self.getMapAttr('map_attributes', 'data_path')
        return None if retrieved == None else retrieved[0] # TODO: 同上

    def getMapLastProbedDepth(self):
        # not accessed as noFetch is set to False
        retrieved = self.getMapAttr('last_update', 'total_depth') #TODO
        if retrieved == None: raise ValueError('No recorded crawled depth information')
        return retrieved[0]

    #def getMapLastCrawledDate(self):
    #    retrieved = self.getMapAttr('last_update', 'date') 


    def getMapLastProbedRenderer(self):
        # not accessed as noFetch is set to False
        retrieved = self.getMapAttr('last_update', 'renderer') #TODO
        if retrieved == None: raise ValueError('No renderer recorded')
        return retrieved[0]

    def getMapAttr(self, table, attr):
        cursor = self.sqliteConnection.cursor()
        cursor.execute('''
            SELECT {}
            FROM {}
            WHERE id = 1
        '''.format(attr, table)) 
        return cursor.fetchone()   

    def deactivateCrawlRecord(self, file_name, date):
        # Soft delete a record in crawl record DB
        # accroadign to given filename and date.
        # show warning message if row(s) other than 1 is affected.
        cursor = self.sqliteConnection.cursor()
        cursor.execute('''
            UPDATE crawl_records
            SET deleted = 1
            WHERE file_name = ?
                AND crawled_at = ?
                AND not deleted
        ''',(file_name, date) 
        )
        if cursor.rowcount != 1:
            self.logger.warning("Deactivated {} lines on {}, {}".format(cursor.rowcount, file_name, date))
        else:
            self.logger.info("Deactivated {} lines on {}, {}".format(cursor.rowcount, file_name, date))
        return 

    def addCrawlRecord(self, file_name, date, ETag, zoom_level, coord_x, coord_y):
        cursor = self.sqliteConnection.cursor()
        cursor.execute('''
            INSERT INTO crawl_records
            (file_name, crawled_at, ETag, zoom_level, coord_x, coord_y)
            VALUES (?,?,?,?,?,?)''',
            (file_name, date, ETag, zoom_level, coord_x, coord_y) 
        )

    def updateETag(self, file_name, date, ETag):
        # BUG/Feature: updating ETag of a history image will affest its crawl status...
        #   if we crawl once, delete the new record and crawl again same day.
        cursor = self.sqliteConnection.cursor()
        cursor.execute('''
            UPDATE crawl_records
            SET ETag = ?
            WHERE file_name = ?
                AND crawled_at = ?
                AND not deleted
        ''',(ETag, file_name, date) 
        )
        if cursor.rowcount != 1:
            self.logger.warning("Updated ETag for {} lines on {}, {}".format(cursor.rowcount, file_name, date))

    def commit(self):
        self.sqliteConnection.commit()
    
    def close(self):
        self.sqliteConnection.close()