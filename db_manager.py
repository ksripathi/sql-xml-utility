#!/usr/bin/python

import MySQLdb as db
import json

class DBManager(object):

    def __init__(self):
        self.db_conn = None

    def open_connection(self, **kwargs):
        try:
            host = kwargs['db_host']
            user = kwargs['db_user']
            pswd = kwargs['db_pswd']
            name = kwargs['db_name']
            self.db_conn = db.connect(host, user, pswd, name)
            print "###########"
            print "Connection opened to '%s' database" % (name)
            print "###########"
            return self.db_conn

        except Exception as e:
            raise e
    def close_connection(self, db_conn):
        print "Connection closed to database"
        db_conn.close()

    def get_tables(self):
        try:
            conn = self.db_conn
            cursor = conn.cursor()
            query = "show tables"
            res = cursor.execute(query)
            tables = cursor.fetchall()
            return tables
        except Exception as e:
            raise e

    def create_table(self, query):

        try:
            conn = self.db_conn
            cursor = conn.cursor()
            res = cursor.execute(query)
            return True
        except Exception as e:
            raise e
        
    def drop_table(self, table):
        try:
            conn = self.db_conn
            cursor = conn.cursor()
            query = "drop table %s" % table
            res = cursor.execute(query)
            return True
        except Exception as e:
            raise e
    def alter_column(self, query):

        try:
            conn = self.db_conn
            cursor = conn.cursor()
            res = cursor.execute(query)
            return True
        except Exception as e:
            raise e
        
        
if __name__ == '__main__':
    db_manager = DBManager()
    
    with open('config.json') as f:
        config = json.load(f)

    conn = db_manager.open_connection(**config['dev'])
    tables = db_manager.get_tables()
    db_manager.close_connection(conn)
