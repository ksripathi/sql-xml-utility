#!/usr/bin/python

from db_manager import DBManager
from execute_commands import execute_command
import json
import os

class DOF_Converter(object):

    def __init__(self):
        self.db_manager = DBManager()
        with open('config.json') as f:
            config = json.load(f)

        conn = self.db_manager.open_connection(**config['dev'])
        self.host = config['dev']['db_host']
        self.user = config['dev']['db_user']
        self.pswd = config['dev']['db_pswd']
        self.name = config['dev']['db_name']
        self.path = config['dev']['file_save_path']

    def convert_table_to_xml(self, tbl_name):

        try:
            if not os.makedirs(self.path):
                os.path.isdir(self.path)
        except Exception as e:
            pass
        
        cmd = "mysqldump --xml --no-data -h %s -u %s -p%s %s %s > %s/%s.xml" % \
              (self.host, self.user, self.pswd, self.name, tbl_name, self.path, tbl_name)
        try:
            (ret_code, output) = execute_command(cmd)
            if ret_code !=0 :
                print "Error in executing command %s" % (cmd)
        except Exception as e:
            raise e

if __name__ == '__main__':
    
    convert = DOF_Converter()
    tables = convert.db_manager.get_tables()
    for table in tables:
        convert.convert_table_to_xml(table[0])
