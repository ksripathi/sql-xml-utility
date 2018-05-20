import json
from xml_parser import *
from db_manager import *
import os
from execute_commands import execute_command

def rsync_files(src_file, dest_file):
    try:
        copy_command = "rsync -arz --progress " + src_file + " " + dest_file
        command = ('%s' % (copy_command))
        (ret_code, output) = execute_command(command)
        if ret_code == 0:
            print ""
        else:
            print "Error in while command %s" % (copy_command)
    except Exception as e:
        print str(e)

def remove_files(file_path):
    try:
        cmd = "rm -rf %s" % (file_path)
        (ret_code, output) = execute_command(cmd)
        if ret_code == 0:
            print ""
        else:
            print "Error in while %s" % (cmd)
    except Exception as e:
        print str(e)

class XML_Mysql_Conveter(object):
    
    def __init__(self):

        self.db_manager = DBManager()
        with open('config.json') as f:
            config = json.load(f)
        self.db_manager.open_connection(**config['qa'])
        self.src_xml_path = config['qa']['src_xml_path']
        self.target_xml_path = config['qa']['target_xml_path']
        self.xml_parser = XMLParser()
        self.src_xml_file = None
        self.target_xml_file = None

    def get_xml_file_names(self, path):
        files = [name for name in os.listdir(path) if name.endswith('.xml')]
        return files

    def add_pkeys(self, keys):
        # self.xml_parser.parse(self.src_xml_file)
        query = self.xml_parser.get_add_pkey_ddl(keys)
        self.db_manager.alter_column(query)

    def remove_pkeys(self):
        # self.xml_parser.parse(self.src_xml_file)
        query = self.xml_parser.get_rm_pkey_ddl()
        self.db_manager.alter_column(query)

    def add_columns(self, cols):
        #self.xml_parser.parse(self.src_xml_file)
        for col in cols:
            query = self.xml_parser.get_add_col_ddl(col)
            self.db_manager.alter_column(query)
            
    def remove_columns(self, cols):
        #self.xml_parser.parse(self.src_xml_file)
        for col in cols:
            query = self.xml_parser.get_rm_col_ddl(col)
            self.db_manager.alter_column(query)

    def update_columns(self, cols):
        #self.xml_parser.parse(self.src_xml_file)
        for col in cols:
            query = self.xml_parser.get_update_col_ddl(col)
            self.db_manager.alter_column(query)
        
    def add_xml_files(self, files):
        for file in files:
            src_file_path = "%s/%s" % (self.src_xml_path, file)
            self.xml_parser.parse(src_file_path)
            query = self.xml_parser.get_create_table_ddl()
            self.db_manager.create_table(query)
            dest_file_path = "%s/%s" % (self.target_xml_path, file)
            rsync_files(src_file_path, dest_file_path)

    def remove_xml_files(self, files):
        for file in files:
            table_name = get_table_name(file)
            self.db_manager.drop_table(table_name)
            dest_path = "%s/%s" % (self.target_xml_path, file)
            remove_files(dest_path)
            
    def cmp_xml_files(self, file):

        self.src_xml_file = "%s/%s" % (self.src_xml_path, file)
        self.xml_parser.parse(self.src_xml_file)
        src_json = self.xml_parser.json_obj
        
        src_col_names = self.xml_parser.get_col_names()
        src_pkeys = []
        if self.xml_parser.is_pkey_exist():
            src_pkeys = self.xml_parser.get_pkey_names()

        self.target_xml_file = "%s/%s" % (self.target_xml_path, file)
        self.xml_parser.parse(self.target_xml_file)
        target_json = self.xml_parser.json_obj
        target_col_names = self.xml_parser.get_col_names()
        target_pkeys = []
        if self.xml_parser.is_pkey_exist():
            target_pkeys = self.xml_parser.get_pkey_names()
        
        new_col_names = list(set(src_col_names) - set(target_col_names))
        del_col_names = list(set(target_col_names) - set(src_col_names))
        common_cols = list(set(src_col_names) & set(target_col_names))

        new_pkeys = list(set(src_pkeys) - set(target_pkeys))
        del_pkeys = list(set(target_pkeys) - set(src_pkeys))
        common_pkeys = list(set(src_pkeys) & set(target_pkeys))
        
        if new_pkeys:
            self.remove_pkeys()
            self.add_pkeys(new_pkeys)
        if del_col_names:
            self.remove_pkeys()
        if common_cols:
            pass
            
        self.add_columns(new_col_names)
        self.remove_columns(del_col_names)
        self.update_columns(common_cols)
        rsync_files(self.src_xml_file, self.target_xml_file)
        
    def diff_xml_files(self):
        src_files = set(self.get_xml_file_names(self.src_xml_path))
        target_files = set(self.get_xml_file_names(self.target_xml_path))

        new_files = list(src_files - target_files)
        self.add_xml_files(new_files)

        remove_files = list(target_files - src_files)
        self.remove_xml_files(remove_files)

        common_files = list(src_files & target_files)
        for file in common_files:
            self.cmp_xml_files(file)
        
if __name__ == '__main__':
    converter = XML_Mysql_Conveter()
    converter.diff_xml_files()
