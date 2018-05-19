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

class DiffXML(object):
    
    def __init__(self, src_xml_path, target_xml_path):

        self.src_xml_path = src_xml_path
        self.target_xml_path = target_xml_path
        self.db_manager = DBManager()
        with open('config.json') as f:
            config = json.load(f)
        self.db_manager.open_connection(**config['qa'])
        self.src_xml_file = None
        self.target_xml_file = None

    def get_xml_files(self, path):
        # files = [os.path.join(path, name) for name in os.listdir(path) if name.endswith('.xml')]
        files = [name for name in os.listdir(path) if name.endswith('.xml')]
        return files

    def add_columns(self, cols):
        parser = ParseXML(self.src_xml_file)
        for col in cols:
            query = parser.get_add_col_ddl(col)
            self.db_manager.alter_column(query)
            
    def remove_columns(self, cols):
        parser = ParseXML(self.src_xml_file)
        for col in cols:
            query = parser.get_rm_col_ddl(col)
            self.db_manager.alter_column(query)

    def update_columns(self, cols):
        parser = ParseXML(self.src_xml_file)
        for col in cols:
            query = parser.get_update_col_ddl(col)
            self.db_manager.alter_column(query)
        
    def add_xml_files(self, files):
        for file in files:
            src_file_path = "%s/%s" % (self.src_xml_path, file)
            parser = ParseXML(src_file_path)
            query = parser.get_ddl()
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
        src_json = ParseXML(self.src_xml_file)
        src_col_names = src_json.get_col_names()
        src_pkeys = None
        if src_json.is_pkey_exist():
            src_pkeys = src_json.pkeys

        self.target_xml_file = "%s/%s" % (self.target_xml_path, file)
        target_json = ParseXML(self.target_xml_file)
        target_col_names = target_json.get_col_names()
        target_pkeys = None
        if target_json.is_pkey_exist():
            target_pkeys = target_json.pkeys

        new_col_names = list(set(src_col_names) - set(target_col_names))
        del_col_names = list(set(target_col_names) - set(src_col_names))
        common_cols = list(set(src_col_names) & set(target_col_names))
        self.add_columns(new_col_names)
        self.remove_columns(del_col_names)
        self.update_columns(common_cols)
        rsync_files(self.src_xml_file, self.target_xml_file)
        
    def diff_xml_files(self):
        src_files = set(self.get_xml_files(self.src_xml_path))
        target_files = set(self.get_xml_files(self.target_xml_path))

        new_files = list(src_files - target_files)
        self.add_xml_files(new_files)

        remove_files = list(target_files - src_files)
        self.remove_xml_files(remove_files)

        cmp_files = list(src_files & target_files)
        for file in cmp_files:
            self.cmp_xml_files(file)
        

    
if __name__ == '__main__':
    src_path = "/home/sripathi/projects/sql-xml-utility/src"
    dest_path = "/home/sripathi/projects/sql-xml-utility/dest"
    d = DiffXML(src_path, dest_path)
    d.diff_xml_files()
