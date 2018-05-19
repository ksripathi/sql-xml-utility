import json
from xml_parser import *
from db_manager import *
import os
from execute_commands import execute_command


def rsync_files(src_file, dest_file):
    try:
        copy_command = "rsync -arz --progress " + src_file + " " + dest_file
        command = ('%s' % (copy_command))
        print command
        (ret_code, output) = execute_command(command)
        if ret_code == 0:
            print copy_command
        else:
            print "Error in while command %s" % (copy_command)
    except Exception as e:
        print str(e)

def remove_files(file_path):
    try:
        cmd = "rm -rf %s" % (file_path)
        (ret_code, output) = execute_command(cmd)
        if ret_code == 0:
            print cmd
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
        

    def get_xml_files(self, path):
        # files = [os.path.join(path, name) for name in os.listdir(path) if name.endswith('.xml')]
        files = [name for name in os.listdir(path) if name.endswith('.xml')]
        return files

    def add_xml_files(self, files):
        for file in files:
            src_file_path = "%s/%s" % (self.src_xml_path, file)
            print src_file_path
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
            
    def compare_xml_files(self, src_xml, target_xml):
        pass

    def diff_xml_files(self):
        src_files = set(self.get_xml_files(self.src_xml_path))
        target_files = set(self.get_xml_files(self.target_xml_path))
        new_files = list(src_files - target_files)
        self.add_xml_files(new_files)
        remove_files = list(target_files - src_files)
        self.remove_xml_files(remove_files)
        #cmp_files = list(src_files & target_files)
        #print cmp_files
        # self.cmp_files(cmp_files)
        

    
if __name__ == '__main__':
    src_path = "/home/sripathi/projects/sql-xml-utility/src"
    dest_path = "/home/sripathi/projects/sql-xml-utility/dest"
    d = DiffXML(src_path, dest_path)
    d.diff_xml_files()
