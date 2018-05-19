import json
from xml_parser import *
import os

class DiffXML(object):
    
    def __init__(self, src_xml_path, target_xml_path):

        self.src_xml_path = src_xml_path
        self.target_xml_path = target_xml_path
    def get_xml_files(self, path):
        files = [name for name in os.listdir(path) if name.endswith('.xml')]
        return files

    def add_xml_files(self, files):
        for file in files:
            os.chdir(self.src_xml_path)
            parser = ParseXML(file)
            print parser.get_ddl()

    def remove_xml_files(self):
        pass

    def compare_xml_files(self, src_xml, target_xml):
        pass

    def diff_xml_files(self):
        src_files = set(self.get_xml_files(self.src_xml_path))
        target_files = set(self.get_xml_files(self.target_xml_path))
        new_files = src_files - target_files
        self.add_xml_files(new_files)
        # remove_files = target_files - src_files
        # self.remove_xml_files(remove_files)
        # cmp_files = src_files & target_files
        # self.cmp_files(cmp_files)
        

    
if __name__ == '__main__':
    print "hello"
    src_path = "/home/sripathi/"
    dest_path = "/home/sripathi/project/"
    d = DiffXML(src_path, dest_path)
    d.diff_xml_files()
