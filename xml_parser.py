import json
import xmltodict
from db_manager import DBManager

def xml_to_json(xml_file):
    with open(xml_file, 'r') as f:
        xmlString = f.read()
    jsonString = json.dumps(xmltodict.parse(xmlString), indent=4)
    return json.loads(jsonString)

class ParseXML(object):

    def __init__(self, xml_file):

        self.xml_file = xml_file
        self.data = xml_to_json(xml_file)
        self.keys = None
        self.pkeys= None
        
    def get_table_name(self):
        return self.xml_file.split('.')[0]
    
    def is_key_exist(self):
        try:
            self.keys = self.data['mysqldump']['database']['table_structure']['key']
            return True
        except Exception as e:
            return False

    def is_pkey_exist(self):
        try:
            if self.is_key_exist():
                self.pkeys = filter(lambda key: key['@Key_name'] == 'PRIMARY', self.keys)
                return True
            else:
                return False
        except Exception as e:
            return False

    def get_columns(self):
        cols = self.data['mysqldump']['database']['table_structure']['field']
        if type(cols) is list:
            return cols
        else:
            temp = []
            temp.append(cols)
            return temp

    def get_query_stmt(self):
        query_stmt = "PRIMARY KEY ("
        count = 0
        for p_key in self.pkeys:
            count += 1
            if count != len(self.pkeys):
                query_stmt = query_stmt + "%s," % (p_key['@Column_name'])
            else:
                query_stmt = query_stmt + "%s),\n" % (p_key['@Column_name'])
        return query_stmt
    
    def get_ddl(self):
        table_name = self.get_table_name()
        cols = self.get_columns()
        print cols
        col_no = 0
        query = "create table %s(\n" % (table_name)
        primary = ""
        
        for col in cols:
            if col['@Null'] == "YES":
                col['@Null'] = "NULL"

            elif col['@Null'] == "NO":
                col['@Null'] = "NOT NULL"

            else:
                pass

            col_no += 1
            if col_no != len(cols):
                query = query + "%s %s %s %s" % (col['@Field'], col['@Type'], col['@Null'], col['@Extra'])
                if not col.has_key('DEFAULT'):
                    query = query + ",\n"
                else:
                    query = query + "DEFAULT '%s',\n" % (col['@Default'])
            else:
                count = 0
                if self.is_pkey_exist():
                    query = query + self.get_query_stmt()
                query = query + "%s %s %s %s" % (col['@Field'], col['@Type'], col['@Null'], col['@Extra'])
                if not col.has_key('@Default'):
                    query = query + ");\n"
                else:
                    query = query + "DEFAULT '%s'\n);\n" % (col['@Default'])

        return query
    
if __name__ == '__main__':
    
    xml_parser = ParseXML("test_db_lab_developers.xml")
    query = xml_parser.get_ddl()
    print query
    # db_manager = DBManager()
    # with open('config.json') as f:
    #     config = json.load(f)
    # conn = db_manager.open_connection(**config['qa'])
    # db_manager.create_table(query)
    # #db_manager.drop_table(xml_parser.get_table_name())
    
