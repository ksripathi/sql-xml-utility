import json
import xmltodict
from db_manager import DBManager

def xml_to_json(xml_file):
    with open(xml_file, 'r') as f:
        xmlString = f.read()
    jsonString = json.dumps(xmltodict.parse(xmlString), indent=4)
    return json.loads(jsonString)

def get_table_name(xml_file):
    return xml_file.split('.')[0].split('/')[-1]

class ParseXML(object):

    def __init__(self, xml_file):

        self.xml_file = xml_file
        self.data = xml_to_json(xml_file)
        self.keys = None
        self.pkeys= None

    def get_add_col_ddl(self, col_name):
        col = [i for i in self.get_columns() if i['@Field'] == col_name]
        table_name = get_table_name(self.xml_file)
        col = col[0]
        if col is not None:
            if col['@Null'] == 'YES':
                col['@Null'] = 'NULL'
            elif col['@Null'] == 'NO':
                col['@Null'] = 'NOT NULL'
            else:
                col['@Null'] = ''
            query = "ALTER TABLE %s ADD %s %s %s %s" % (table_name, col['@Field'], col['@Type'], col['@Null'], col['@Extra'])
            if col.has_key('@DEFAULT'):
                query = "%s DEFAULT %s;" % (query, col['@DEFAULT'])
            else:
                query = "%s;" % query
        return query

    def get_rm_col_ddl(self, col_name):
        table_name = get_table_name(self.xml_file)
        query = "ALTER TABLE %s DROP %s;" % (table_name, col_name)
        return query

    def get_update_col_ddl(self, col_name):
        table_name = get_table_name(self.xml_file)
        col = [i for i in self.get_columns() if i['@Field'] == col_name]
        col = col[0]
        if col is not None:
            if col['@Null'] == 'YES':
                col['@Null'] = 'NULL'
            elif col['@Null'] == 'NO':
                col['@Null'] = 'NOT NULL'
            else:
                col['@Null'] = ''
            query = "ALTER TABLE %s MODIFY %s %s %s %s" % (table_name, col['@Field'], col['@Type'], col['@Null'], col['@Extra'])
            if col.has_key('@DEFAULT'):
                query = "%s DEFAULT %s;" % (query, col['@DEFAULT'])
            else:
                query = "%s;" % query
        return query

    
        
    def get_col_names(self):
        cols = [i['@Field'] for i in self.get_columns() if i.has_key('@Field')]
        return cols
    
    def is_key_exist(self):
        try:
            keys = self.data['mysqldump']['database']['table_structure']['key']
            if type(keys) is list:
                self.keys = keys
            else:
                temp_list = []
                temp_list.append(keys)
                self.keys = temp_list
            return True
        except Exception as e:
            print str(e)
            print "No key is exist"
            return False

    def get_db_engine(self):
        return self.data['mysqldump']['database']['table_structure']['options']['@Engine']
        
    def is_pkey_exist(self):
        try:
            if self.is_key_exist():

                pkeys = filter(lambda key: key['@Key_name'] == 'PRIMARY', self.keys)
                if type(pkeys) is list:
                    self.pkeys =  pkeys
                else:
                    temp_list = []
                    templ_list.append(pkeys)
                    self.pkeys = temp_list
                return True

        except Exception as e:
            print str(e)
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
        table_name = get_table_name(self.xml_file)
        cols = self.get_columns()
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
                    query = query + ")"
                else:
                    query = query + "DEFAULT '%s'\n)" % (col['@Default'])

        query = query + "Engine=%s;\n" % (self.get_db_engine())

        return query
    
if __name__ == '__main__':
    file_name = "/home/sripathi/projects/sql-xml-utility/src/src_pet.xml"
    xml_parser = ParseXML(file_name)
    query = xml_parser.get_ddl()
    print query
    # db_manager = DBManager()
    # with open('config.json') as f:
    #     config = json.load(f)
    # conn = db_manager.open_connection(**config['qa'])
    # db_manager.create_table(query)
    # #db_manager.drop_table(xml_parser.get_table_name())
    
