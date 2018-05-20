import json
import xmltodict
from db_manager import DBManager

COL_STR_TYPES = ["CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT","LONGTEXT"]

def is_str_type(value):
    value = value.split('(')[0].upper()
    if value in COL_STR_TYPES:
        return True
    else:
        return False
        
def xml_to_json(xml_file):
    try:
        with open(xml_file, 'r') as f:
            xml_str = f.read()
        json_str = json.dumps(xmltodict.parse(xml_str), indent=4)
        return json.loads(json_str)
    except Exception as e:
        raise str(e)

def get_table_name(xml_file):
    return xml_file.split('.')[0].split('/')[-1]


class XMLParser(object):

    def __init__(self):

        self.xml_file = None
        self.table_name = None
        self.json_obj = None
        self.keys = None
        self.pkeys= None
        
    def parse(self, xml_file):
        self.xml_file = xml_file
        self.table_name = get_table_name(self.xml_file)
        self.json_obj = xml_to_json(self.xml_file)

    def get_table_name(self):
        try:
            return self.xml_file.split('.')[0].split('/')[-1]
        except Exception as e:
            raise str(e)
        
    def get_add_pkey_ddl(self, keys):

        if keys:
            query = "ALTER TABLE %s ADD PRIMARY KEY(" % (self.table_name)
            keys = ",".join(keys)
            query = "%s%s);\n" % (query, keys)
            print query
            return query
        
    def get_rm_pkey_ddl(self):
        query = "ALTER TABLE %s DROP PRIMARY KEY;" % (self.table_name)
        return query

    def get_rm_col_ddl(self, col_name):
        query = "ALTER TABLE %s DROP %s;" % (self.table_name, col_name)
        return query
    
    def get_null_value(self, value):
        
        if value == 'YES':
            return 'NULL'
        elif value == 'NO':
            return 'NOT NULL'
        else:
            return ''
        
    def get_add_col_ddl(self, col_name):

        ####### Code to be refactored ######### this should return dict insted list of dict
        col = filter(lambda col: col['@Field'] == col_name, self.get_columns())
        #col = [col for col in self.get_columns() if col['@Field'] == col_name]
        print col
        col = col[0]
        col['@Null'] = self.get_null_value(col['@Null'])
        col_name = col['@Field']
        col_type = col['@Type']
        col_null = col['@Null']
        col_extra = col['@Extra']
            
        query_str = "ALTER TABLE %s ADD %s %s %s %s" % (self.table_name, col_name, col_type, col_null, col_extra)
        if col.has_key('@Default'):
            col_default = col['@Default']
            query_str = "%s Default" % (query_str) 
            if is_str_type(col_type):
                query = "%s '%s';" % (query_str, col_default)
            else:
                query = "%s %s;" % (query_str, col_default)
        else:
            query = "%s;" % query_str
        return query


    def get_update_col_ddl(self, col_name):
        col = filter(lambda col: col['@Field'] == col_name, self.get_columns())
        #col = [col for col in self.get_columns() if col['@Field'] == col_name]
        col = col[0]
        col['@Null'] = self.get_null_value(col['@Null'])
        col_name = col['@Field']
        col_type = col['@Type']
        col_null = col['@Null']
        col_extra = col['@Extra']

        query_str = "ALTER TABLE %s MODIFY %s %s %s %s" % (self.table_name, col_name, col_type, col_null, col_extra)
        if col.has_key('@Default'):
            col_default = col['@Default']
            query_str = "%s Default" % (query_str) 
            if is_str_type(col_type):
                query = "%s '%s';" % (query_str, col_default)
            else:
                query = "%s %s;" % (query_str, col_default)
        else:
            query = "%s;" % query_str
        return query

    def get_col_names(self):
        col_names = [col['@Field'] for col in self.get_columns() if col.has_key('@Field')]
        return col_names
    
    def is_key_exist(self):
        try:
            keys = self.json_obj['mysqldump']['database']['table_structure']['key']
            if type(keys) is list:
                self.keys = keys
            else:
                key_list = []
                key_list.append(keys)
                self.keys = key_list
            return True
        except Exception as e:
            print str(e)
            return False

    def get_db_engine(self):
        return self.json_obj['mysqldump']['database']['table_structure']['options']['@Engine']
    
    def get_pkey_names(self):
        pkey_names = [key['@Column_name'] for key in self.pkeys if key['@Key_name'] == 'PRIMARY']
        return pkey_names
        
    def is_pkey_exist(self):

        try:
            if self.is_key_exist():
                print self.keys
                pkeys = filter(lambda key: key['@Key_name'] == 'PRIMARY', self.keys)
                if not pkeys:
                    return False
                elif type(pkeys) is dict:
                    pkey_list = []
                    pkey_list.append(pkeys)
                    self.pkeys = pkey_list
                else:
                    self.pkeys =  pkeys
                return True
            else:
                return False
        except Exception as e:
            print str(e)
            return False

    def get_columns(self):

        cols = self.json_obj['mysqldump']['database']['table_structure']['field']
        if type(cols) is list:
            return cols
        else:
            cols_list = []
            cols_list.append(cols)
            return cols_list

    def get_pkey_ddl_stmt(self, pkeys):
        query_str = "PRIMARY KEY ("
        print "%%%%%%%%"
        print pkeys
        query = "%s%s),\n" % (query_str, ",".join(pkeys))
        return query
    
    def get_create_table_ddl(self):
        cols = self.get_columns()
        query_str = "CREATE TABLE %s(\n" % (self.table_name)
        col_no = 0

        for col in cols:
            col['@Null'] = self.get_null_value(col['@Null'])
            col_name = col['@Field']
            col_type = col['@Type']
            col_null = col['@Null']
            col_extra = col['@Extra']

            col_no += 1
            if col_no != len(cols):
                query_str = query_str + "%s %s %s %s" % (col_name, col_type, col_null, col_extra)
                if not col.has_key('@Default'):
                    query_str = query_str + ",\n"
                else:
                    col_default = col['@Default']
                    query_str = "%s Default" % (query_str) 
                    if is_str_type(col_type):
                        query_str = "%s '%s',\n" % (query_str, col_default)
                    else:
                        query_str = "%s %s,\n" % (query_str, col_default)
            else:
                if self.is_pkey_exist():
                    query_str = query_str + self.get_pkey_ddl_stmt(self.get_pkey_names())
                query_str = query_str + "%s %s %s %s" % (col_name, col_type, col_null, col_extra)
                if not col.has_key('@Default'):
                    query_str = query_str + ")"
                else:
                    col_default = col['@Default']
                    query_str = "%s Default" % (query_str) 
                    if is_str_type(col_type):
                        query_str = "%s '%s',\n" % (query_str, col_default)
                    else:
                        query_str = "%s %s,\n" % (query_str, col_default)

        query = query_str + "Engine=%s;\n" % (self.get_db_engine())

        return query
    
if __name__ == '__main__':
    print "xml_parser module"
    # file_name = "/home/sripathi/projects/sql-xml-utility/src/pet.xml"
    # xml_parser = XMLParser()
    # xml_parser.parse(file_name)
    # query = xml_parser.get_create_table_ddl()
    # print query
    # db_manager = DBManager()
    # with open('config.json') as f:
    #     config = json.load(f)
    # conn = db_manager.open_connection(**config['qa'])
    # db_manager.create_table(query)
    # #db_manager.drop_table(xml_parser.get_table_name())
    
