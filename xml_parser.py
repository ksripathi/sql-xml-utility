import json
import xmltodict
from db_manager import DBManager

COL_STR_TYPES = ["CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT","LONGTEXT"]

def is_str_type(value):
    try:
        value = value.split('(')[0].upper()
        if value in COL_STR_TYPES:
            return True
        else:
            return False
    except Exception as e:
        raise str(e)
    
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
        self.ukeys = None
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
        try:
            if keys:
                query = "ALTER TABLE %s ADD PRIMARY KEY(" % (self.table_name)
                keys = ",".join(keys)
                query = "%s%s);\n" % (query, keys)
                return query
        except Exception as e:
            raise str(e)
        
    def get_add_ukey_ddl(self, keys):
        try:
            if keys:
                query = "ALTER TABLE %s ADD UNIQUE KEY(" % (self.table_name)
                keys = ",".join(keys)
                query = "%s%s);\n" % (query, keys)
                return query
        except Exception as e:
            raise str(e)
        

    def get_rm_pkey_ddl(self):
        try:
            query = "ALTER TABLE %s DROP PRIMARY KEY;" % (self.table_name)
            return query
        except Exception as e:
            raise str(e)
        
    def get_rm_ukey_ddl(self, key):
        try:
            query = "DROP INDEX %s ON %s;" % (key, self.table_name)
            return query
        except Exception as e:
            raise str(e)

    def get_rm_col_ddl(self, col_name):
        try:
            query = "ALTER TABLE %s DROP %s;" % (self.table_name, col_name)
            return query
        except Exception as e:
            raise str(e)
    
    def get_null_value(self, value):
        try:
            if value == 'YES':
                return 'NULL'
            elif value == 'NO':
                return 'NOT NULL'
            else:
                return ''
        except Exception as e:
            raise str(e)
        
    def get_add_col_ddl(self, col_name):

        ####### Code to be refactored ######### this should return dict insted of list with single dict
        col = filter(lambda col: col['@Field'] == col_name, self.get_columns())
        #col = [col for col in self.get_columns() if col['@Field'] == col_name]
        col = col[0]
        col['@Null'] = self.get_null_value(col['@Null'])
        col_name = col['@Field']
        col_type = col['@Type']
        col_null = col['@Null']
        col_extra = col['@Extra']
            
        query_str = "ALTER TABLE %s ADD %s %s %s %s" % \
                    (self.table_name, col_name, col_type, col_null, col_extra)
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
        ####### Code to be refactored ######### this should return dict insted of list with single dict        
        col = filter(lambda col: col['@Field'] == col_name, self.get_columns())
        #col = [col for col in self.get_columns() if col['@Field'] == col_name]
        col = col[0]
        col['@Null'] = self.get_null_value(col['@Null'])
        col_name = col['@Field']
        col_type = col['@Type']
        col_null = col['@Null']
        col_extra = col['@Extra']

        query_str = "ALTER TABLE %s MODIFY %s %s %s %s" % \
                    (self.table_name, col_name, col_type, col_null, col_extra)
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
        try:
            col_names = [col['@Field'] for col in self.get_columns() \
                         if col.has_key('@Field')]
            return col_names
        except Exception as e:
            raise str(e)
    
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
        try:
            return self.json_obj['mysqldump']['database']\
                ['table_structure']['options']['@Engine']
        except Exception as e:
            raise str(e)

    def get_pkey_names(self):
        try:
            if self.is_pkey_exist():
                pkey_names = [key['@Column_name'] \
                              for key in self.pkeys \
                              if key['@Key_name'] == 'PRIMARY']
                return pkey_names
        except Exception as e:
            raise str(e)

    def is_ukey_exist(self):
        if self.get_ukeys():
            return True
        else:
            return False
        
    def is_col_has_ukey(self, col):
        try:
            if col['@Key'] == 'UNI':
                return True
            elif ((col['@Key'] == 'PRI') or (col['@Key'] == 'MUL')) and \
                 (any(key['@Key_name'] == col['@Field'] for key in self.keys)):
                return True
            else:
                return False
        except Exception as e:
            raise str(e)

        
    def get_ukeys(self):
        
        try:
            if self.is_key_exist():
                ukeys = [col for col in self.get_columns() \
                         if self.is_col_has_ukey(col)]
                self.ukeys = ukeys
                return ukeys
        except Exception as e:
            raise str(e)
        
    def get_ukey_names(self):
        try:
            if self.is_key_exist():
                pkey_names = [col['@Field'] for col in \
                              self.get_ukeys() if True]
                return pkey_names
        except Exception as e:
            raise str(e)
        
    def is_pkey_exist(self):

        try:
            if self.is_key_exist():
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
        try:
            cols = self.json_obj['mysqldump']['database']['table_structure']['field']
            if type(cols) is list:
                return cols
            else:
                cols_list = []
                cols_list.append(cols)
                return cols_list
        except Exception as e:
            raise str(e)

    def get_ukey_ddl_stmt(self, ukeys):

        try:
            if ukeys:
                query_str = "UNIQUE KEY ("
                query = "%s%s),\n" % (query_str, ",".join(ukeys))
                return query
        except Exception as e:
            raise str(e)
        
    def get_pkey_ddl_stmt(self, pkeys):
        
        try:
            if pkeys:
                query_str = "PRIMARY KEY ("
                query = "%s%s),\n" % (query_str, ",".join(pkeys))
                return query
        except Exception as e:
            raise str(e)
        
    def get_create_table_ddl(self):
            
        cols = self.get_columns()
        query_str = "CREATE TABLE %s(\n" % (self.table_name)
        col_no = 0
        try:
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

                    if self.is_ukey_exist():
                        query_str = query_str + self.get_ukey_ddl_stmt(self.get_ukey_names())
                        
                    query_str = query_str + "%s %s %s %s" % (col_name, col_type, col_null, col_extra)
                    if not col.has_key('@Default'):
                        query_str = query_str + ")"
                    else:
                        col_default = col['@Default']
                        query_str = "%s Default" % (query_str) 
                        if is_str_type(col_type):
                            query_str = "%s '%s')\n" % (query_str, col_default)
                        else:
                            query_str = "%s %s)\n" % (query_str, col_default)

            query = query_str + "Engine=%s;\n" % (self.get_db_engine())
            return query
        except Exception as e:
            raise str(e)
        
if __name__ == '__main__':
    print "xml_parser module"
    file_name = "/home/sripathi/projects/sql-xml-utility/src/Persons.xml"
    xml_parser = XMLParser()
    xml_parser.parse(file_name)
    ukeys = xml_parser.get_ukey_names()
    print xml_parser.get_ukey_ddl_stmt(ukeys)
    # pkeys = xml_parser.get_pkey_names()
    # print xml_parser.get_pkey_ddl_stmt(pkeys)
    # query = xml_parser.get_create_table_ddl()
    # print query
    # db_manager = DBManager()
    # with open('config.json') as f:
    #     config = json.load(f)
    # conn = db_manager.open_connection(**config['qa'])
    # db_manager.create_table(query)
    # #db_manager.drop_table(xml_parser.get_table_name())
    
