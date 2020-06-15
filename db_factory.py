import mysql.connector
import psycopg2
import pymysql
import argparse
#from nps_zip_module import zip_dir
from datetime import datetime, timedelta

"""
Define an interface for creating an object, but let subclasses decide
which class to instantiate. Factory Method lets a class defer
instantiation to subclasses.
"""

import abc


class Creator(metaclass=abc.ABCMeta):
    """
    Declare the factory method, which returns an object of type Product.
    Creator may also define a default implementation of the factory
    method that returns a default ConcreteProduct object.
    Call the factory method to create a Product object.
    """

    def __init__(self):
        self.product = self._factory_method()

    @abc.abstractmethod
    def _factory_method(self, type_report="NONE"):
        pass

    """
    Non mi sembra utile
    def some_operation(self):
        self.product.interface()
    """


class ConcreteCreatorMySQL(Creator):
    """
    Override the factory method to return an instance of a
    ConcreteProduct1.
    """

    def _factory_method(self):
        return ConcreteDatabaseMySQL()


class ConcreteCreatorPostSQL(Creator):
    """
    Override the factory method to return an instance of a
    ConcreteProduct2.
    """

    def _factory_method(self):
        return ConcreteDatabasePostSQL()


class Database(metaclass=abc.ABCMeta):
    """
    Define the interface of objects the factory method creates.
    """

    @abc.abstractmethod
    def connection(self,
                   host,  # your host
                   user,  # username
                   passwd,  # password
                   db):
        pass

    @abc.abstractmethod
    def get_columns(self):
        pass

    @abc.abstractmethod
    def select(self, statement):
        pass

    @abc.abstractmethod
    def delete(self, statement):
        pass

    @abc.abstractmethod
    def update(self, statement):
        pass

    @abc.abstractmethod
    def insert(self, statement):
        pass


class ConcreteDatabaseMySQL(Database):
    """
    Implement the Product interface.
    """

    def __init__(self):
        self.db_con = None
        self.cur = None
        self.db_host = None

    def name_server(self):
        return self.db_host

    def connection(self,
                   host="db801rco.intranet.fw",  # your host
                   user="rogarofalo",  # username
                   passwd="R0garof4lo",  # password
                   db="nps"):
        self.db_con = pymysql.connect(host,  # your host
                                      user,  # username
                                      passwd,  # password
                                      db)  # name of the database
        mysql.connector.connect()
        self.cur = self.db_con.cursor()
        self.db_host = 'db801rco'

    def select(self, statement):
        self.cur.execute(statement)
        return self.cur.fetchall()

    def get_columns(self):
        return self.cur.description

    def delete(self, statement):
        pass

    def update(self, statement):
        pass

    def insert(self, statement):
        self.cur.execute(statement)


class ConcreteDatabasePostSQL(Database):
    """
    Implement the Product interface.
    """

    def __init__(self):
        self.db_con = None
        self.cur = None

    def connection(self,
                   host="db801rco.intranet.fw",  # your host
                   user="rogarofalo",  # username
                   passwd="R0garof4lo",  # password
                   db="nps"):
        self.db_con = psycopg2.connect(host,  # your host
                                       user=user,  # username
                                       password=passwd,  # password
                                       database=db)  # name of the database

    def select(self, statement):
        self.cur = self.db_con.cursor()
        self.cur.execute(statement)
        return self.cur.fetchall()

    def get_columns(self):
        return self.cur.description

    def delete(self, statement):
        pass

    def update(self, statement):
        pass


class Scriptsql(metaclass=abc.ABCMeta):
    """
    Define the interface of objects the factory method creates.
    """

    @abc.abstractmethod
    def produce_script_sql(self, dao):
        pass


class ConcreteSqlScript(Scriptsql):
    """
    Implement the Report interface.
    """

    def __init__(self, directory_log, interval_days=0, schema_name ='nps', schema_name_dest = 'catalog'):
        self.directory = directory_log
        self.report_conf = dict()
        self.file_name_start = 'sql_aggiornamento_'
        self.days_to_subtract = interval_days
        self.list_file = []
        self.time_label = ''
        self.zip_file_name = None
        now = datetime.today() - timedelta(days=self.days_to_subtract)
        self.schema_name = schema_name
        self.schema_name_dest = schema_name_dest
#select table_name from information_schema.tables where table_schema = 'nps';
#select column_name from information_schema.columns where table_schema = 'nps' and table_name = 'ACCOUNT';
        lista_esclusione = '';
        view = 'VIEW'
        operative = '%OP%'
        temporanee = '%TEMP%'


        self.report_conf = {
            'ESTRAI_TABELLE_NPS': 'SELECT table_name  FROM  information_schema.tables WHERE table_schema = X AND (TABLE_COMMENT <> Y) AND(TABLE_COMMENT NOT LIKE Z AND TABLE_COMMENT NOT LIKE J)'.replace(
                'X', repr(str(schema_name))).replace('Y',repr(str(view))).replace('Z',repr(str(operative))).replace('J',repr(str(temporanee))),
            'ESTRAI_LIST_COLONNE_NPS': 'SELECT column_name FROM information_schema.columns where table_schema = X and table_name = Y'.replace(
                'X', repr(str(schema_name))),
            'UPGRADE_TABELLA_CATALOGO': 'UPDATE catalog.map_db801rco(oblivion, o_field_rif_1, o_fields)  SET ( X, J, Z)  WHERE source_schema = K AND source_table = L'.replace(
                'k', repr(str(schema_name)))
        }
        self.report_items = self.report_conf.items()

    def produce_script_sql(self, dao):
        now = datetime.today() - timedelta(days=self.days_to_subtract)
        self.time_label = now.strftime("%d-%m-%Y")
        report_file = '{}\{}_{}_{!s}.{}'.format(self.directory, self.file_name_start, 'update_catalog',
                                                self.time_label,
                                                'sql')
        select_lista_tabelle = self.report_conf['ESTRAI_TABELLE_NPS']
        result = dao.select(select_lista_tabelle)
        dict_tables_column = dict()
        pattern_to_check = ('acc', 'mail', 'cust')
        for table_to_check in result:
            table = table_to_check[0]
            print("Processing the table = ", table)
            sql_check_col = self.report_conf['ESTRAI_LIST_COLONNE_NPS'].replace('Y', repr(table))
            print("SQL di check colonne = ", sql_check_col)
            # cur.execute(sql_check_col)
            # result_columns = cur.fetchall()
            result_columns = dao.select(sql_check_col)
            columns_label = []
            for column_name in result_columns:
                column_n = column_name[0]
                for pattern in pattern_to_check:
                    if pattern in column_n:
                        columns_label.append(column_name)
                dict_tables_column.update({table: columns_label})
        print(dict_tables_column)
        update_sql = self.report_conf['UPGRADE_TABELLA_CATALOGO']
        update_sql_commands = []
        for tab in dict_tables_column:
            columns_list = dict_tables_column[tab]

            update_sql_commands


        for report_conf in self.report_items:
            report_file = '{}\{}_{}_{!s}.{}'.format(self.directory, self.file_name_start, report_conf[0],
                                                    self.time_label,
                                                    'csv')
            self.list_file.append(report_file)
            # Select data from table using SQL query.
            result = dao.select(report_conf[1])

            # Getting Field Header names
            column_names = [i[0] for i in dao.get_columns()]
            fp = open(report_file, 'w+')
            my_file = csv.writer(fp, lineterminator='\n', delimiter=';', )  # use lineterminator for windows
            my_file.writerow(column_names)
            my_file.writerows(result)
            fp.close()

    # def produce_zip_report(self, nome_zip: str):
    #     nome_zip_file = '{}\{}_{!s}.{}'.format(self.directory, nome_zip, self.time_label,
    #                                            'zip')
    #     # filtro_file = '{}{!s}.{}'.format('*', self.time_label, 'csv')
    #     filtro_file = '{!s}{!s}.{}'.format(self.file_name_start + '*', self.time_label, 'csv')
    #     self.zip_file_name = nome_zip_file
    #     zip_dir(nome_zip_file, self.directory, filtro_file)

    # def get_file_name_zip(self):
    #     return self.zip_file_name


class ConcreteBaseReportConfigurator(Creator):
    """
    Override the factory method to return an instance of a
    ConcreteProduct1.
    """

    def _factory_method(self, type_report):
            return ConcreteSqlScript()

def help_msg():
    """ help to describe the script"""
    help_str = """
               """
    return help_str

def main():
    parser = argparse.ArgumentParser(description=help_msg())
    parser.add_argument('-d', '--directory_report',
                      default='C:\\Users\\rogarofalo\\Documents\\WorkingEnv\\NPS_DATA_DICTIONARY',
                      help='Directory dove risiedono gli script sql ', required=False)

    args = parser.parse_args()

    directory = args.directory_report
    print(directory)
    # Produzione
    # concrete_db_nps = ConcreteDatabaseMySQL()
    # concrete_db_nps.connection(host="db801rco.intranet.fw",  # your host
    #                             user="rogarofalo",  # username
    #                             passwd="R0garof4lo",  # password
    #                             db="nps")
    # concrete_db_data_conf = ConcreteDatabaseMySQL()
    # concrete_db_data_conf.connection(host="db801rco.intranet.fw",  # your host
    #                            user="rogarofalo",  # username
    #                            passwd="R0garof4lo",  # password
    #                            db="catalog")

    #TEST
    concrete_db_nps = ConcreteDatabaseMySQL()
    concrete_db_nps.connection(host="localhost",  # your host
                                user="root",  # username
                                passwd="rgarofal",  # password
                                db="nps")
    concrete_db_data_conf = ConcreteDatabaseMySQL()
    concrete_db_data_conf.connection(host="localhost",  # your host
                               user="root",  # username
                               passwd="rgarofal",  # password
                               db="catalog")
    generator_script = ConcreteSqlScript(directory,0,'nps','catalog')
    generator_script.produce_script_sql(concrete_db_nps)

if __name__ == "__main__":
    main()
