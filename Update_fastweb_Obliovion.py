import mysql.connector
import logging
import psycopg2
import pymysql
import argparse
# from nps_zip_module import zip_dir
from datetime import datetime, timedelta


def help_msg():
    """ help to describe the script"""
    help_str = """
               """
    return help_str

def write_sql_script(directory, file_name, table_ref, time_label, data):
    report_file = '{}\{}_{}_{!s}.{}'.format(directory, file_name, table_ref,  time_label, 'sql')


    fp = open(report_file, 'w+')
    #myFile = csv.writer(fp, lineterminator='\n', delimiter=';', )  # use lineterminator for windows
    for elemen in data:
        fp.write(elemen+'\n')
    fp.close()

def log_tables_on_working(lg, dict_tables_column ):
    num_tbl_da_lavorare=0
    num_tbl_escluse = 0
    for tab in dict_tables_column:
       columns_list_tbl = dict_tables_column[tab]
       if len(columns_list_tbl) != 0:
           lg.info(f'Tabella da processare = {tab}')
           num_tbl_da_lavorare = num_tbl_da_lavorare + 1
       else:
           lg.info(f'Tabella ESCLUSA = {tab}')
           num_tbl_escluse = num_tbl_escluse + 1
    report = f'Num Tabelle Processate = {num_tbl_da_lavorare} - Num Tabelle Escluse = {num_tbl_escluse} '
    return report

if __name__ == '__main__':


    parser = argparse.ArgumentParser(description=help_msg())
    parser.add_argument('-d', '--directory_report',
                        default='C:\\Users\\roberto.garofalo\\Documents\\WorkingEnv\\NPS_DATA_DICTIONARY',
                        help='Directory dove risiedono gli script sql ', required=False)
    parser.add_argument('-t', '--tabella_catalogo',
                        default='map_db801rco',
                        help='tabella schema catalogo', required=False)
    parser.add_argument('-f', '--test',
                        default='false',
                        help='test se true', required=False)
    parser.add_argument('-r', '--db_repository',
                        default='false',
                        help='database storico', required=False)
    args = parser.parse_args()

    directory = args.directory_report
    test = args.test
    db_storico = args.db_repository


    days_to_subtract = 0
    now = datetime.today() - timedelta(days=days_to_subtract)
    time_label = now.strftime("%d-%m-%Y")

    #logging setting
    log_file = '{}\{}_{!s}.{}'.format(directory, 'oblivion_app', time_label, 'log')
    lg = logging
    lg.basicConfig(filename=log_file, filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    lg.info("START - OBLIVION NPS DATA CATALOG")
    lg.debug(f'Directory script = {directory}')
    schema_name = 'nps'
    schema_name_dest = 'catalog'
    view = 'VIEW'
    operative = '%OP - %'
    temporanee = '%TEMP - %'
    table_catalog = args.tabella_catalogo
        #'map_db801rco'
    report_conf = {
        'ESTRAI_TABELLE_NPS': 'SELECT table_name  FROM  information_schema.tables WHERE table_schema = X AND (TABLE_COMMENT <> Y) AND(TABLE_COMMENT NOT LIKE Z AND TABLE_COMMENT NOT LIKE J)'.replace(
            'X', repr(str(schema_name))).replace('Y', repr(str(view))).replace('Z', repr(str(operative))).replace('J', repr(str(temporanee))),
        'ESTRAI_LIST_COLONNE_NPS': 'SELECT column_name FROM information_schema.columns where table_schema = X and table_name = Y'.replace(
            'X', repr(str(schema_name))),
        'UPGRADE_TABELLA_CATALOGO': 'UPDATE catalog.B  SET oblivion = X, o_field_rif_1 = J, o_fields = Z  WHERE source_schema = K AND source_table = L;'.replace(
            'K', repr(str(schema_name))).replace('B',str(table_catalog))
    }
    if (test == True):
        lg.info("START - in TEST MODE")
        db =  mysql.connector.connect(host="localhost",  # your host
                             user="root",  # username
                             passwd="rgarofal",  # password
                             db="nps")  # name of the database
    else:
        if db_storico == 'True':
            lg.info("START - in PRODUZIONE MODE su STORICO ")
            db = mysql.connector.connect(host="db804rco.intranet.fw",  # your host
                                         user="rogarofalo",  # username
                                         passwd="R073r7o£2",  # password
                                         db="nps",
                                         port=3307)  # name of the database

        else:
           lg.info("START - in PRODUZIONE MODE")
           db = mysql.connector.connect(host="db801rco.intranet.fw",  # your host
                                        user="rogarofalo",  # username
                                        passwd="R0garof4lo",  # password
                                        db="nps")  # name of the database

    cur = db.cursor()
    select_lista_tabelle = report_conf['ESTRAI_TABELLE_NPS']
    cur.execute(select_lista_tabelle)
    result = cur.fetchall()
    stat_tab_totale = len(result)
    # result = dao.select(select_lista_tabelle)
    dict_tables_column = dict()
    pattern_to_check = ('acc', 'mail', 'cust', 'customer_number')
    pattern_to_exclude =('stato_account','email_creazione','email_gestione','email_modifica','email_creazione_tts')
    for table_to_check in result:
       table = table_to_check[0]
       lg.debug(f'Processing the table = {table}')
       sql_check_col = report_conf['ESTRAI_LIST_COLONNE_NPS'].replace('Y', repr(table))
       lg.debug(f'SQL check colonne = {sql_check_col}')
       cur.execute(sql_check_col)
       result_columns = cur.fetchall()
       # result_columns = dao.select(sql_check_col)
       columns_label = []
       for column_name in result_columns:
          column_n = column_name[0]
          for pattern in pattern_to_check:
            if pattern in column_n:
                columns_label.append(column_name)
          dict_tables_column.update({table: columns_label})
    #Aggiorno log con stato tabelle
    stat_report = log_tables_on_working(lg, dict_tables_column )
    stat_num_table_to_cat = 0
    update_sql_commands = []
    for tab in dict_tables_column:
       lg.info(f'Check TABELLA SU CATALOGO = {tab}')
       columns_list = dict_tables_column[tab]
       to_add = False
       columns_value_l = []
       log_only_once = True

       for name_col in columns_list:
           lg.debug(f'Nome colonna estratta dal catalogo tabella = {name_col}')
           if (pattern_to_check[0] in name_col[0] or pattern_to_check[3] in name_col[0]) and (pattern_to_exclude[0] not in name_col[0]):
               lg.debug(f'Ho trovato account o customer_number - {name_col[0]} - {tab}')
               update_sql = report_conf['UPGRADE_TABELLA_CATALOGO'].replace('X', repr(str('Y'))).replace('L', repr(
                str(tab))).replace('J', repr(str(name_col[0]))).replace('Z', 'null')
               lg.debug(update_sql)
               to_add = True
               if log_only_once:
                   lg.info(f'TABELLA DA AGGIORNARE SU CATALOGO = {tab}')
                   stat_num_table_to_cat = stat_num_table_to_cat +1
                   log_only_once = False
           if (pattern_to_check[1] in name_col[0] or pattern_to_check[2] in name_col[0]) and (pattern_to_exclude[1] not in name_col[0] and pattern_to_exclude[2] not in name_col[0] and pattern_to_exclude[3] not in name_col[0]):
               lg.debug(f'Ho trovato le colonne di informazione - {name_col[0]} - {tab}')
               if name_col[0] != pattern_to_check[3]:
                  columns_value_l.append(name_col[0])
               # ', '.join(name_col[0])

       if len(columns_value_l) != 0:
           columns_value = ";".join(str(val) for val in columns_value_l)
           lg.debug(f'Colonne da Aggiungere = {columns_value}')
           update_sql = update_sql.replace('null', repr(str(columns_value)))
           lg.debug(f'Update finale = {update_sql}')
       if to_add == True:
           update_sql_commands.append(update_sql)
    lg.debug(update_sql_commands)
    stat_report = stat_report + f' Num TOTALE TABELLE = {stat_tab_totale}' + f' Numero tabelle da registrare su catalog  = {stat_num_table_to_cat}' f' Num TABELLE DA AGGIORNARE SUL CATALOG = {len(update_sql_commands)}'
    lg.info(f'STATISTICA TABELLE = {stat_report}')
    file_name = 'script_sql_'
    lg.info("SCRITTURA SUL FILE SCRIPT SQL")
    write_sql_script(directory, file_name, time_label, table_catalog, update_sql_commands)