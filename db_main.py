import mysql.connector
import argparse
import csv
from datetime import datetime, timedelta


class DB:
    def connection(self, engine):
        global db
        if engine == 'MYSQL':
            db = mysql.connector.connect(host="db801rco.intranet.fw",  # your host
                                         user="rogarofalo",  # username
                                         passwd="R0garof4lo",  # password
                                         db="nps")  # name of the database
        return db


def init():

    file_name_start = 'F_report_assistenza_tecnica_'
    days_to_subtract = 0
    now = datetime.today() - timedelta(days=days_to_subtract)
    time_label = now.strftime("%d-%m-%Y")
    report_conf = {
        'CANCELLAZIONI': 'SELECT distinct * FROM nps.assistenza_tecnica_report where date(data_insert_rco) = current_date() - INTERVAL X DAY'.replace(
            'X', str(days_to_subtract)),
        'COMPLETO': 'SELECT distinct * FROM nps.assistenza_tecnica_stage where date(data_insert_rco) = current_date() - INTERVAL X DAY'.replace(
            'X', str(days_to_subtract)),
        'CONSOLIDATO': 'SELECT distinct * FROM nps.ASSISTENZA_TECNICA where date(data_insert_rco) = current_date() - INTERVAL X DAY'.replace(
            'X', str(days_to_subtract))}
    return report_conf


def connection():
    db = mysql.connector.connect(host="db801rco.intranet.fw",  # your host
                                 user="rogarofalo",  # username
                                 passwd="R0garof4lo",  # password
                                 db="nps")  # name of the database
    return db


def help_msg():
    """ help to describe the script"""
    help_str = """
               """
    return help_str


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=help_msg())
    parser.add_argument('-d', '--directory_report',
                        default='C:\\Users\\rogarofalo\\Documents\\WorkingEnv\\NPS_DATA_DICTIONARY',
                        help='Directory dove risiedono gli script sql ', required=False)

    args = parser.parse_args()

    directory = args.directory_report
    print(directory)
    init()
    db = connection()

    cur = db.cursor()

    days_to_subtract = 0
    now = datetime.today() - timedelta(days=days_to_subtract)
    time_label = now.strftime("%d-%m-%Y")
    report_configuration = init()
    file_name_start = 'script_sql_'
    report_items = report_configuration.items()
    list_file = []
    for report_conf in report_items:
        report_file = '{}\{}_{}_{!s}.{}'.format(directory, file_name_start, report_conf[0], time_label, 'csv')
        list_file.append(report_file)
        # Select data from table using SQL query.
        cur.execute(report_conf[1])
        result = cur.fetchall()

        # Getting Field Header names
        column_names = [i[0] for i in cur.description]
        fp = open(report_file, 'w+')
        myFile = csv.writer(fp, lineterminator='\n', delimiter=';', )  # use lineterminator for windows
        myFile.writerow(column_names)
        myFile.writerows(result)
        fp.close()
