import json
import csv
import os
import itertools
import psycopg2
import re
import ast
import datetime
import errno
import operator

def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

class SignersDatabaseLoader:
    def __init__(self, conn):
        """
        Инициализация загрузчика подписей в базу данных

        Arguments:
        conn -- соединение с базой данных (psycopg2.connect)
        """
        self.conn = conn
        with self.conn.cursor() as curs:
            curs.execute("""SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_name = 'petitions');""")
            if not curs.fetchone()[0]:
                with open("init_database.sql") as f:
                    curs.execute(f.read())
                    conn.commit()


    def insert_signer(self, signer):
        """
        Добавление подписи в базу данных
        """
        with self.conn.cursor() as curs:
            curs.execute("""INSERT INTO signers (petition_uuid, fio, created_date)
                                                    VALUES (%s, %s, %s);""",
                                                     (signer["petitionId"], signer["fio"],
                                                      signer["createdDate"]))
            self.conn.commit()
    
    def load_signers(self):
        """
        Добавление подписей в базу данных
        """
        csv_file_datetimes = [datetime.datetime.strptime(f[8:23], "%Y%m%d_%H%M%S")
                              for f in os.listdir() if (os.path.isfile(f) and
                                                        re.search("^signers.*[.]csv$", f))]
        csv_file_datetimes.sort()
        csv_file_names = ["signers_"+f.strftime("%Y%m%d_%H%M%S")+".csv"
                          for f in csv_file_datetimes]
        
        for csv_file_name in csv_file_names:
            with open(csv_file_name, newline='', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file)
                column_names = next(csv_reader)
                rows = [[f[0], datetime.datetime.fromisoformat(f[1]), f[2]]
                        for f in csv_reader]
                rows = sorted(rows, key=operator.itemgetter(1))
                with self.conn.cursor() as curs:
                    curs.execute("""SELECT EXISTS (
                                        SELECT FROM signers
                                        WHERE created_date >= %s);""",
                                 (rows[0][1],))
                    if not curs.fetchone()[0]:
                        for row in rows:
                            signer = dict(zip(column_names, row))
                            self.insert_signer(signer)
            silent_remove(csv_file_name)

if __name__ == "__main__":
    conn = psycopg2.connect("postgres://postgres:arman@localhost:5432/test")
    signers_db_loader = SignersDatabaseLoader(conn)
    signers_db_loader.load_signers()


