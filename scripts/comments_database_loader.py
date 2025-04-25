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

class CommentsDatabaseLoader:
    def __init__(self, conn):
        """
        Инициализация загрузчика комментариев в базу данных

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


    def insert_comment(self, comment):
        """
        Добавление комментария в базу данных
        """
        with self.conn.cursor() as curs:
            curs.execute("SELECT EXISTS (SELECT FROM comments WHERE uuid = %s);", (comment["id"],))
            if not curs.fetchone()[0]:
                if comment["parentId"] == "": comment["parentId"] = None
                if comment["repliesCount"] == "": comment["repliesCount"] = None
                curs.execute("""INSERT INTO comments (uuid, petition_uuid, parent_uuid, fio,
                                                        comment, replies_count, created_date)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s);""",
                                                         (comment["id"], comment["petitionId"],
                                                          comment["parentId"], comment["fio"],
                                                          comment["comment"], comment["repliesCount"],
                                                          comment["createdDate"]))
                self.conn.commit()
    
    def load_comments(self):
        """
        Добавление комментариев в базу данных
        """
        csv_file_datetimes = [datetime.datetime.strptime(f[9:24], "%Y%m%d_%H%M%S")
                              for f in os.listdir() if (os.path.isfile(f) and
                                                        re.search("^comments.*[.]csv$", f))]
        csv_file_datetimes.sort()
        csv_file_names = ["comments_"+f.strftime("%Y%m%d_%H%M%S")+".csv"
                          for f in csv_file_datetimes]
        
        for csv_file_name in csv_file_names:
            with open(csv_file_name, newline='', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file)
                column_names = next(csv_reader)
                rows = [[f[0], f[1], f[2], f[3], f[4], f[5],
                             datetime.datetime.fromisoformat(f[6])]
                            for f in csv_reader]
                rows = sorted(rows, key=operator.itemgetter(6))
                for row in rows:
                    comment = dict(zip(column_names, row))
                    self.insert_comment(comment)
            silent_remove(csv_file_name)

if __name__ == "__main__":
    conn = psycopg2.connect("postgres://postgres:arman@localhost:5432/test")
    comments_db_loader = CommentsDatabaseLoader(conn)
    comments_db_loader.load_comments()

