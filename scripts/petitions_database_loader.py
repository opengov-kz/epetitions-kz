import json
import csv
import os
import itertools
import psycopg2

class PetitionsDatabaseLoader:
    def __init__(self, conn, csv_path):
        """
        Инициализация загрузчика петиций в базу данных

        Arguments:
        conn -- соединение с базой данных (psycopg2.connect)
        csv_path -- путь к CSV файлу
        """
        self.conn = conn
        with conn.cursor() as curs:
            curs.execute("""SELECT EXISTS(
                                SELECT FROM information_schema.tables
                                WHERE table_schema = 'public'
                                AND    table_name   = 'petitions');""")
            print(curs.fetchone())
            curs.execute("""CREATE TABLE "petitions" (
                              "id" int,
                              "uuid" uuid,
                              "title" text,
                              "description" text,
                              "reg_number" text,
                              "state_id" int,
                              "source_id" int,
                              "apply_date" timestamp,
                              "deadline" timestamp,
                              "signers_count" int,
                              "required_count" int,
                              "viewers_count" int,
                              "cover_file_id" int,
                              "applicant_first_name" text,
                              "applicant_last_name" text,
                              "organization_id" int,
                              "location_latitude" text,
                              "location_longitude" text,
                              "location_address" text,
                              "decision_message_kk" text,
                              "decision_message_ru" text,
                              "decision_reply_date" timestamp);""")
            conn.commit()
            

conn = psycopg2.connect("postgres://postgres:arman@localhost:5432/test")

petitions_db_loader = PetitionsDatabaseLoader(conn, "test.csv")
