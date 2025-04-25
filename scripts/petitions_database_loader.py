import json
import csv
import os
import itertools
import psycopg2
import re
import ast
import datetime
import errno

def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

class PetitionsDatabaseLoader:
    def __init__(self, conn):
        """
        Инициализация загрузчика петиций в базу данных

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
    
    def insert_name_and_return_id(self, table_name, name):
        """
        Добавление записи в таблицу со столбцами id и name в случае если name новый,
        и возвращение id
        """
        with self.conn.cursor() as curs:
            curs.execute("SELECT EXISTS (SELECT FROM "+table_name+" WHERE name = %s);", (name,))
            if not curs.fetchone()[0]:
                curs.execute("INSERT INTO "+table_name+" (name) VALUES (%s);", (name,))
                self.conn.commit()
            curs.execute("SELECT id FROM "+table_name+" WHERE name = %s;", (name,))
            return curs.fetchone()[0]

    def insert_location(self, location):
        """
        Добавление записи в таблицу локации в случае если локация новая,
        и возвращение id
        """
        with self.conn.cursor() as curs:
            curs.execute("SELECT EXISTS (SELECT FROM locations WHERE uuid = %s);", (location["id"],))
            if not curs.fetchone()[0]:
                type_id = self.insert_name_and_return_id("location_types", location["type"])
                curs.execute("""INSERT INTO locations (uuid, parent_uuid, name_ru, name_kk, type_id,
                                                        path, external_id, external_parent_id, kato_code)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                                                         (location["id"], location["parentId"], location["nameRu"],
                                                          location["nameKk"], type_id, location["path"],
                                                          location["externalId"], location["externalParentId"],
                                                          location["katoCode"]))
                self.conn.commit()
            curs.execute("SELECT id FROM locations WHERE uuid = %s;", (location["id"],))
            return curs.fetchone()[0]

    def insert_organization_type(self, org_type):
        """
        Добавление записи в таблицу типов организаций в случае если тип организации новый,
        и возвращение id
        """
        with self.conn.cursor() as curs:
            curs.execute("SELECT EXISTS (SELECT FROM organization_types WHERE uuid = %s);", (org_type["id"],))
            if not curs.fetchone()[0]:
                curs.execute("""INSERT INTO organization_types (uuid, name_ru, name_kk, is_unit, accept_appeal)
                                                                VALUES (%s, %s, %s, %s, %s);""",
                                                                 (org_type["id"], org_type["nameRu"],
                                                                  org_type["nameKk"], org_type["isUnit"],
                                                                  org_type["acceptAppeal"]))
                self.conn.commit()
            curs.execute("SELECT id FROM organization_types WHERE uuid = %s;", (org_type["id"],))
            return curs.fetchone()[0]
        
    
    def insert_organization(self, org):
        """
        Добавление записи в таблицу организаций в случае если организация новая,
        и возвращение id
        """
        with self.conn.cursor() as curs:
            curs.execute("SELECT EXISTS (SELECT FROM organizations WHERE uuid = %s);", (org["id"],))
            if not curs.fetchone()[0]:
                type_id = self.insert_organization_type(org["orgType"])
                location_id = self.insert_location(org["location"])
                curs.execute("""INSERT INTO organizations (uuid, name_ru, name_kk, parent_uuid,
                                                            type_id, location_id, path, has_unit)
                                                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s);""",
                                                             (org["id"], org["nameRu"], org["nameKk"],
                                                              org["parentId"], type_id, location_id,
                                                              org["path"], org["hasUnit"]))
                self.conn.commit()
            curs.execute("SELECT id FROM organizations WHERE uuid = %s;", (org["id"],))
            return curs.fetchone()[0]

    def insert_file(self, file):
        """
        Добавление записи в таблицу файлов в случае если файл новый,
        и возвращение id
        """
        if file is None:
            return None
        with self.conn.cursor() as curs:
            curs.execute("SELECT EXISTS (SELECT FROM files WHERE uuid = %s);", (file["fileId"],))
            if not curs.fetchone()[0]:
                if "mimeType" in file:
                    mime_type_id = self.insert_name_and_return_id("mime_types", file["mimeType"])
                else: mime_type_id = None
                curs.execute("""INSERT INTO files (uuid, name, hash, mime_type_id)
                                                    VALUES(%s, %s, %s, %s);""",
                                                     (file["fileId"], file["fileName"],
                                                      file["fileHash"], mime_type_id))
                self.conn.commit()
            curs.execute("SELECT id FROM files WHERE uuid = %s;", (file["fileId"],))
            return curs.fetchone()[0]

    def insert_file_to_petition(self, table_name, petition_id, file):
        """
        Добавление записи в таблицу файлов many-to-many в случае если связь новая
        """
        with self.conn.cursor() as curs:
            file_id = self.insert_file(file)
            curs.execute("SELECT EXISTS (SELECT FROM "+table_name+
                         " WHERE petition_id = %s AND file_id = %s);""",
                         (petition_id, file_id))
            if not curs.fetchone()[0]:
                curs.execute("INSERT INTO "+table_name+" (petition_id, file_id)"+
                             "VALUES(%s, %s);",
                             (petition_id, file_id))
                self.conn.commit()

    
    def load_petitions(self):
        """
        Добавление петиций в базу данных
        """
        csv_file_names = [f for f in os.listdir() if (os.path.isfile(f) and
                                                      re.search("^petitions.*[.]csv$", f))]
        csv_file_datetimes = [datetime.datetime.strptime(f[10:25], "%Y%m%d_%H%M%S")
                              for f in csv_file_names]
        csv_file_name = "petitions_"+max(csv_file_datetimes).strftime("%Y%m%d_%H%M%S")+".csv"
        
        with open(csv_file_name, newline='', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            column_names = next(csv_reader)
            
            with self.conn.cursor() as curs:
                curs.execute("""TRUNCATE TABLE decision_files CASCADE;
                                TRUNCATE TABLE petition_files CASCADE;
                                TRUNCATE TABLE files CASCADE;
                                TRUNCATE TABLE organization_types CASCADE;
                                TRUNCATE TABLE locations CASCADE;
                                TRUNCATE TABLE organizations CASCADE;
                                TRUNCATE TABLE petitions CASCADE;""")
                self.conn.commit()
            
            for row in csv_reader:
                petition = dict(zip(column_names, row))
                    
                uuid = petition["id"]
                title = petition["title"]
                description = petition["description"]
                reg_number = petition["regNumber"]
                    
                state_id = self.insert_name_and_return_id("petition_states", petition["state"])
                source_id = self.insert_name_and_return_id("petition_sources", petition["source"])
                    
                apply_date = datetime.datetime.fromisoformat(petition["applyDate"])
                deadline = datetime.datetime.fromisoformat(petition["deadline"])
                    
                applicant_first_name = ast.literal_eval(petition["applicant"])["firstName"]
                applicant_last_name = ast.literal_eval(petition["applicant"])["lastName"]
                    
                organization_id = self.insert_organization(ast.literal_eval(petition["organization"]))
                    
                language_id = self.insert_name_and_return_id("petition_languages", petition["lang"])
                    
                location_latitude = ast.literal_eval(petition["location"])["positionLatitude"]
                location_longitude = ast.literal_eval(petition["location"])["positionLongitude"]
                location_address = ast.literal_eval(petition["location"])["positionAddress"]
                    
                signers_count = petition["signersCount"]                    
                required_count = petition["requiredCount"]
                viewers_count = petition["viewersCount"]

                
                if petition["cover"] == "" or petition["cover"] is None:
                    cover_file_id = None
                else:
                    cover_file_id = self.insert_file(ast.literal_eval(petition["cover"]))

                petition_files = ast.literal_eval(petition["files"])

                if petition["decision"] == "" or petition["decision"] is None:
                    decision_message_kk, decision_message_ru, decision_reply_date, decision_files = None, None, None, []
                else:
                    decision_message_kk = ast.literal_eval(petition["decision"])["messageKk"]
                    decision_message_ru = ast.literal_eval(petition["decision"])["messageRu"]
                    decision_reply_date = datetime.datetime.fromisoformat(ast.literal_eval(petition["decision"])["replyDate"])
                    decision_files = ast.literal_eval(petition["decision"])["files"]

                with self.conn.cursor() as curs:
                    curs.execute("""INSERT INTO petitions (uuid, title, description, reg_number,
                                                            state_id, source_id, apply_date, deadline,
                                                            signers_count, required_count, viewers_count,
                                                            cover_file_id, applicant_first_name,
                                                            applicant_last_name, organization_id,
                                                            language_id, location_latitude,
                                                            location_longitude, location_address,
                                                            decision_message_kk, decision_message_ru,
                                                            decision_reply_date)
                                                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s,
                                                                    %s, %s, %s, %s, %s, %s, %s, %s,
                                                                    %s, %s, %s, %s, %s, %s);""",
                                                             (uuid, title, description, reg_number,
                                                                state_id, source_id, apply_date, deadline,
                                                                signers_count, required_count, viewers_count,
                                                                cover_file_id, applicant_first_name,
                                                                applicant_last_name, organization_id,
                                                                language_id, location_latitude,
                                                                location_longitude, location_address,
                                                                decision_message_kk, decision_message_ru,
                                                                decision_reply_date))
                    self.conn.commit()
                    curs.execute("SELECT id FROM petitions WHERE uuid = %s;", (uuid,))
                    petition_id = curs.fetchone()[0]
                    for file in petition_files:
                        self.insert_file_to_petition("petition_files", petition_id, file)
                    for file in decision_files:
                        self.insert_file_to_petition("decision_files", petition_id, file)

        for csv_file_name in csv_file_names:
            silent_remove(csv_file_name)

if __name__ == "__main__":
    conn = psycopg2.connect("postgres://postgres:arman@localhost:5432/test")
    petitions_db_loader = PetitionsDatabaseLoader(conn)
    petitions_db_loader.load_petitions()

