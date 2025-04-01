import requests
import json
import csv
from datetime import datetime
import os
import itertools
import errno

def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

class CommentsParser:
    def __init__(self, api_url="https://epetition.kz/api/public/v1/petitions"):
        """
        Инициализация парсера комментариев

        Arguments:
        api_url -- API url для получения комментариев
        """
        self.api_url = api_url
        self.timeout = 1
        self.max_page_size = 1
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        }
    
    def fetch_comments(self, petition_id):
        """
        Получение комментариев одной петиции из API

        Arguments:
        petition_id -- id петиции
        """
        comments = []
        for i in itertools.count():
            while True:
                try:
                    response = requests.get(f"{self.api_url}/{petition_id}/comments",
                                            params={"size":1000, "page":i},
                                            headers=self.headers,
                                            timeout=self.timeout)
                    response.raise_for_status()
                    break
                except requests.exceptions.Timeout:
                    continue
            
            comments.extend(response.json()["content"])
            if response.json()["last"]:
                break
        return comments

    def fetch_petition_list_page(self, size, page):
        """
        Получение страницы списка петиций из API

        Arguments:
        size -- размер одной страницы списка
        page -- номер страницы списка
        """
        while True:
            try:
                response = requests.get(f"{self.api_url}/short",
                                        params={"size":size, "page":page},
                                        headers=self.headers,
                                        timeout=self.timeout)
                response.raise_for_status()
                break
            except requests.exceptions.Timeout:
                continue
        return response.json()

    def save_to_csv(self, comments, csv_path):
        """
        Сохранение комментариев в CSV файл

        Arguments:
        comments -- list комментариев в формате json
        csv_path -- Путь к CSV файлу
        """
        if len(comments) == 0:
            return
        field_names = comments[0].keys()
        
        with open(csv_path, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=field_names)

            if os.path.getsize(csv_path) == 0:
                writer.writeheader()
            writer.writerows(comments)

    def run(self, csv_path):
        """
        Парсинг всех комментариев и сохранение в CSV файле
        
        Arguments:
        csv_path -- Путь к CSV файлу
        """
        silent_remove(csv_path)
        try:
            for i in itertools.count():
                petition_list_page = self.fetch_petition_list_page(self.max_page_size, i)

                for short_petition in petition_list_page["content"]:
                    comments = self.fetch_comments(short_petition["id"])
                    self.save_to_csv(comments, csv_path)

                if petition_list_page["last"]:
                    break
        except:
            silent_remove(csv_path)
            raise

if __name__ == "__main__":
    import argparse
    arg_parser = argparse.ArgumentParser(prog="comments parser")
    arg_parser.add_argument("-f", "--filename")
    args = arg_parser.parse_args()
    
    parser = CommentsParser()

    if args.filename:
        parser.run(f"{args.filename}.csv")
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parser.run(f"comments_{timestamp}.csv")


