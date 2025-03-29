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


class PetitionsParser:
    def __init__(self, api_url="https://epetition.kz/api/public/v1/petitions"):
        """
        Инициализация парсера петиций

        Arguments:
        api_url -- API url для получения петиций
        """
        self.api_url = api_url
        self.timeout = 1
        self.max_page_size = 10
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        }

    def remove_cover_file_from_petition(self, petition):
        """
        Удаление файла обложки из петиции
        
        Arguments:
        petition - петиция в формате json
        """
        if petition["cover"]:
            petition["cover"].pop("fileData", None)

    def fetch_petition(self, petition_id):
        """
        Получение петиции из API

        Arguments:
        petition_id -- id петиции
        """
        while True:
            try:
                response = requests.get(f"{self.api_url}/{petition_id}",
                                        headers=self.headers,
                                        timeout=self.timeout)
                response.raise_for_status()
                break
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError):
                continue
        return response.json()

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
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError):
                continue
        return response.json()

    def save_to_csv(self, petition, csv_path):
        """
        Сохранение петиции в CSV файл

        Arguments:
        petition -- петиция в формате json
        csv_path -- путь к CSV файлу
        """
        field_names = petition.keys()
        
        with open(csv_path, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            
            if os.path.getsize(csv_path) == 0:
                writer.writeheader()
            writer.writerow(petition)

    def run(self, csv_path):
        """
        Парсинг петиций и сохранение в CSV файле
        
        Arguments:
        csv_path -- путь к CSV файлу
        """
        try:
            temp_csv_path = f"{csv_path}.temp"
            
            for i in itertools.count():
                petition_list_page = self.fetch_petition_list_page(self.max_page_size, i)
                
                for short_petition in petition_list_page["content"]:
                    petition = self.fetch_petition(short_petition["id"])
                    self.remove_cover_file_from_petition(petition)
                    self.save_to_csv(petition, temp_csv_path)
                
                if petition_list_page["last"]:
                    break
        except:
            silent_remove(temp_csv_path)
            raise
        
        silent_remove(csv_path)
        os.rename(temp_csv_path, csv_path)


if __name__ == "__main__":
    parser = PetitionsParser()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    parser.run(f"petitions_{timestamp}.csv")

