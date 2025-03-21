import requests
import json
import csv
from datetime import datetime

class PetitionsParser:
    def __init__(self, api_url="https://epetition.kz/api/public/v1/petitions"):
        """
        Инициализация парсера петиций

        Arguments:
        api_url -- API url для получения петиций
        """
        self.api_url = api_url
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        }

    def fetch_petition(self, petition_id):
        """
        Получение петиции из API

        Arguments:
        petition_id -- id петиции
        """
        while True:
            try:
                response = requests.get(f"{self.api_url}/{petition_id}", headers=self.headers, timeout=1)
                if response.status_code == 200: break
                else: raise
                
            except requests.exceptions.Timeout:
                continue
        return response.json()
        
    
    def fetch_petitions(self):
        """
        Получение петиций из API
        """
        while True:
            try:
                response = requests.get(f"{self.api_url}/short",
                                        params={"size":1},
                                        headers=self.headers, timeout=2)
                if response.status_code != 200: raise
                
                response = requests.get(f"{self.api_url}/short",
                                        params={"size":response.json()["totalElements"]},
                                        headers=self.headers, timeout=2)
                response.raise_for_status()
                if response.status_code == 200: break
                else: raise

            except requests.exceptions.Timeout:
                continue
            
        petitions = []
        content = response.json()["content"]
        for i, petition in enumerate(content, start=1):
            print(f"Fetching petition {i} of {len(content)}")
            petitions.append(self.fetch_petition(petition["id"]))
        
        return petitions

    def save_to_csv(self, petitions, csv_path):
        """
        Сохранение петиций в CSV файл

        Arguments:
        petitions -- list петиций в формате json
        csv_path -- Путь к CSV файлу
        """
        field_names = petitions[0].keys()
        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(petitions)

    def run(self, csv_path):
        """
        Парсинг петиций и сохранение в CSV файле
        
        Arguments:
        csv_path -- Путь к CSV файлу
        """
        petitions = self.fetch_petitions()
        self.save_to_csv(petitions, csv_path)
        return petitions


if __name__ == "__main__":
    parser = PetitionsParser()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    parser.run(f"petitions_{timestamp}.csv")


