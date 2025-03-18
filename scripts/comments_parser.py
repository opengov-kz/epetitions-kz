import requests
import json
import csv
import itertools
from datetime import datetime

class CommentsParser:
    def __init__(self, api_url="https://epetition.kz/api/public/v1/petitions/"):
        """
        Инициализация парсера комментариев

        Arguments:
        api_url -- API url для получения комментариев
        """
        self.api_url = api_url
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
        try:
            comments = []
            for i in itertools.count():
                response = requests.get(f"{self.api_url}{petition_id}/comments",
                                        params={"size":1000, "page":i},
                                        headers=self.headers, timeout=3)
                response.raise_for_status()
                comments.extend(response.json()["content"])
                if response.json()["last"]:
                    break
            
            return comments
        except requests.exceptions.Timeout:
            return self.fetch_comments(petition_id)
        except:
            raise
        
    
    def fetch_all_comments(self):
        """
        Получение всех комментариев из API
        """
        try:
            response = requests.get(self.api_url+"short", params={"size":1}, headers=self.headers, timeout=2)
            response.raise_for_status()
            
            totalElements = response.json()["totalElements"]
            response = requests.get(self.api_url+"short", params={"size":totalElements}, headers=self.headers, timeout=2)
            response.raise_for_status()

            all_comments = []
            content = response.json()["content"]
            for idx, petition in enumerate(content, start=1):
                print(f"Fetching comments from petition {idx} of {len(content)}")
                all_comments.extend(self.fetch_comments(petition["id"]))
            
            return all_comments
        except requests.exceptions.Timeout:
            return self.fetch_all_comments()
        except:
            raise

    def save_to_csv(self, all_comments, csv_path):
        """
        Сохранение всех комментариев в CSV файл

        Arguments:
        all_comments -- list всех комментариев в формате json
        csv_path -- Путь к CSV файлу
        """
        try:
            field_names = all_comments[0].keys()
            with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(all_comments)
        except:
            raise

    def run(self, csv_path):
        """
        Парсинг всех комментариев и сохранение в CSV файле
        
        Arguments:
        csv_path -- Путь к CSV файлу
        """
        try:
            all_comments = self.fetch_all_comments()
            self.save_to_csv(all_comments, csv_path)

            return all_comments
        except:
            raise

if __name__ == "__main__":
    try:
        parser = CommentsParser()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parser.run(f"comments_{timestamp}.csv")
    except:
        raise


