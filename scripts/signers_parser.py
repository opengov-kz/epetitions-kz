import requests
import json
import csv
import itertools
from datetime import datetime

class SignersParser:
    def __init__(self, api_url="https://epetition.kz/api/public/v1/petitions"):
        """
        Инициализация парсера подписей

        Arguments:
        api_url -- API url для получения подписей
        """
        self.api_url = api_url
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        }

    def fetch_signers(self, petition_id):
        """
        Получение подписей одной петиции из API

        Arguments:
        petition_id -- id петиции

        INTERNAL_SERVER_ERROR:
        https://epetition.kz/api/public/v1/petitions/5e230abb-839e-4c35-ab68-83434354c8bf/signers?size=1&page=74
        """
        try:
            signers = []
            for i in itertools.count():
                response = requests.get(f"{self.api_url}/{petition_id}/signers",
                                        params={"size":100, "page":i},
                                        headers=self.headers, timeout=3)
                if response.status_code == 500:
                    for j in range(i * 100, (i + 1) * 100):
                        response = requests.get(f"{self.api_url}/{petition_id}/signers",
                                                params={"size":1, "page":j},
                                                headers=self.headers, timeout=3)
                        if response.status_code == 500:
                            continue
                        response.raise_for_status()
                        signers.extend(response.json()["content"])
                        if response.json()["last"]:
                            break
                    continue
                
                response.raise_for_status()
                signers.extend(response.json()["content"])
                if response.json()["last"]:
                    break

            for signer in signers:
                signer["petitionId"] = petition_id
                
            return signers
        except requests.exceptions.Timeout:
            return self.fetch_signers(petition_id)
        except:
            raise
        
    
    def fetch_all_signers(self):
        """
        Получение всех подписей из API
        """
        try:
            response = requests.get(f"{self.api_url}/short",
                                    params={"size":1},
                                    headers=self.headers, timeout=2)
            response.raise_for_status()
            
            response = requests.get(f"{self.api_url}/short",
                                    params={"size":response.json()["totalElements"]},
                                    headers=self.headers, timeout=2)
            response.raise_for_status()
            
            all_signers = []
            content = response.json()["content"]
            for i, petition in enumerate(content, start=1):
                print(f"Fetching signers from petition {i} of {len(content)}")
                all_signers.extend(self.fetch_signers(petition["id"]))
            
            return all_signers
        except requests.exceptions.Timeout:
            return self.fetch_all_signers()
        except:
            raise

    def save_to_csv(self, all_signers, csv_path):
        """
        Сохранение всех подписей в CSV файл

        Arguments:
        all_signers -- list всех подписей в формате json
        csv_path -- Путь к CSV файлу
        """
        try:
            field_names = all_signers[0].keys()
            with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(all_signers)
        except:
            raise

    def run(self, csv_path):
        """
        Парсинг всех подписей и сохранение в CSV файле
        
        Arguments:
        csv_path -- Путь к CSV файлу
        """
        try:
            all_signers = self.fetch_all_signers()
            self.save_to_csv(all_signers, csv_path)

            return all_signers
        except:
            raise

if __name__ == "__main__":
    try:    
        parser = SignersParser()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parser.run(f"signers_{timestamp}.csv")
    except:
        raise

