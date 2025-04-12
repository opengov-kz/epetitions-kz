import requests
import json
import csv
from datetime import datetime
from datetime import timedelta
import os
import itertools
import errno

def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


class SignersParser:
    def __init__(self, api_url="https://epetition.kz/api/public/v1/petitions"):
        """
        Инициализация парсера подписей

        Arguments:
        api_url -- API url для получения подписей
        """
        self.api_url = api_url
        self.timeout = 1
        self.max_page_size = 1
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        }


    def get_new_signers_from_list(self, signers):
        """
        Получение новых подписей из списка

        Arguments:
        signers -- list подписей
        """
        left, right = 0, len(signers)
        while left < right:
            mid = left + (right - left) // 2
            created_date = datetime.fromisoformat(signers[mid]["createdDate"][:19])
            if created_date > self.last_parsing_datetime:
                left = mid + 1
            else:
                right = mid
        first_old_signer = left
        
        for i in itertools.count():
            if i == len(signers):
                first_new_signer = i
                break
            created_date = datetime.fromisoformat(signers[i]["createdDate"][:19])
            if created_date <= self.new_last_parsing_datetime:
                first_new_signer = i
                break
        
        return {"content":signers[first_new_signer:first_old_signer],
                "last": False if first_old_signer == len(signers) else True}


    def fetch_new_signers(self, petition_id):
        """
        Получение новых подписей одной петиции из API

        Arguments:
        petition_id -- id петиции

        INTERNAL_SERVER_ERROR:
        https://epetition.kz/api/public/v1/petitions/5e230abb-839e-4c35-ab68-83434354c8bf/signers?size=1&page=74
        """
        all_signers = []
        for i in itertools.count():
            while True:
                try:
                    response = requests.get(f"{self.api_url}/{petition_id}/signers",
                                            params={"size":100, "page":i},
                                            headers=self.headers, timeout=3)
                    if (response.status_code == 500): break
                    response.raise_for_status()
                    break
                except (requests.exceptions.Timeout,
                        requests.exceptions.ConnectionError):
                    continue
            
            if response.status_code == 200:
                signers = self.get_new_signers_from_list(response.json()["content"])
                all_signers.extend(signers["content"])
                if signers["last"]:
                    break
            elif response.status_code == 500:
                for j in range(i * 100, (i + 1) * 100):
                    while True:
                        try:
                            response = requests.get(f"{self.api_url}/{petition_id}/signers",
                                                    params={"size":1, "page":j},
                                                    headers=self.headers, timeout=3)
                            if (response.status_code == 500): break
                            response.raise_for_status()
                            break
                        except (requests.exceptions.Timeout,
                                requests.exceptions.ConnectionError):
                            continue
                        
                    if response.status_code == 200:
                        created_date = datetime.fromisoformat(response.json()["content"][0]["createdDate"][:19])
                        if created_date > self.new_last_parsing_datetime:
                            continue
                        if created_date <= self.last_parsing_datetime:
                            break
                        all_signers.extend(response.json()["content"])
                        if response.json()["last"]: break

            created_date = datetime.fromisoformat(response.json()["content"][0]["createdDate"][:19])
            if response.json()["last"] or created_date <= self.last_parsing_datetime:
                break
            
        for signer in all_signers:
            signer["petitionId"] = petition_id
        
        return all_signers


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


    def save_to_csv(self, signers, csv_path):
        """
        Сохранение подписей в CSV файл

        Arguments:
        signers -- list подписей в формате json
        csv_path -- Путь к CSV файлу
        """
        if len(signers) == 0:
            return
        field_names = signers[0].keys()
        
        with open(csv_path, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=field_names)

            if os.path.getsize(csv_path) == 0:
                writer.writeheader()
            writer.writerows(signers)


    def run(self, csv_path):
        """
        Парсинг новых подписей, либо всех при первом запуске, и сохранение в CSV файле
        
        Arguments:
        csv_path -- Путь к CSV файлу
        """
        try:
            with open("last_parsing_of_signers.txt", "r") as f:
                self.last_parsing_datetime = datetime.fromisoformat(f.read())
        except:
            self.last_parsing_datetime = datetime.fromisoformat("1900-01-01T00:00:00")   
        self.new_last_parsing_datetime = datetime.now().replace(microsecond=0) - timedelta(seconds=1)
        
        silent_remove(csv_path)
        try:
            for i in itertools.count():
                petition_list_page = self.fetch_petition_list_page(self.max_page_size, i)

                for short_petition in petition_list_page["content"]:
                    signers = self.fetch_new_signers(short_petition["id"])
                    self.save_to_csv(signers, csv_path)

                if petition_list_page["last"]:
                    break
                
            with open("last_parsing_of_signers.txt", "w") as f:
                f.write(self.new_last_parsing_datetime.isoformat())
        except:
            silent_remove(csv_path)
            raise


if __name__ == "__main__":
    import argparse
    arg_parser = argparse.ArgumentParser(prog="signers parser")
    arg_parser.add_argument("-f", "--filename")
    args = arg_parser.parse_args()
    
    parser = SignersParser()

    if args.filename:
        parser.run(f"{args.filename}.csv")
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parser.run(f"signers_{timestamp}.csv")

