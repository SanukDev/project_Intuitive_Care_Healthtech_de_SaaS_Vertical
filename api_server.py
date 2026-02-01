# To collect the api data
import requests
# To Create a repository
import os
# To collect file names
from bs4 import BeautifulSoup
# To extract .zip files
import zipfile

class ApiCollect:

    def __init__(self, url):
        self.url = url
        self.file_list = []
    # 1.1. Acesso à API de Dados Abertos da ANS
    # Function to download files
    def download_file_zip(self, folder="downloads"):
        url = self.url
        # Collecting the requests
        response = requests.get(url, stream=True)# 'stream=True': split the data to not fulling memory
        # Check likely error
        response.raise_for_status()

        # Scraping the data with BeautfulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        self.file_list = []
        # Collecting the file names by html file
        for a in soup.find_all('a'):
            if a["href"].endswith('.zip') or a["href"].endswith('.csv'):
                self.file_list.append(a.text)
        # Creating repository
        os.makedirs(folder, exist_ok=True)

        # For in file_list to navigate all file name
        for filename in self.file_list:
            r = requests.get(f'{url}/{filename}')
            # Open and downloading the file by file name in file_list
            with open(f"{folder}/{filename}", "wb") as f:
                f.write(r.content)
        # Return the files name in a list
        return self.file_list



