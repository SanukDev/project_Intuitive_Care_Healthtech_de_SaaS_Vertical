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
            if a["href"].endswith('.zip'):
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

    # 1.2. Processamento de Arquivos
    # Function to extract files
    def extract_files(self, file_list, folder='downloads'):
        """This function extract the files and put in folder called extracted. Receive variable like file_list = [] type
         list receive a list with names of files, folder='download' type str that contain name of folder where the file is"""
        file_list = file_list
        for filename in file_list:
            # Extract all files
            with zipfile.ZipFile(f"{folder}/{filename}") as myzip:
                myzip.extractall(f"{folder}/extracted")

