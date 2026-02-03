import pandas as pd
from zipfile import ZipFile

class DataProcess:

    # Trade-off tecnico
    def chunking_func(self,file):
        """This function split the data process in 500 line by turn and return the data frame"""
        chunks = []
        file = file
        for chunk in pd.read_csv(f'{file}', sep=None, engine='python', chunksize=500):
            # the "sep=" allows to choice a separator between ';', ',' and others, and engine='python', is bear that C language than python,
            # therefore this function allows other file extension like txt, cdv and xlsx
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
        return df

    def to_zip(self,file_name, name_zip='compacted', folder=''):
        with ZipFile(f'{folder}/{name_zip}.zip', 'w') as my_zip:
            my_zip.write(f'{file_name}')

    # 1.2. Processamento de Arquivos
    # Function to extract files
    def extract_files(self, file_list, folder='downloads'):
        """This function extract the files and put in folder called extracted. Receive variable like file_list = [] type
         list receive a list with names of files, folder='download' type str that contain name of folder where the file is"""
        file_list = file_list
        for filename in file_list:
            # Extract all files
            with ZipFile(f"{folder}/{filename}") as myzip:
                myzip.extractall(f"{folder}/extracted")