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
        df = pd.concat(chunks)
        return df

    def to_zip(self,file_name, name_zip='compacted'):
        with ZipFile(f'{name_zip}.zip', 'w') as my_zip:
            my_zip.write(f'{file_name}')

