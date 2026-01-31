from api_server import ApiCollect
from pathlib import Path
import pandas as pd
from zipfile import PyZipFile

URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/"

data_api = ApiCollect(url=URL)
files_name = data_api.download_file_zip()

# files_name = ['1T2025.zip']
# The file names must be passed in a list
data_api.extract_files(files_name)

# This line allows read .csv .xlsx and .txt files with same structure
# df = pandas.read_csv('downloads/extracted/1T2025.csv',sep=None, engine='python') # the "sep=" allows to choice a separator between ';', ',' and others

site = Path('downloads/extracted/')

# ----------------------------- NO CHUNKING
# df_list = []
# for file in site.iterdir():
#     print(file)
#     df = pd.read_csv(f'{file}',sep=None, engine='python')
#
#     print(df.shape)
#     result = df[df['DESCRICAO'].str.contains("Despesas com")]
#     df_list.append(result)
#     print(result.shape)
#     print(result.head())


#-------------------------------- CHUNKING
chunks = []
dfs_list = []
# Trade-off tecnico
def chunking_func(file):
    """This function split the data process in 500 line by turn and return the data frame"""
    file = file
    for chunk in pd.read_csv(f'{file}', sep=None, engine='python', chunksize=500):
    # the "sep=" allows to choice a separator between ';', ',' and others, and engine='python', is bear that C language than python,
    # therefore this function allows other file extension like txt, cdv and xlsx
        chunks.append(chunk)
    df = pd.concat(chunks)
    return df


# 1.2. Processamento de Arquivos
for file in site.iterdir():
    print(f'Processing the file: {file}')
    df = chunking_func(file=file)
    print(f'Size of dataFrame: {df.shape}')
    # search data
    result = df[df['DESCRICAO'].str.contains("Despesas com")]
    print(f'size the dataFrame after search is: {result.shape}')
    dfs_list.append(result)


df_conso = pd.concat(dfs_list)

print(df_conso.shape)
print(df_conso['DESCRICAO'].head())
df_conso.to_csv('df_cons.csv')

with PyZipFile('df_cons.csv','r') as myzip:
    myzip.writepy('df_zip/')