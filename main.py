from api_server import ApiCollect
from pathlib import Path
import pandas as pd
from data_process import DataProcess

URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/"

data_api = ApiCollect(url=URL)
files_name = data_api.download_file_zip()

# files_name = ['1T2025.zip']
# The file names must be passed in a list
data_api.extract_files(files_name)

# Creating DataProcess object
data_proc = DataProcess()
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
dfs_list = []
file_end_name = "consolidado_despesas.csv"
df_consolidado = []
# To try open the file consolidado_despesas.csv, but case not exist jump it to the routine to create it
try:
    df_consolidado = data_proc.chunking_func(file=file_end_name)

except:
    # 1.2. Processamento de Arquivos
    for file in site.iterdir():
        print(f'Processing the file: {file}\n')
        df = data_proc.chunking_func(file=file)
        print(f'Size of dataFrame: {df.shape}\n')
        # search data
        result = df[df['DESCRICAO'].str.contains("Despesas com")]
        print(f'size the dataFrame after search is: {result.shape}\n')

        #Checking if there are NaN values
        print('There is bad rows in dataFrame?: ')
        print(result.isna().sum())

        # Checking if there are duplicated values
        print(f'\nThere are duplicated values?: {result.duplicated().sum()}\n')
        # If there are bad data, the '.dropna()' method will delete it
        result = result.dropna()

        # Printing information of dataFrame with 'info()' method
        print('\nInformation of data: ')
        print(result.info())

        dfs_list.append(result)
    df_consolidado = pd.concat(dfs_list)
    # ------------------------- Checking the final data


print(df_consolidado.shape)
df_consolidado['DATA'] = pd.to_datetime(df_consolidado['DATA'])
print(df_consolidado['DESCRICAO'].head())
print('\n')
print(df_consolidado['CD_CONTA_CONTABIL'].duplicated().sum())

print(df_consolidado.describe())
print(df_consolidado.info())
try:
    df_consolidado.to_csv(file_end_name, mode='x' )
    data_proc.to_zip(file_name=file_end_name, name_zip='consolidado_despesas')
except FileExistsError:
   print("\nThe file already exist...\n")
