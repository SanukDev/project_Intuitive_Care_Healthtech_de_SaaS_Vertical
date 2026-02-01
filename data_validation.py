from api_server import ApiCollect
from data_process import DataProcess
import pandas as pd

URL = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/'

data_collect = ApiCollect(url=URL)
data_collect.download_file_zip()

data_proc = DataProcess()
df_relatorio = data_proc.chunking_func(file='downloads/Relatorio_cadop.csv')
df_conso = data_proc.chunking_func(file='consolidado_despesas.csv')
print('The size of data frame: ')
print(df_relatorio.shape)

# start data verification
print('\nInformation of data frame: \n')
print(df_relatorio.info())

print(f'There are NaN values? ')
print(df_relatorio.isna().sum())

print(f'\nThere are duplicated values? {df_relatorio.duplicated().sum()}\n')
# df_relatorio = df_relatorio.dropna()
# the most important data are correct and complete therefore I will keep the NaN values

# ------------------------------- merger the data

df_final = df_relatorio.merge(df_conso, left_on=['REGISTRO_OPERADORA'], right_on=['REG_ANS'], how='left')

#
print(df_final.shape)
print(df_final.info())
print(df_final.isna().sum())

#-------------------- Clean duplicated data in the Dataframe
df_final = df_final.drop(df_final[df_final['CNPJ'].duplicated()].index)
print(df_final['Razao_Social'].tail(50))
print(df_final.shape)
