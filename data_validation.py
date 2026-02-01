from api_server import ApiCollect
from data_process import DataProcess
import pandas as pd

URL = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/'

data_collect = ApiCollect(url=URL)
data_collect.download_file_zip()

data_proc = DataProcess()
df_relatorio = data_proc.chunking_func(file='downloads/Relatorio_cadop.csv')
df_conso = data_proc.chunking_func(file='consolidado_despesas.csv')
print('Verification Relatorio cadop\n')
print('The size of data frame Relatorio cadop: ')
print(df_relatorio.shape)

# start data verification
print('\nInformation of data frame: \n')
print(df_relatorio.info())

print(f'\nThere are NaN values? ')
print(df_relatorio.isna().sum())

print(f'\nThere are duplicated values? {df_relatorio.duplicated().sum()}\n')
# df_relatorio = df_relatorio.dropna()
# the most important data are correct and complete therefore I will keep the NaN values
print('Verification dataFrame Consolidado despesas \n')
print('The size of data frame Consolidado despesas: ')
print(df_conso.shape)

# start data verification
print('\nInformation of data frame: \n')
print(df_conso.info())

print(f'\nThere are NaN values? \n')
print(df_conso.isna().sum())

print(df_conso['REG_ANS'].duplicated().sum())
# ------------------------------- merger the data

df_final = df_conso.merge(df_relatorio, right_on=['REGISTRO_OPERADORA'], left_on=['REG_ANS'], how='right')

#
print(df_final.shape)
print(df_final.info())
print(df_final.isna().sum())

#-------------------- Clean duplicated data in the Dataframe
df_final = df_final.drop(df_final[df_final['CNPJ'].duplicated()].index)
print(df_final['VL_SALDO_INICIAL'].head(50))
print(df_final.shape)
df_final['VL_SALDO_INICIAL'] = df_final['VL_SALDO_INICIAL'].str.replace(',', '.').astype(float)
df_final['VL_SALDO_FINAL'] = df_final['VL_SALDO_FINAL'].str.replace(',', '.').astype(float)
print(df_final['VL_SALDO_INICIAL'].head(50))
# Creating new final dataframe with values correct

df_despesas = pd.DataFrame()
df_despesas['CNPJ'] = df_final['CNPJ']
df_despesas['RazaoSocial'] = df_final['Razao_Social']
df_despesas['Trimestre'] = df_final['DATA']
df_despesas['Ano']  = df_final['Data_Registro_ANS']
df_despesas['ValorDespesas']  = df_final['VL_SALDO_INICIAL'] - df_final['VL_SALDO_FINAL']
df_despesas['RegistroANS'] =  df_final['REG_ANS']
df_despesas['Modalidade'] =  df_final['Modalidade']
df_despesas['UF'] = df_final['UF']
print(df_despesas.head())
