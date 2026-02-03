from api_server import ApiCollect
from pathlib import Path
import pandas as pd
import os
from data_process import DataProcess


URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/"

data_collect = ApiCollect(url=URL)
files_name = data_collect.download_file_zip()

# Creating DataProcess object
data_proc = DataProcess()
# files_name = ['1T2025.zip']
# The file names must be passed in a list
data_proc.extract_files(files_name)


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



dfs_list = []
file_end_name = "consolidado_despesas.csv"
df_consolidado = []
folder_final_data = 'final_data'
# To try open the file consolidado_despesas.csv, but case not exist jump it to the routine to create it
try:
    df_consolidado = data_proc.chunking_func(file=f'{folder_final_data}/{file_end_name}')

except:
    # 1.2. Processamento de Arquivos
    trimestre =  1
    for file in site.iterdir():
        print(f'Processing the file: {file}...\n')
        # -------------------------------- CHUNKING-----------------------------------
        chunks = []
        for chunk in pd.read_csv(f'{file}', sep=None, engine='python', chunksize=500):
            # the "sep=" allows to choice a separator between ';', ',' and others, engine='python' allows automatic delimiter detection, but is slower than the C engine,
            # therefore this function allows other file extension like txt, csv and xlsx
            chunks.append(chunk[chunk['DESCRICAO'].str.contains("Despesas com Eventos/Sinistros")])
        result = pd.concat(chunks)

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
df_consolidado['VL_SALDO_INICIAL'] = df_consolidado['VL_SALDO_INICIAL'].astype(str).str.replace(',', '.').astype(float)
df_consolidado['VL_SALDO_FINAL'] = df_consolidado['VL_SALDO_FINAL'].astype(str).str.replace(',', '.').astype(float)
print(df_consolidado['DESCRICAO'].head())
print('\n')
print(df_consolidado['CD_CONTA_CONTABIL'].duplicated().sum())


print(df_consolidado.describe())
print(df_consolidado.info())
try:
    os.makedirs(folder_final_data, exist_ok=True)
    df_consolidado.to_csv(f'{folder_final_data}/{file_end_name}', mode='x' )
    data_proc.to_zip(file_name=f'{folder_final_data}/{file_end_name}', name_zip='consolidado_despesas', folder=folder_final_data)
except FileExistsError:
   print("\nThe file already exist...\n")





# ------------------------------------------------------------------------- TESTE 2


data_collect_t2 = ApiCollect(url='https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/')
try:
    df_relatorio = data_proc.chunking_func(file='downloads/Relatorio_cadop.csv')
except:
    data_collect_t2.download_file_zip()
    df_relatorio = data_proc.chunking_func(file='downloads/Relatorio_cadop.csv')

df_conso = data_proc.chunking_func(file=f'{folder_final_data}/consolidado_despesas.csv')

#-------------------------- start data verification df_relatorio
print('Verification Relatorio cadop\n')
print('The size of data frame Relatorio cadop: ')
print(df_relatorio.shape)

# start data verification
print('\nInformation of data frame: df_relatorio\n')
print(df_relatorio.info())

print(f'\nThere are NaN values? ')
print(df_relatorio.isna().sum())

print(f'\nThere are duplicated values? {df_relatorio.duplicated().sum()}\n')
# df_relatorio = df_relatorio.dropna()
# the most important data are correct and complete therefore I will keep the NaN values
#-------------------------resolving data df_relatorio
df_relatorio['CEP'] = df_relatorio['CEP'].to_string()
df_relatorio['DDD'] = df_relatorio['DDD'].to_string()
df_relatorio['Telefone'] = df_relatorio['Telefone'].to_string()
df_relatorio['Fax'] = df_relatorio['Fax'].to_string()
df_relatorio['CNPJ'] = df_relatorio['CNPJ'].to_string()
df_relatorio['Regiao_de_Comercializacao'] = df_relatorio['Regiao_de_Comercializacao'].to_numpy(int)
df_relatorio['Data_Registro_ANS'] = pd.to_datetime(df_relatorio['Data_Registro_ANS'])
print('\nInformation of data frame: df_relatorio after solving data\n')
print(df_relatorio.info())

# ----------------------start data verification df_conso

print('Verification dataFrame Consolidado despesas \n')
print('The size of data frame Consolidado despesas: ')
print(df_conso.shape)


print('\nInformation of data frame: df_conso\n')
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
#df_final = df_final.drop(df_final[df_final['CNPJ'].duplicated()].index)
print("\n\nvalores duplicados")
print(df_final[['DATA','CNPJ', 'Razao_Social','VL_SALDO_INICIAL', 'VL_SALDO_FINAL','Modalidade','Nome_Fantasia','Representante']].duplicated().value_counts())
print(df_final.shape)
df_final = df_final.drop(df_final[df_final[['DATA','CNPJ', 'Razao_Social','VL_SALDO_INICIAL', 'VL_SALDO_FINAL','Modalidade','Nome_Fantasia','Representante']].duplicated()].index)
print('\n\nShape after drop duplicated values')
print(df_final.shape)

# -------------
df_final['VL_SALDO_INICIAL'] = df_final['VL_SALDO_INICIAL'].str.replace(',', '.').astype(float)
df_final['VL_SALDO_FINAL'] = df_final['VL_SALDO_FINAL'].str.replace(',', '.').astype(float)
print(df_final['VL_SALDO_INICIAL'].head(50))


# Creating new final dataframe with values correct

df_despesas = pd.DataFrame()
df_despesas['CNPJ'] = str(df_final['CNPJ'])
df_despesas['RazaoSocial'] = df_final['Razao_Social']
df_despesas['Trimestre'] = pd.to_datetime(df_final['DATA'])
df_despesas['Ano'] = pd.to_datetime(df_final['Data_Registro_ANS'])
df_despesas['ValorDespesas'] = df_final['VL_SALDO_FINAL'] - df_final['VL_SALDO_INICIAL']
df_despesas['RegistroANS'] = df_final['REG_ANS'].to_numpy(int)
df_despesas['Modalidade'] = df_final['Modalidade']
df_despesas['UF'] = df_final['UF']
print(df_despesas[['RazaoSocial', 'ValorDespesas']].head())
# Info despesas
print("\n\nData information df_despesas ")
print(df_despesas.info())

# Sort values by 'RazaoSocial' and 'UF
df_despesas.sort_values(['RazaoSocial', 'UF'], ascending=True, inplace=True)
# print(df_despesas.groupby(['RazaoSocial','UF'], as_index=False))
print(df_despesas[['RazaoSocial', 'UF']].head(50))

# Creating file -------------- DESPESAS AGREGADAS
df_despesas.to_csv(f'{folder_final_data}/despesas_agregadas.csv')
data_proc.to_zip(file_name=f'{folder_final_data}/despesas_agregadas.csv',name_zip='Teste_Samuel_Melo',folder=folder_final_data)


#----------------------------------Calculating values

#--------------------------
# Calc ValorDespesas by UF
df_valor_by_uf = df_despesas[['ValorDespesas','UF']].groupby('UF').sum(numeric_only=True)

print('\n\nValues by UF: ')
print(df_valor_by_uf.sort_values('ValorDespesas', ascending=False))
df_valor_by_uf = df_valor_by_uf.sort_values('ValorDespesas', ascending=False)
#Creating the final files
df_valor_by_uf.to_csv(f'{folder_final_data}/valor_by_uf.csv')

#------------------------
# Calc ValorDespesas by Trimestre
df_valor_by_trimestre = df_despesas[['ValorDespesas','Trimestre']].groupby('Trimestre').sum()

print('\n\nValues by Trimestre: ')
print(df_valor_by_trimestre.sort_values('ValorDespesas', ascending=False))
df_valor_by_trimestre = df_valor_by_trimestre.sort_values('ValorDespesas', ascending=False)
#Creating the final files
df_valor_by_trimestre.to_csv(f'{folder_final_data}/valor_by_trimestre.csv')


