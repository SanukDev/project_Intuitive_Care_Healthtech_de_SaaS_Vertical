from api_server import ApiCollect
from pathlib import Path
import pandas

URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/"


data_api = ApiCollect(url=URL)
data_api.download_file()

# This line allows read .csv .xlsx and .txt files with same structure
# df = pandas.read_csv('downloads/extracted/1T2025.csv',sep=None, engine='python') # the "sep=" allows to choice a separator between ';', ',' and others

site = Path('downloads/extracted/')

df_list = []
for file in site.iterdir():
    print(file)
    df = pandas.read_csv(f'{file}',sep=None, engine='python')

    print(df.shape)
    result = df[df['DESCRICAO'].str.contains("Despesas com")]
    df_list.append(result)
    print(result.shape)
    print(result.head())

merged_df = df_list[0]

merged_df.merge(df_list[1])

print(merged_df.shape)