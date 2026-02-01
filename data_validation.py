from api_server import ApiCollect
from data_process import DataProcess
import pandas as pd

URL = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/'

data_collect = ApiCollect(url=URL)
data_collect.download_file_zip()

data_proc = DataProcess()
df_relatorio = data_proc.chunking_func(file='downloads/Relatorio_cadop.csv')

print(df_relatorio.shape)

