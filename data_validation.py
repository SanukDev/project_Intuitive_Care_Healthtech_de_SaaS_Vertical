from api_server import ApiCollect
from main import chunking_func, to_zip

URL = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/'

data_validation = ApiCollect(url=URL)
data_validation.download_file_zip()