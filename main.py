from api_server import ApiCollect
import pandas

URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/"


data_api = ApiCollect(url=URL)
data_api.download_file()
