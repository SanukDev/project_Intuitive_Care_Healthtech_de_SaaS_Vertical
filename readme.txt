📊 Pipeline de Análise de Despesas das Operadoras de Saúde – ANS


📌 Visão Geral

Este projeto implementa um pipeline completo de dados, desde a coleta até a exposição das informações, utilizando dados públicos disponibilizados pela ANS (Agência Nacional de Saúde Suplementar).
O objetivo principal do projeto é:
Automatizar o download de arquivos disponibilizados via web (API aberta / repositório público da ANS)
Realizar o processamento de arquivos CSV compactados (.zip)
Tratar grandes volumes de dados utilizando processamento em chunks
Consolidar e enriquecer dados financeiros e cadastrais das operadoras
Gerar automaticamente scripts SQL compatíveis com MySQL, incluindo:
Criação de tabelas
Inserção de dados
Expor os dados consolidados por meio de uma API REST, permitindo:
Consulta de operadoras
Consulta de despesas por operadora
Acesso a estatísticas agregadas
Disponibilizar uma interface web simples para visualização dos dados e gráficos
O projeto foi desenvolvido com foco em clareza, reprodutibilidade e escalabilidade, adotando boas práticas de engenharia de dados e desenvolvimento backend.


🏗️ Arquitetura Geral
O pipeline é dividido em quatro camadas principais, cada uma com responsabilidades bem definidas:

ApiCollect  →  DataProcess  →  SqlServer  →  API Flask + Frontend
(Coleta)       (Tratamento)     (Persistência)   (Exposição)


Cada camada segue o princípio de Single Responsibility, facilitando manutenção, testes e evolução do sistema.
📁 Estrutura do Projeto

project/
│
├── downloads/                  # Arquivos brutos baixados da ANS
│   └── extracted/              # Arquivos extraídos dos .zip
│
├── final_data/                 # Dados tratados e consolidados
│   ├── despesas_agregadas.csv
│   └── *.zip
│
├── despesas_agregadas.sql      # Script SQL gerado automaticamente
│
│
├── src/
│   ├── api_collect.py          # Classe ApiCollect (coleta de dados)
│   ├── data_process.py         # Classe DataProcess (tratamento de dados)
│   └── sql_server.py           # Classe SqlServer (geração de SQL)
│
├── server_api.py               # API Flask (exposição dos dados)
│
│
├── templates/
│   └── index.html              # Interface Web (Vue.js + Chart.js)
│
└── main.py



4. Classe ApiCollect
Responsabilidade
A classe ApiCollect é responsável pela coleta dos dados brutos diretamente da fonte pública da ANS.
Ela realiza:
Requisições HTTP
Leitura de conteúdo HTML
Identificação de arquivos disponíveis para download
Download automático dos arquivos
Inicialização
ApiCollect(url)
Parâmetros:
url (str): URL base onde os arquivos da ANS estão disponíveis
Método: download_file_zip()
download_file_zip(folder="downloads")
Descrição
Acessa a URL fornecida
Faz o scraping do HTML usando BeautifulSoup
Identifica links com extensão .zip ou .csv
Cria o diretório de destino, caso não exista
Realiza o download de todos os arquivos encontrados
Retorno
list[str]: Lista com os nomes dos arquivos baixados
Observações Técnicas
Utiliza stream=True para evitar sobrecarga de memória
O uso de web scraping permite adaptação caso novos arquivos sejam adicionados futuramente


5. Classe DataProcess
Responsabilidade
A classe DataProcess cuida da transformação e preparação dos dados, atuando como a camada de processamento do pipeline.
Método: chunking_func()
chunking_func(file)
Descrição
Lê arquivos CSV utilizando pandas em blocos de 500 linhas (chunking)
Detecta automaticamente o delimitador (;, ,, etc.)
Concatena os blocos em um único DataFrame
Motivação Técnica
O uso de chunking reduz o consumo de memória, tornando o pipeline mais robusto para arquivos grandes.
Método: extract_files()
extract_files(file_list, folder="downloads")
Descrição
Extrai arquivos .zip baixados
Armazena os arquivos extraídos em downloads/extracted
Parâmetros
file_list (list): Lista de arquivos compactados
folder (str): Diretório onde os arquivos estão localizados
Método: to_zip()
to_zip(file_name, name_zip="compacted", folder="")
Descrição
Compacta arquivos processados em um novo .zip
Útil para versionamento ou transporte de dados tratados


6. Classe SqlServer
Responsabilidade
A classe SqlServer é responsável por converter DataFrames em scripts SQL, sem depender de ORM ou conexão direta com o banco.
Essa abordagem facilita:
Portabilidade
Auditoria dos dados
Execução manual ou automatizada
Inicialização
SqlServer(file_name)
Parâmetros:
file_name (str): Caminho do arquivo .sql que será gerado
Método: infer_sql_type()
infer_sql_type(series)
Descrição
Analisa o tipo da coluna pandas
Converte automaticamente para tipos SQL compatíveis com MySQL
Tipo Pandas	Tipo SQL
string	VARCHAR(255)
int	INT
float	DECIMAL(15,2)
datetime	DATETIME
Método: create_table()
create_table(df_new, table_name)
Descrição
Normaliza os nomes das colunas
Remove caracteres especiais
Gera dinamicamente o comando CREATE TABLE
Chama automaticamente o método de inserção
Observações
Os nomes das colunas são convertidos para snake_case
Evita conflitos com caracteres inválidos em SQL
Método: insert_values()
insert_values(df, table_name, list_titles)
Descrição
Gera comandos INSERT INTO
Trata valores nulos, vazios e strings
Escreve cada linha do DataFrame como uma instrução SQL
Trade-off
Inserções linha a linha priorizam clareza e rastreabilidade
Para grandes volumes, recomenda-se bulk insert


7. Como Executar o Projeto
Pré-requisitos
Python 3.9+
Bibliotecas:
pip install requests pandas beautifulsoup4
Execução
Defina a URL da ANS no main.py
Execute o script principal:
python main.py
O script irá:
Baixar os arquivos
Extrair os dados
Processar os CSVs
Gerar o arquivo SQL final


8. Decisões Técnicas e Trade-offs
Não utilização de ORM para manter transparência do SQL
Geração de scripts ao invés de conexão direta com banco
Chunking para eficiência de memória
Tipagem automática baseada em inferência pandas


9. Possíveis Melhorias Futuras
Inserção em lote (bulk insert)
Validação de schema
Paralelização do processamento
Integração com Airflow
Dashboard analítico (Power BI / Tableau)


10. Conclusão
Este projeto demonstra um pipeline de dados completo, com separação clara de responsabilidades, foco em dados reais e preocupação com escalabilidade e legibilidade — características essenciais em projetos profissionais de engenharia e análise de dados.




Camada de API – Flask (Exposição dos Dados)
Visão Geral
Esta parte do projeto implementa uma API RESTful utilizando Flask, responsável por expor os dados tratados e consolidados nas etapas anteriores do pipeline ETL.
A API consome diretamente os arquivos .csv gerados no processamento (ETL) e disponibiliza endpoints para:
consulta de operadoras
detalhamento por CNPJ
despesas por operadora
estatísticas agregadas
Essa abordagem separa claramente:
Processamento de dados (ETL)
Exposição e consumo de dados (API)
Tecnologias Utilizadas
Flask: microframework para criação da API
Flask-CORS: habilita requisições cross-origin (frontend separado)
Pandas: leitura, filtragem, agregação e serialização dos dados
JSON: formato padrão de resposta da API
Inicialização da Aplicação
from flask import Flask, render_template, request, jsonify
from src.data_process import DataProcess
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
Justificativa Técnica
Flask(__name__): inicializa a aplicação web
CORS(app): permite que a API seja consumida por aplicações frontend (ex: Vue, React)
Separação clara entre backend de dados e frontend
Carregamento dos Dados
dataproc = DataProcess()
df_operadoras = dataproc.chunking_func("downloads/Relatorio_cadop.csv")
df_despesas = dataproc.chunking_func("final_data/despesas_agregadas.csv")
O que acontece aqui
Os dados já tratados no ETL são carregados
O método chunking_func evita alto consumo de memória
Os DataFrames permanecem em memória, garantindo respostas rápidas
Padronização de Tipos
df_operadoras["CNPJ"] = df_operadoras["CNPJ"].astype(str)
df_despesas["CNPJ"] = df_despesas["CNPJ"].astype(str)
Motivo
Evita inconsistências na comparação entre DataFrames
Garante que o CNPJ seja tratado como identificador textual
Previne erros silenciosos em filtros e joins
Rota Principal (Frontend)
@app.route("/")
def index():
    return render_template("index.html")
Finalidade
Serve a página inicial da aplicação
Pode ser integrada com um frontend (HTML, Vue, React)
Endpoints da API


1️⃣ GET /api/operadoras
@app.route("/GET/api/operadoras", methods=["GET"])
def listar_operadoras():
Descrição
Retorna uma lista paginada de operadoras de saúde.
Parâmetros de Query
page (int): página atual (default = 1)
limit (int): quantidade de registros por página (default = 10)
Funcionamento Interno
start = (page - 1) * limit
end = start + limit
data = df_operadoras.iloc[start:end]
Resposta JSON
{
  "page": 1,
  "limit": 10,
  "total": 1500,
  "data": [ ... ]
}
Justificativa
Implementa paginação manual
Evita retorno excessivo de dados
Boa prática para APIs reais


2️⃣ GET /api/operadoras/{cnpj}
@app.route("/GET/api/operadoras/<cnpj>", methods=["GET"])
def operadora_detalhe(cnpj):
Descrição
Retorna os dados cadastrais de uma operadora específica.
Funcionamento
operadora = df_operadoras[df_operadoras["CNPJ"] == cnpj]
Tratamento de Erro
if operadora.empty:
    return jsonify({"error": "Operadora não encontrada"}), 404
Resposta
JSON com os dados completos da operadora
HTTP 404 se não encontrada


3️⃣ GET /api/operadoras/{cnpj}/despesas
@app.route("/GET/api/operadoras/<cnpj>/despesas", methods=["GET"])
def despesas_operadora(cnpj):
Descrição
Retorna todas as despesas associadas a uma operadora.
Funcionamento
despesas = df_despesas[df_despesas["CNPJ"] == cnpj]
Resposta JSON
{
  "cnpj": "xxxxxxxxxxxxxx",
  "total_registros": 25,
  "data": [ ... ]
}
Valor Analítico
Permite análises financeiras por operadora
Endpoint fundamental para dashboards


4️⃣ GET /api/estatisticas
@app.route("/GET/api/estatisticas", methods=["GET"])
def estatisticas():
Descrição
Retorna estatísticas consolidadas do sistema.
Métricas Calculadas
Total de operadoras
total_operadoras = df_operadoras["CNPJ"].nunique()
Despesas por UF
df_despesas.groupby("UF")["ValorDespesas"].sum()
Despesas por Trimestre
df_despesas.groupby("Trimestre")["ValorDespesas"].sum()
Resposta JSON
{
  "total_operadoras": 1234,
  "despesas_por_uf": { "SP": 1000000, "RJ": 800000 },
  "despesas_por_trimestre": { "1T2025": 500000 }
}
Inicialização da Aplicação
if __name__ == '__main__':
    app.run(debug=True)
Camada Frontend – Interface Web (HTML + Vue.js)
Visão Geral
Este arquivo HTML implementa uma interface web simples e funcional para consumo da API Flask do projeto.
Ele permite visualizar operadoras, consultar despesas e exibir gráficos estatísticos, servindo como camada de apresentação dos dados processados no backend.
A solução utiliza:
HTML para estrutura
Vue.js 3 (CDN) para reatividade
Chart.js para visualização gráfica
Fetch API para comunicação com o backend Flask
Estrutura Geral da Página
<div id="app">
O elemento #app é o ponto de montagem da aplicação Vue, responsável por controlar todo o comportamento dinâmico da interface.
Recursos Externos
<script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js" defer></script>
Finalidade
Vue.js: gerenciamento de estado, eventos e renderização dinâmica
Chart.js: geração de gráficos estatísticos (despesas por UF)
Funcionalidades Implementadas


1️⃣ Listagem de Operadoras
Consome o endpoint:
GET /api/operadoras
Exibe:
Razão social
CNPJ
Botão para consulta de despesas
<li v-for="op in operadoras" :key="op.CNPJ">
Utiliza renderização reativa com v-for.


2️⃣ Consulta de Despesas por Operadora
Acionada ao clicar no botão Despesas
Consome o endpoint:
GET /api/operadoras/{cnpj}/despesas
<button @click="verDespesas(op.CNPJ)">
Exibe:
Trimestre
Valor das despesas formatado em reais


3️⃣ Gráfico de Despesas por UF
Gerado automaticamente ao carregar a página
Consome o endpoint:
GET /api/estatisticas
mounted() {
  this.carregarGraficoUF()
}
O gráfico:
Tipo: barra
Eixo X: UFs
Eixo Y: total de despesas
Aplicação Vue.js
createApp({
  delimiters: ['[[', ']]'],
Observação importante
Os delimitadores foram alterados para evitar conflito com o Jinja2 (Flask).
Estado da Aplicação (data())
data() {
  return {
    operadoras: [],
    despesas: [],
    page: 1,
    limit: 10,
    chart: null
  }
}
Controle de:
Dados carregados da API
Paginação
Instância do gráfico
Métodos Principais
carregarOperadoras()
Busca operadoras com paginação
Atualiza a lista exibida na tela
verDespesas(cnpj)
Busca despesas de uma operadora específica
Atualiza a lista de despesas
carregarGraficoUF()
Busca estatísticas agregadas
Renderiza gráfico com Chart.js
Garante atualização limpa destruindo gráficos anteriores
Integração com o Projeto
Esta camada:
Não processa dados
Atua exclusivamente como consumidora da API
Demonstra integração completa entre:
ETL (Pandas)
Backend (Flask)
Frontend (Vue.js + Chart.js)



▶️ Roteiro para Executar a Aplicação
1️⃣ Pré-requisitos
Certifique-se de ter instalado:
Python 3.9+
pip
Acesso à internet (para download dos dados da ANS)
2️⃣ Clonar o repositório
git clone <url-do-repositorio>
cd project
3️⃣ Criar e ativar ambiente virtual (opcional, recomendado)
python -m venv venv
source venv/bin/activate   # Linux / Mac
venv\Scripts\activate      # Windows
4️⃣ Instalar as dependências
pip install -r requirements.txt
(ou instalar manualmente: pandas, requests, beautifulsoup4, flask, flask-cors)
5️⃣ Executar o pipeline de dados (ETL)
python main.py
Essa etapa irá:
baixar os dados da ANS
processar e consolidar os arquivos
gerar os CSVs finais
criar o script SQL automaticamente
6️⃣ Executar a API Flask
python server_api.py
A API ficará disponível em:
http://127.0.0.1:5000
7️⃣ Acessar a aplicação web
Abra o navegador e acesse:
http://127.0.0.1:5000/
A interface irá:
listar operadoras
permitir consulta de despesas
exibir gráficos estatísticos por UF