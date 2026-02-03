from flask import Flask, render_template, request, jsonify
from src.data_process import DataProcess
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ------------------ Load data (CSV gerados no Teste 3)
dataproc = DataProcess()
df_operadoras = dataproc.chunking_func("downloads/Relatorio_cadop.csv")
df_despesas = dataproc.chunking_func("final_data/despesas_agregadas.csv")

# Solvin data
df_operadoras["CNPJ"] = df_operadoras["CNPJ"].astype(str)
df_despesas["CNPJ"] = df_despesas["CNPJ"].astype(str)

@app.route("/")
def index():
    return render_template("index.html")


# 4.2.1 GET /api/operadoras
@app.route("/GET/api/operadoras", methods=["GET"])
def listar_operadoras():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))

    start = (page - 1) * limit
    end = start + limit

    data = df_operadoras.iloc[start:end]
    #data json
    return jsonify({
        "page": page,
        "limit": limit,
        "total": len(df_operadoras),
        "data": data.to_dict(orient="records")
    })

# 4.2.2 GET /api/operadoras/{cnpj}
@app.route("/GET/api/operadoras/<cnpj>", methods=["GET"])
def operadora_detalhe(cnpj):
    operadora = df_operadoras[df_operadoras["CNPJ"] == cnpj]

    if operadora.empty:
        return jsonify({"error": "Operadora não encontrada"}), 404
    #data json
    return jsonify(operadora.iloc[0].to_dict())

# 4.2.3 GET /api/operadoras/{cnpj}/despesas
@app.route("/GET/api/operadoras/<cnpj>/despesas", methods=["GET"])
def despesas_operadora(cnpj):
    despesas = df_despesas[df_despesas["CNPJ"] == cnpj]

    #data json
    return jsonify({
        "cnpj": cnpj,
        "total_registros": len(despesas),
        "data": despesas.to_dict(orient="records")
    })


# 4.2.4 GET /api/estatisticas
@app.route("/GET/api/estatisticas", methods=["GET"])
def estatisticas():
    total_operadoras = df_operadoras["CNPJ"].nunique()

    despesas_por_uf = (df_despesas.groupby("UF")["ValorDespesas"].sum().sort_values(ascending=False).to_dict())

    despesas_por_trimestre = (df_despesas.groupby("Trimestre")["ValorDespesas"].sum().to_dict())

    # data .json
    return jsonify({
        "total_operadoras": total_operadoras,
        "despesas_por_uf": despesas_por_uf,
        "despesas_por_trimestre": despesas_por_trimestre
    })

#start app
if __name__ == '__main__':
    app.run(debug=True)