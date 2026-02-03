from flask import Flask, render_template, request, jsonify
from data_process import DataProcess
from flask_cors import CORS
import pandas as pd

app = Flask(__name__)
CORS(app)

# ------------------ Load data (CSV gerados no Teste 3)
dataproc = DataProcess()
df_operadoras = dataproc.chunking_func("downloads/Relatorio_cadop.csv")
df_despesas = dataproc.chunking_func("final_data/despesas_agregadas.csv")

# Garantir tipo correto
df_operadoras["CNPJ"] = df_operadoras["CNPJ"].astype(str)
df_despesas["CNPJ"] = df_despesas["CNPJ"].astype(str)

@app.route('/')
def home():
    return render_template('index.html')

@app.route("/api/operadoras", methods=["GET"])
def listar_operadoras():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))

    start = (page - 1) * limit
    end = start + limit

    data = df_operadoras.iloc[start:end]

    return jsonify({
        "page": page,
        "limit": limit,
        "total": len(df_operadoras),
        "data": data.to_dict(orient="records")
    })

# ====================================================
@app.route("/api/operadoras/<cnpj>", methods=["GET"])
def operadora_detalhe(cnpj):
    operadora = df_operadoras[df_operadoras["CNPJ"] == cnpj]

    if operadora.empty:
        return jsonify({"error": "Operadora não encontrada"}), 404

    return jsonify(operadora.iloc[0].to_dict())

@app.route("/api/operadoras/<cnpj>/despesas", methods=["GET"])
def despesas_operadora(cnpj):
    despesas = df_despesas[df_despesas["CNPJ"] == cnpj]

    return jsonify({
        "cnpj": cnpj,
        "total_registros": len(despesas),
        "data": despesas.to_dict(orient="records")
    })


@app.route("/api/estatisticas", methods=["GET"])
def estatisticas():
    total_operadoras = df_operadoras["CNPJ"].nunique()

    despesas_por_uf = (
        df_despesas.groupby("UF")["ValorDespesas"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )

    despesas_por_trimestre = (
        df_despesas.groupby("Trimestre")["ValorDespesas"]
        .sum()
        .to_dict()
    )

    return jsonify({
        "total_operadoras": total_operadoras,
        "despesas_por_uf": despesas_por_uf,
        "despesas_por_trimestre": despesas_por_trimestre
    })
if __name__ == '__main__':
    app.run(debug=True)