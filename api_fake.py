from flask import Flask, request, jsonify
import pandas as pd
import time

app = Flask(__name__)

# Configurações básicas
CSV_PATH = r"C:\Users\Win10\Documents\Fiap\ClickBus\df_t.csv"  # nome do seu arquivo CSV
API_KEY = "seutoken123"  # token de acesso

# Carrega os dados uma vez ao iniciar
df = pd.read_csv(CSV_PATH)
df["date_purchase"] = pd.to_datetime(df["date_purchase"], errors="coerce").astype(str)

@app.route("/dados", methods=["GET"])
def get_dados():
    token = request.headers.get("x-api-key")
    if token != API_KEY:
        return jsonify({"erro": "Acesso não autorizado"}), 401

    # Simula latência
    time.sleep(2)

    cliente = request.args.get("cliente")
    data = request.args.get("data")

    dados_filtrados = df.copy()

    if cliente:
        dados_filtrados = dados_filtrados[dados_filtrados["fk_contact"] == cliente]

    if data:
        dados_filtrados = dados_filtrados[dados_filtrados["date_purchase"].str.startswith(data)]

    return jsonify(dados_filtrados.to_dict(orient="records"))

@app.route("/")
def home():
    return "<h1>API Fake ClickBus Rodando!</h1>"

if __name__ == "__main__":
    app.run(debug=True)