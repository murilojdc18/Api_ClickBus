import os
import time
import pandas as pd
from flask import Flask, request, jsonify

app = Flask(__name__)

API_KEY = os.environ.get("API_KEY", "seutoken123")
DELAY = float(os.environ.get("DELAY", "0"))
CSV_URL = os.environ.get("CSV_URL")        # ex: link direto (Drive/Dropbox/S3)
CSV_PATH = os.environ.get("CSV_PATH", "")  # ex: "df_t_pequeno.csv" no repo

_df_cache = None

def load_df():
    """Carrega o DataFrame uma única vez (lazy-load)."""
    global _df_cache
    if _df_cache is not None:
        return _df_cache

    src = CSV_URL or CSV_PATH or "df_t_pequeno.csv"  # fallback
    try:
        app.logger.info(f"Carregando dados de: {src}")
        _df_cache = pd.read_csv(src)
        # ajuste de datas (seguro)
        if "date_purchase" in _df_cache.columns:
            _df_cache["date_purchase"] = pd.to_datetime(
                _df_cache["date_purchase"], errors="coerce"
            ).astype(str)
    except Exception as e:
        app.logger.error(f"Falha ao carregar dados: {e}")
        _df_cache = pd.DataFrame()  # evita crash
    return _df_cache

@app.route("/")
def home():
    return jsonify({"status": "ok", "msg": "API Fake ClickBus rodando"})

@app.route("/dados", methods=["GET"])
def get_dados():
    # auth simples
    token = request.headers.get("x-api-key")
    if token != API_KEY:
        return jsonify({"erro": "Acesso não autorizado"}), 401

    if DELAY:
        time.sleep(DELAY)  # latência fake

    df = load_df()

    # filtros GET ?cliente=...&data=YYYY-MM
    cliente = request.args.get("cliente")
    data = request.args.get("data")

    out = df
    if cliente and "fk_contact" in out.columns:
        out = out[out["fk_contact"].astype(str) == str(cliente)]
    if data and "date_purchase" in out.columns:
        out = out[out["date_purchase"].str.startswith(data)]

    return jsonify(out.to_dict(orient="records"))

if __name__ == "__main__":
    # Render precisa escutar em 0.0.0.0 e na porta do ENV PORT
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
