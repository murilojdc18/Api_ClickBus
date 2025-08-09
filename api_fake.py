import os
import time
import pandas as pd
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# ===== Config =====
API_KEY = os.environ.get("API_KEY", "projetods2025")
DELAY = float(os.environ.get("DELAY", "0"))
CSV_URL = os.environ.get("CSV_URL")        # URL direta do CSV (Drive/Dropbox/S3)
CSV_PATH = os.environ.get("CSV_PATH", "")  # Caminho local do CSV no repo (fallback)

# cache em memória
_df_cache = None

def load_df():
    """Carrega o DataFrame uma única vez (lazy-load) a partir de CSV_URL ou CSV_PATH."""
    global _df_cache
    if _df_cache is not None:
        return _df_cache

    src = CSV_URL or CSV_PATH or "df_t_pequeno.csv"  # fallback seguro
    app.logger.info(f"Carregando dados de: {src}")
    try:
        df = pd.read_csv(src)
    except Exception as e:
        app.logger.error(f"Falha ao ler CSV: {e}")
        df = pd.DataFrame()

    # normalização mínima recomendada
    if "date_purchase" in df.columns:
        df["date_purchase"] = pd.to_datetime(df["date_purchase"], errors="coerce").astype(str)

    return set_df_cache(df)

def set_df_cache(df: pd.DataFrame):
    global _df_cache
    _df_cache = df
    return _df_cache

def require_token():
    token = request.headers.get("x-api-key")
    if token != API_KEY:
        return False
    return True

def coerce_cols(df: pd.DataFrame, cols_param: str):
    """Valida e seleciona colunas solicitadas via ?cols=col1,col2,..."""
    if not cols_param:
        return df
    requested = [c.strip() for c in cols_param.split(",") if c.strip()]
    valid = [c for c in requested if c in df.columns]
    return df[valid]

@app.route("/")
def home():
    return jsonify({"status": "ok", "msg": "API Fake ClickBus rodando"})

# ===== Novo: schema para ajudar a criar tabela no banco =====
@app.route("/schema", methods=["GET"])
def schema():
    if not require_token():
        return jsonify({"erro": "Acesso não autorizado"}), 401

    if DELAY:
        time.sleep(DELAY)

    df = load_df()

    # monta tipos "simples" (str -> string, float64 -> float, int64 -> int)
    mapping = {"object": "string", "float64": "float", "int64": "int", "bool": "bool", "datetime64[ns]": "datetime"}
    cols = []
    for name, dtype in df.dtypes.items():
        cols.append({
            "name": name,
            "type": mapping.get(str(dtype), str(dtype))
        })

    return jsonify({
        "columns": cols,
        "count": int(len(df))
    })

# ===== Dados com filtros, seleção de colunas e paginação =====
@app.route("/dados", methods=["GET"])
def get_dados():
    if not require_token():
        return jsonify({"erro": "Acesso não autorizado"}), 401

    if DELAY:
        time.sleep(DELAY)

    df = load_df().copy()

    # filtros existentes
    cliente = request.args.get("cliente")
    data = request.args.get("data")  # prefixo YYYY-MM

    if cliente and "fk_contact" in df.columns:
        df = df[df["fk_contact"].astype(str) == str(cliente)]

    if data and "date_purchase" in df.columns:
        df = df[df["date_purchase"].str.startswith(data)]

    # ===== novo: selecionar colunas =====
    cols = request.args.get("cols")  # ex.: fk_contact,date_purchase,gmv_success
    if cols:
        df = coerce_cols(df, cols)

    # ===== novo: paginação simples =====
    try:
        limit = int(request.args.get("limit", "0"))
    except ValueError:
        limit = 0
    try:
        offset = int(request.args.get("offset", "0"))
    except ValueError:
        offset = 0

    if offset > 0:
        df = df.iloc[offset:]
    if limit > 0:
        df = df.iloc[:limit]

    # ===== novo: formato NDJSON para ingestão em DBs =====
    fmt = request.args.get("format", "json").lower()
    if fmt == "ndjson":
        # cada linha é um registro JSON; ideal para pipelines/ingestores
        def gen():
            for rec in df.to_dict(orient="records"):
                yield pd.io.json.dumps(rec, ensure_ascii=False) + "\n"
        return Response(gen(), mimetype="application/x-ndjson")

    # padrão: JSON array
    return jsonify(df.to_dict(orient="records"))

# ===== (Opcional) recarregar cache após trocar CSV_URL/CSV_PATH =====
@app.route("/reload", methods=["POST"])
def reload():
    if not require_token():
        return jsonify({"erro": "Acesso não autorizado"}), 401
    set_df_cache(None)
    load_df()
    return jsonify({"status": "ok", "msg": "Dados recarregados"})

if __name__ == "__main__":
    # Render: escutar na porta do ENV e no host 0.0.0.0
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
