# api_fake.py
import os
import json
import time
import pandas as pd
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# =========================
# Configurações por ambiente
# =========================
API_KEY  = os.environ.get("API_KEY", "projetods2025")
DELAY    = float(os.environ.get("DELAY", "0"))
CSV_URL  = os.environ.get("CSV_URL")          # URL direta (Dropbox ?dl=1, Supabase, S3 etc.)
CSV_PATH = os.environ.get("CSV_PATH", "")     # Caminho no repositório (fallback)
CSV_SEP  = os.environ.get("CSV_SEP")          # Forçar separador (ex.: ";")
APP_VER  = os.environ.get("APP_VER", "1.0.0")

# Cache em memória
_df_cache = None


# =========================
# Utilidades
# =========================
def set_df_cache(df):
    """Atualiza o cache."""
    global _df_cache
    _df_cache = df
    return _df_cache


def require_token():
    """Autenticação simples via header x-api-key."""
    token = request.headers.get("x-api-key")
    return token == API_KEY


def _read_csv_robusto(src: str) -> pd.DataFrame:
    """
    Leitura à prova de CSV:
      - aceita .csv e .csv.gz (compression='infer')
      - tenta autodetectar separador
      - permite forçar separador por env CSV_SEP
      - ignora linhas malformadas (on_bad_lines='skip')
    """
    read_common = dict(
        low_memory=False,
        encoding="utf-8",
        compression="infer",
        on_bad_lines="skip",
    )

    tentativas = []
    # 1) se usuário informou separador
    if CSV_SEP:
        tentativas.append({"sep": CSV_SEP, "engine": "python"})
    # 2) autodetecta
    tentativas.append({"sep": None, "engine": "python"})
    # 3) tentativas comuns
    tentativas.append({"sep": ";", "engine": "python"})
    tentativas.append({"sep": ",", "engine": "c"})

    ultimo_erro = None
    for opt in tentativas:
        try:
            df = pd.read_csv(src, **read_common, **opt)
            app.logger.info(f"[CSV] OK sep={opt['sep']} engine={opt['engine']} shape={df.shape}")
            return df
        except Exception as e:
            ultimo_erro = e
            app.logger.warning(f"[CSV] Falhou sep={opt['sep']} engine={opt['engine']}: {e}")

    app.logger.error(f"[CSV] Falha final ao ler {src}: {ultimo_erro}")
    return pd.DataFrame()


def load_df() -> pd.DataFrame:
    """Carrega DataFrame (lazy-load)."""
    global _df_cache
    if _df_cache is not None:
        return _df_cache

    src = CSV_URL or CSV_PATH or "df_t_pequeno.csv"
    app.logger.info(f"Carregando dados de: {src}")
    df = _read_csv_robusto(src)

    # Normalizações leves e seguras
    if not df.empty:
        if "date_purchase" in df.columns:
            df["date_purchase"] = pd.to_datetime(df["date_purchase"], errors="coerce").astype(str)

        if "gmv_success" in df.columns and df["gmv_success"].dtype == object:
            # troca vírgula por ponto se vier no padrão PT-BR
            df["gmv_success"] = pd.to_numeric(
                df["gmv_success"].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            )

        if "total_tickets_quantity_success" in df.columns:
            df["total_tickets_quantity_success"] = pd.to_numeric(
                df["total_tickets_quantity_success"], errors="coerce"
            ).astype("Int64")

    return set_df_cache(df)


def coerce_cols(df: pd.DataFrame, cols_param: str) -> pd.DataFrame:
    """Seleciona apenas as colunas pedidas em ?cols=a,b,c."""
    if not cols_param:
        return df
    req = [c.strip() for c in cols_param.split(",") if c.strip()]
    keep = [c for c in req if c in df.columns]
    return df[keep]


# =========================
# Rotas
# =========================
@app.route("/")
def root():
    return jsonify({"status": "ok", "service": "API Fake ClickBus"})


@app.route("/health")
def health():
    try:
        df = load_df()
        return jsonify({"status": "ok", "rows": int(len(df))})
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


@app.route("/version")
def version():
    return jsonify({"version": APP_VER})


@app.route("/schema", methods=["GET"])
def schema():
    if not require_token():
        return jsonify({"erro": "Acesso não autorizado"}), 401
    if DELAY:
        time.sleep(DELAY)

    df = load_df()
    mapping = {
        "object": "string",
        "float64": "float",
        "int64": "int",
        "Int64": "int",
        "bool": "bool",
        "datetime64[ns]": "datetime",
    }
    cols = [{"name": n, "type": mapping.get(str(t), str(t))} for n, t in df.dtypes.items()]
    return jsonify({"columns": cols, "count": int(len(df))})


@app.route("/dados", methods=["GET"])
def dados():
    if not require_token():
        return jsonify({"erro": "Acesso não autorizado"}), 401
    if DELAY:
        time.sleep(DELAY)

    df = load_df().copy()

    # Filtros
    cliente = request.args.get("cliente")
    data = request.args.get("data")  # prefixo YYYY-MM

    if cliente and "fk_contact" in df.columns:
        df = df[df["fk_contact"].astype(str) == str(cliente)]

    if data and "date_purchase" in df.columns:
        df = df[df["date_purchase"].str.startswith(data)]

    # Seleção de colunas
    cols = request.args.get("cols")
    if cols:
        df = coerce_cols(df, cols)

    # Paginação
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

    # Formatos
    fmt = request.args.get("format", "json").lower()
    if fmt == "ndjson":
        def gen():
            for rec in df.to_dict(orient="records"):
                yield json.dumps(rec, ensure_ascii=False) + "\n"
        return Response(gen(), mimetype="application/x-ndjson")

    return jsonify(df.to_dict(orient="records"))


@app.route("/reload", methods=["POST"])
def reload():
    if not require_token():
        return jsonify({"erro": "Acesso não autorizado"}), 401
    set_df_cache(None)
    load_df()
    return jsonify({"status": "ok", "msg": "Dados recarregados"})


# =========================
# Main (local) / Render
# =========================
if __name__ == "__main__":
    # No Render, ele define PORT; local usa 5000
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
