"""
Consulta tickets Nuvem Envio no Databricks com campos de rastreio (custom fields).

Credenciais: arquivo `.env` na raiz deste projeto (veja `.env.example`) ou,
se ainda usar a pasta Auto-QA ao lado, `../Auto-QA/Databricks/.env`.

Uso CLI (na raiz do repositório):
  python nuvem_envio_rastreio.py --start 2026-03-01 --end 2026-03-27

Preencha nuvem_envio_rastreio_config.json com os field IDs
(veja sql_descobrir_campos_rastreio.sql no Databricks).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_DIR = Path(__file__).resolve().parent


def _load_env_into_os() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    explicit = (os.environ.get("NUVEM_DOTENV_PATH") or "").strip()
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser().resolve())
    candidates.extend(
        [
            _DIR / ".env",
            _DIR / "credenciais" / ".env",
        ]
    )
    # Legado: Auto-QA como pasta irmã do projeto (ex.: Documents/painel + Documents/Auto-QA)
    parent = _DIR.parent
    candidates.extend(
        [
            parent / "Auto-QA" / "Databricks" / ".env",
            parent / "Auto-QA" / "Databricks" / "credenciais" / ".env",
        ]
    )
    # Legado: projeto em outro lugar, mas credenciais no Auto-QA em Documents (Windows comum)
    home_docs = Path.home() / "Documents"
    candidates.extend(
        [
            home_docs / "Auto-QA" / "Databricks" / ".env",
            home_docs / "Auto-QA" / "Databricks" / "credenciais" / ".env",
        ]
    )
    for env_path in candidates:
        if env_path.is_file():
            load_dotenv(dotenv_path=env_path)
            break


def load_config(path: Path | None = None) -> dict[str, Any]:
    p = path or (_DIR / "nuvem_envio_rastreio_config.json")
    if not p.exists():
        raise FileNotFoundError(f"Config não encontrado: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


# Chaves herdadas de cada aba a partir da raiz do JSON (quando existe `tabs`).
_ROOT_KEYS_INHERITED_BY_TAB = (
    "catalog_schema",
    "dashboard_base_url",
    "zendesk_ticket_url_template",
)

DATA_MODEL_BR = "br_three_carriers"
DATA_MODEL_AR = "ar_tracking_single_field"


def effective_config_for_tab(raw: dict[str, Any], tab_key: str) -> dict[str, Any]:
    """Mescla config global com `tabs.<tab_key>` (Brasil / Argentina).

    JSON **sem** chave `tabs` = config legada só para Brasil (um país).
    """
    tabs = raw.get("tabs")
    if not tabs:
        if tab_key == "brasil":
            return dict(raw)
        raise ValueError(
            f"Config sem 'tabs': só há perfil Brasil. tab_key={tab_key!r} não suportado."
        )
    block = tabs.get(tab_key)
    if not isinstance(block, dict):
        raise KeyError(f"tabs.{tab_key} ausente ou inválido no JSON de config.")
    merged: dict[str, Any] = {}
    for k in _ROOT_KEYS_INHERITED_BY_TAB:
        if k in raw:
            merged[k] = raw[k]
    merged.update(block)
    merged.setdefault(
        "data_model",
        DATA_MODEL_AR if tab_key == "argentina" else DATA_MODEL_BR,
    )
    return merged


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


_QTY_FIELDS = (
    "quantidade_rastreio_correios",
    "quantidade_rastreio_jadlog",
    "quantidade_rastreio_loggi",
)

_BR_FIELD_ORDER = (
    "status_rastreamento",
    "quantidade_rastreio_correios",
    "quantidade_rastreio_jadlog",
    "quantidade_rastreio_loggi",
)

# JSON do app Zendesk "Controle de Tracking Numbers" (array com createdAt / completedAt por código).
_BR_OPTIONAL_FIELD_KEYS = ("tracking_numbers_data",)

BR_ZENDESK_FIELD_KEYS = _BR_FIELD_ORDER


def build_sql(
    *,
    start_date: str,
    end_date: str,
    statuses: list[str],
    config: dict[str, Any],
    limit: int | None = None,
    only_with_tracking_filled: bool = False,
) -> str:
    schema = str(config.get("catalog_schema") or "").strip()
    if not schema:
        raise ValueError("Defina catalog_schema no JSON de config (ex.: catalogo.schema_tickets).")
    bu_id = str(config.get("bu_field_id") or "").strip()
    if not bu_id:
        raise ValueError("Defina bu_field_id no JSON de config (ID do custom field de BU no Zendesk).")
    zf = config.get("zendesk_field_ids") or {}
    data_model = str(config.get("data_model") or DATA_MODEL_BR).strip()

    extra_cols: list[tuple[str, str]] = []
    cf_ids: list[str] = [bu_id]

    if data_model == DATA_MODEL_AR:
        for logical, fid in sorted(zf.items(), key=lambda x: x[0]):
            fid_s = str(fid).strip()
            if fid_s:
                extra_cols.append((logical, fid_s))
                if fid_s not in cf_ids:
                    cf_ids.append(fid_s)
    else:
        for logical in _BR_FIELD_ORDER:
            fid_s = str(zf.get(logical) or "").strip()
            if fid_s:
                extra_cols.append((logical, fid_s))
                if fid_s not in cf_ids:
                    cf_ids.append(fid_s)
        for logical in _BR_OPTIONAL_FIELD_KEYS:
            fid_s = str(zf.get(logical) or "").strip()
            if fid_s and not any(c[0] == logical for c in extra_cols):
                extra_cols.append((logical, fid_s))
                if fid_s not in cf_ids:
                    cf_ids.append(fid_s)

    case_lines = [
        f"MAX(CASE WHEN cf_id = '{_sql_escape(bu_id)}' THEN cf_val END) AS bu",
    ]
    for logical, fid_s in extra_cols:
        case_lines.append(
            f"MAX(CASE WHEN cf_id = '{_sql_escape(fid_s)}' THEN cf_val END) AS {logical}"
        )

    id_in_list = ", ".join(f"'{_sql_escape(x)}'" for x in cf_ids)

    grupos = config.get("filtro_grupo_contem") or []
    bus = config.get("filtro_bu_contem") or []
    if not grupos or not bus:
        raise ValueError(
            "Preencha filtro_grupo_contem e filtro_bu_contem no JSON de config (listas não vazias)."
        )

    grupo_or = " OR ".join(
        f"LOWER(COALESCE(g.name, '')) LIKE '%{_sql_escape(g.lower())}%'" for g in grupos
    )
    bu_or = " OR ".join(
        f"LOWER(COALESCE(p.bu, '')) LIKE '%{_sql_escape(b.lower())}%'" for b in bus
    )

    status_list = ", ".join(f"'{_sql_escape(s)}'" for s in statuses)

    limit_clause = ""
    if limit is not None:
        n = int(limit)
        if n > 0:
            limit_clause = f"LIMIT {n}"

    logical_set = {c[0] for c in extra_cols}

    if data_model == DATA_MODEL_AR and "tracking_numbers_data" in logical_set:
        # Conta itens se for JSON array; senão 1 se houver texto não vazio (Zendesk pode variar o formato).
        total_qtd_sql = """(
      CASE
        WHEN p.tracking_numbers_data IS NULL
          OR TRIM(CAST(p.tracking_numbers_data AS STRING)) IN ('', '{}', 'null', '[]')
        THEN 0
        ELSE COALESCE(
          TRY_CAST(SIZE(FROM_JSON(TRIM(CAST(p.tracking_numbers_data AS STRING)), 'ARRAY<STRING>')) AS INT),
          1
        )
      END
    ) AS total_qtd_rastreio"""
    else:
        sum_parts = [
            f"COALESCE(TRY_CAST(p.{f} AS INT), 0)"
            for f in _QTY_FIELDS
            if f in logical_set
        ]
        total_qtd_sql = (
            "(" + " + ".join(sum_parts) + ") AS total_qtd_rastreio"
            if sum_parts
            else "CAST(0 AS INT) AS total_qtd_rastreio"
        )

    tracking_filter_sql = ""
    if only_with_tracking_filled and extra_cols:
        if data_model == DATA_MODEL_AR and "tracking_numbers_data" in logical_set:
            tracking_filter_sql = """  AND (
      p.tracking_numbers_data IS NOT NULL
      AND TRIM(CAST(p.tracking_numbers_data AS STRING)) NOT IN ('', '{}', 'null', '[]')
    )
"""
        else:
            qty_checks = [
                f"COALESCE(TRY_CAST(p.{f} AS INT), 0) > 0"
                for f in _QTY_FIELDS
                if f in logical_set
            ]
            status_checks: list[str] = []
            if "status_rastreamento" in logical_set:
                status_checks.append(
                    """(
      p.status_rastreamento IS NOT NULL
      AND TRIM(CAST(p.status_rastreamento AS STRING)) != ''
      AND TRIM(CAST(p.status_rastreamento AS STRING)) != '{}'
      AND LOWER(TRIM(CAST(p.status_rastreamento AS STRING))) != 'null'
    )"""
                )
            parts = qty_checks + status_checks
            if parts:
                tracking_filter_sql = "  AND (" + " OR ".join(parts) + ")\n"

    return f"""
WITH cf_raw AS (
    SELECT
        t.id AS ticket_id,
        get_json_object(cf_item, '$.id') AS cf_id,
        get_json_object(cf_item, '$.value') AS cf_val
    FROM {schema}.tickets t
    LATERAL VIEW EXPLODE(FROM_JSON(t.custom_fields, 'ARRAY<STRING>')) e AS cf_item
    WHERE CAST(t.updated_at AS DATE) >= '{_sql_escape(start_date)}'
      AND CAST(t.updated_at AS DATE) <= '{_sql_escape(end_date)}'
      AND t.status IN ({status_list})
),
cf_pivot AS (
    SELECT
        ticket_id,
        {", ".join(case_lines)}
    FROM cf_raw
    WHERE cf_id IN ({id_in_list})
      AND cf_val IS NOT NULL AND cf_val != '' AND cf_val != 'null'
    GROUP BY ticket_id
)
SELECT
    t.id AS ticket_id,
    t.status,
    CAST(t.created_at AS TIMESTAMP) AS created_at,
    CAST(t.updated_at AS TIMESTAMP) AS updated_at,
    COALESCE(g.name, CAST(t.group_id AS STRING)) AS grupo,
    p.bu,
    {", ".join(f"p.{c[0]}" for c in extra_cols) if extra_cols else "CAST(NULL AS STRING) AS _sem_campos_opcionais"},
    {total_qtd_sql}
FROM {schema}.tickets t
LEFT JOIN {schema}.groups g ON t.group_id = g.id
LEFT JOIN cf_pivot p ON p.ticket_id = t.id
WHERE CAST(t.updated_at AS DATE) >= '{_sql_escape(start_date)}'
  AND CAST(t.updated_at AS DATE) <= '{_sql_escape(end_date)}'
  AND t.status IN ({status_list})
  AND (
    ({grupo_or})
    OR ({bu_or})
  )
{tracking_filter_sql}ORDER BY total_qtd_rastreio DESC, t.updated_at DESC
{limit_clause}
"""


def fetch_dataframe(
    *,
    start_date: str,
    end_date: str,
    statuses: list[str] | None = None,
    config: dict[str, Any] | None = None,
    limit: int | None = None,
    only_with_tracking_filled: bool | None = None,
    tab_key: str = "brasil",
) -> pd.DataFrame:
    _load_env_into_os()
    token = (os.getenv("databricks_token") or "").strip()
    host = (os.getenv("databricks_host") or "").strip()
    http_path = (os.getenv("databricks_http_path") or "").strip()
    if not token or not host or not http_path:
        raise RuntimeError(
            "Defina databricks_token, databricks_host e databricks_http_path no .env na raiz deste projeto "
            "(veja .env.example e README; opcionalmente NUVEM_DOTENV_PATH para outro arquivo .env)."
        )

    raw = config or load_config()
    if isinstance(raw.get("tabs"), dict) and tab_key:
        cfg = effective_config_for_tab(raw, tab_key)
    else:
        cfg = raw
    st = statuses or ["new", "open", "pending", "hold", "solved", "closed"]
    if only_with_tracking_filled is None:
        only_track = bool(cfg.get("somente_com_rastreio_preenchido", False))
    else:
        only_track = only_with_tracking_filled
    sql = build_sql(
        start_date=start_date,
        end_date=end_date,
        statuses=st,
        config=cfg,
        limit=limit,
        only_with_tracking_filled=only_track,
    )

    from databricks import sql as dbsql

    with dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in (cur.description or [])]
            rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=cols)
    if "_sem_campos_opcionais" in df.columns:
        df = df.drop(columns=["_sem_campos_opcionais"])
    for col in (
        "quantidade_rastreio_correios",
        "quantidade_rastreio_jadlog",
        "quantidade_rastreio_loggi",
    ):
        if col in df.columns:
            df[f"{col}_num"] = pd.to_numeric(df[col], errors="coerce")
    if "tracking_numbers_data" in df.columns:
        df["tracking_numbers_data_num"] = pd.to_numeric(
            df["tracking_numbers_data"], errors="coerce"
        )
    if "total_qtd_rastreio" in df.columns:
        df["total_qtd_rastreio"] = pd.to_numeric(df["total_qtd_rastreio"], errors="coerce")
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description="Exporta CSV de tickets NE + rastreio")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--status",
        default="new,open,pending,hold,solved,closed",
        help="Lista separada por vírgula",
    )
    parser.add_argument("--out", default="", help="Arquivo CSV de saída (opcional)")
    parser.add_argument(
        "--tab",
        default="brasil",
        choices=("brasil", "argentina"),
        help="Com JSON em tabs.*: qual aba usar na consulta (padrão: brasil).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Máximo de tickets retornados (0 = sem limite). Útil para testes.",
    )
    g = parser.add_mutually_exclusive_group()
    g.add_argument(
        "--somente-rastreio",
        action="store_true",
        help="Só tickets com quantidade > 0 em alguma transportadora ou status de rastreio preenchido.",
    )
    g.add_argument(
        "--todos-tickets",
        action="store_true",
        help="Inclui tickets sem dados de rastreio (ignora somente_com_rastreio_preenchido do JSON).",
    )
    args = parser.parse_args()
    statuses = [s.strip() for s in args.status.split(",") if s.strip()]
    lim = args.limit if args.limit and args.limit > 0 else None
    track_filter: bool | None
    if args.somente_rastreio:
        track_filter = True
    elif args.todos_tickets:
        track_filter = False
    else:
        track_filter = None
    try:
        df = fetch_dataframe(
            start_date=args.start,
            end_date=args.end,
            statuses=statuses,
            limit=lim,
            only_with_tracking_filled=track_filter,
            tab_key=args.tab,
        )
    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        return 1
    print(f"Linhas: {len(df)}")
    if args.out:
        df.to_csv(args.out, index=False, encoding="utf-8-sig")
        print(f"Salvo: {args.out}")
    else:
        print(df.head(20).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
