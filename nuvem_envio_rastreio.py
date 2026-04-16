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
            _DIR / "databricks" / ".env",
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
    _apply_streamlit_secrets_to_env()


def _apply_streamlit_secrets_to_env() -> None:
    """Preenche variáveis de ambiente a partir de st.secrets (Streamlit Community Cloud, etc.)."""
    try:
        import streamlit as st
    except ImportError:
        return
    try:
        sec = st.secrets
    except Exception:
        return
    for key in ("databricks_host", "databricks_http_path", "databricks_token"):
        try:
            if key not in sec:
                continue
            val = str(sec[key]).strip()
        except Exception:
            continue
        if val and not (os.environ.get(key) or "").strip():
            os.environ[key] = val


def load_config(path: Path | None = None) -> dict[str, Any]:
    """Carrega JSON de config: arquivo explícito, NUVEM_CONFIG_JSON, st.secrets ou arquivo padrão na raiz."""
    if path is not None:
        if not path.is_file():
            raise FileNotFoundError(f"Config não encontrado: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    env_json = (os.environ.get("NUVEM_CONFIG_JSON") or "").strip()
    if env_json:
        return json.loads(env_json)

    try:
        import streamlit as st

        sec = st.secrets
        if "nuvem_config_json" in sec:
            raw = str(sec["nuvem_config_json"]).strip()
            if raw:
                return json.loads(raw)
    except (ImportError, json.JSONDecodeError, KeyError, TypeError, RuntimeError, Exception):
        pass

    p = _DIR / "nuvem_envio_rastreio_config.json"
    if p.is_file():
        return json.loads(p.read_text(encoding="utf-8"))
    raise FileNotFoundError(
        "Config não encontrado: crie nuvem_envio_rastreio_config.json na raiz do projeto, "
        "defina a variável de ambiente NUVEM_CONFIG_JSON com o JSON completo, "
        "ou no Streamlit Cloud adicione o secret nuvem_config_json (JSON em uma linha ou TOML multilinha). "
        "Veja README e .streamlit/secrets.example.toml."
    )


# Chaves herdadas de cada aba a partir da raiz do JSON (quando existe `tabs`).
_ROOT_KEYS_INHERITED_BY_TAB = (
    "catalog_schema",
    "dashboard_base_url",
    "zendesk_ticket_url_template",
)

# Argentina: OR extra em `filtro_grupo_contem` (além do JSON). Esvazie a tupla para desligar no código.
# Deploy: use também NE_FILTRO_GRUPO_EXTRA_ARGENTINA (lista separada por vírgula ou ;).
_EXTRA_GRUPOS_ARGENTINA_TEMP: tuple[str, ...] = ()

# Argentina: estes ticket_id entram na query mesmo fora de grupo/BU (OR no WHERE). Preferir
# ``tabs.argentina.incluir_ticket_ids`` no JSON; deixe esta tupla vazia em produção.
_EXTRA_TICKET_IDS_ARGENTINA_TEMP: tuple[str, ...] = ()

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
    if tab_key == "argentina":
        _merge_extra_grupos_argentina(merged)
    return merged


def _merge_extra_grupos_argentina(merged: dict[str, Any]) -> None:
    """Acrescenta nomes de grupo ao filtro OR da SQL (aba Argentina)."""
    extras: list[str] = []
    seen_e: set[str] = set()
    for p in _EXTRA_GRUPOS_ARGENTINA_TEMP:
        t = str(p).strip()
        if t and t.lower() not in seen_e:
            extras.append(t)
            seen_e.add(t.lower())
    env_raw = (os.environ.get("NE_FILTRO_GRUPO_EXTRA_ARGENTINA") or "").strip()
    if env_raw:
        for chunk in env_raw.replace(";", ",").split(","):
            t = chunk.strip()
            if t and t.lower() not in seen_e:
                extras.append(t)
                seen_e.add(t.lower())
    if not extras:
        return
    fg = [str(x).strip() for x in (merged.get("filtro_grupo_contem") or []) if str(x).strip()]
    seen = {x.lower() for x in fg}
    for p in extras:
        if p.lower() not in seen:
            fg.append(p)
            seen.add(p.lower())
    merged["filtro_grupo_contem"] = fg


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


def _grupo_substring_match_sql(grupos: list[str]) -> str:
    """Substring no nome do grupo (case-insensitive), sem LIKE — evita metacaracteres em Databricks/Hive."""
    parts: list[str] = []
    for g in grupos:
        needle = _sql_escape(str(g).lower())
        parts.append(
            f"locate('{needle}', lower(cast(coalesce(g.name, '') as string))) > 0"
        )
    return " OR ".join(parts)


def _ticket_ids_from_env_and_temp_ar(
    data_model: str, config: dict[str, Any] | None = None
) -> list[str]:
    """Só `data_model` Argentina: IDs extra no OR do WHERE.

    Fontes (sem duplicar): env `NE_INCLUIR_TICKET_IDS`, tupla `_EXTRA_TICKET_IDS_ARGENTINA_TEMP`,
    JSON `tabs.argentina.incluir_ticket_ids` (lista de números/strings).

    Nunca aplicar em Brasil — `NE_INCLUIR_TICKET_IDS` não é lido fora do modelo AR.
    """
    if data_model != DATA_MODEL_AR:
        return []
    out: list[str] = []

    def _push_id(raw: object) -> None:
        xs = str(raw).strip().lstrip("#")
        if xs.endswith(".0") and xs[:-2].isdigit():
            xs = xs[:-2]
        if xs.isdigit() and xs not in out:
            out.append(xs)

    env_raw = (os.environ.get("NE_INCLUIR_TICKET_IDS") or "").strip()
    if env_raw:
        for part in env_raw.replace(";", ",").split(","):
            _push_id(part)
    for x in _EXTRA_TICKET_IDS_ARGENTINA_TEMP:
        _push_id(x)
    cfg_ids = (config or {}).get("incluir_ticket_ids")
    if isinstance(cfg_ids, (list, tuple)):
        for x in cfg_ids:
            _push_id(x)
    return out


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
    window_start_ts: str,
    window_end_ts: str,
    statuses: list[str],
    config: dict[str, Any],
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

    grupos = [str(x).strip() for x in (config.get("filtro_grupo_contem") or []) if str(x).strip()]
    bus = [str(x).strip() for x in (config.get("filtro_bu_contem") or []) if str(x).strip()]
    if not grupos:
        raise ValueError(
            "Preencha filtro_grupo_contem no JSON de config (lista não vazia de substrings do grupo)."
        )
    if data_model != DATA_MODEL_AR and not bus:
        raise ValueError(
            "No modelo Brasil, preencha filtro_bu_contem (lista não vazia). "
            "Na Argentina o filtro por BU não entra na SQL (valores iguais aos do Brasil puxavam tickets BR)."
        )

    grupo_or = _grupo_substring_match_sql(grupos)
    # Brasil: grupo OU BU. Argentina: só grupo + tickets forçados — OR por BU misturava tickets BR
    # (mesmo custom field de BU com envio_nube / envío_nube em ambos os países).
    bu_or = ""
    if data_model != DATA_MODEL_AR:
        bu_or = " OR ".join(
            f"LOWER(COALESCE(p.bu, '')) LIKE '%{_sql_escape(b.lower())}%'" for b in bus
        )

    extra_ticket_sql = ""
    _tid_force = _ticket_ids_from_env_and_temp_ar(data_model, config)
    _ids_sql = ""
    if _tid_force:
        _ids_sql = ", ".join(f"'{_sql_escape(x)}'" for x in _tid_force)
        extra_ticket_sql = f" OR CAST(t.id AS STRING) IN ({_ids_sql})"

    status_list = ", ".join(f"'{_sql_escape(s)}'" for s in statuses)

    logical_set = {c[0] for c in extra_cols}

    _trk_fid_ar = ""
    if data_model == DATA_MODEL_AR:
        for logical, fid_s in extra_cols:
            if logical == "tracking_numbers_data" and str(fid_s).strip():
                _trk_fid_ar = str(fid_s).strip()
                break
    _use_trk_fb = data_model == DATA_MODEL_AR and bool(_trk_fid_ar)
    _trk_src_sql = (
        "COALESCE(p.tracking_numbers_data, fb.trk_val)" if _use_trk_fb else "p.tracking_numbers_data"
    )

    if data_model == DATA_MODEL_AR and "tracking_numbers_data" in logical_set:
        # Quantidade na SQL ainda usa ARRAY<STRING> (legado); o total correto vem do pós-processamento Python.
        _n_tr = f"""CASE
        WHEN ({_trk_src_sql}) IS NULL
          OR TRIM(CAST(({_trk_src_sql}) AS STRING)) IN ('', '{{}}', 'null', '[]')
        THEN 0
        ELSE COALESCE(
          TRY_CAST(SIZE(FROM_JSON(TRIM(CAST(({_trk_src_sql}) AS STRING)), 'ARRAY<STRING>')) AS INT),
          0
        )
      END"""
        total_qtd_sql = f"(({_n_tr})) AS total_qtd_rastreio"
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
            # Não filtrar rastreio na SQL: o total AR usa JSON objeto (carrier → lista), mas a
            # expressão legada aqui é ARRAY<STRING> — zera e some o ticket antes do fetch.
            # O corte "só com rastreio" fica em ``fetch_dataframe`` com
            # ``_ar_segment_count_from_tracking_raw`` (igual ao total exibido).
            pass
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

    # Tickets forçados (AR): priorizar no ORDER BY para aparecerem no topo ao analisar casos pontuais.
    if _tid_force and _ids_sql:
        order_by_sql = (
            "ORDER BY CASE WHEN CAST(t.id AS STRING) IN ("
            + _ids_sql
            + ") THEN 0 ELSE 1 END ASC, total_qtd_rastreio DESC, t.updated_at DESC"
        )
    else:
        order_by_sql = "ORDER BY total_qtd_rastreio DESC, t.updated_at DESC"

    # Janela em timestamp (dashboard envia limites em America/Sao_Paulo como string ``YYYY-MM-DD HH:mm:ss``).
    # Filtro em ``updated_at`` permite uso de partition/cluster no lakehouse quando existir.
    _ws = _sql_escape(window_start_ts.strip())
    _we = _sql_escape(window_end_ts.strip())
    _date_core = (
        f"(CAST(t.updated_at AS TIMESTAMP) >= CAST('{_ws}' AS TIMESTAMP) "
        f"AND CAST(t.updated_at AS TIMESTAMP) <= CAST('{_we}' AS TIMESTAMP))"
    )
    # Argentina + IDs forçados: inclui o ticket mesmo fora do intervalo de datas (senão some do cf_raw).
    if _tid_force and _ids_sql and data_model == DATA_MODEL_AR:
        ticket_date_predicate = f"({_date_core} OR CAST(t.id AS STRING) IN ({_ids_sql}))"
    else:
        ticket_date_predicate = _date_core

    if extra_cols:
        _sel_extra = []
        for logical, _fid in extra_cols:
            if _use_trk_fb and logical == "tracking_numbers_data":
                _sel_extra.append(f"COALESCE(p.{logical}, fb.trk_val) AS {logical}")
            else:
                _sel_extra.append(f"p.{logical}")
        _extra_select_sql = ", ".join(_sel_extra)
    else:
        _extra_select_sql = "CAST(NULL AS STRING) AS _sem_campos_opcionais"

    _cte_trk_fb = ""
    _join_trk_fb = ""
    if _use_trk_fb:
        _cte_trk_fb = f""",
cf_trk_fb AS (
    SELECT CAST(ticket_id AS STRING) AS tid_s, MAX(cf_val) AS trk_val
    FROM cf_raw
    WHERE cf_id = '{_sql_escape(_trk_fid_ar)}'
    GROUP BY CAST(ticket_id AS STRING)
)"""
        _join_trk_fb = "\nLEFT JOIN cf_trk_fb fb ON fb.tid_s = CAST(t.id AS STRING)"

    # Uma linha por ticket: pipelines Zendesk→lakehouse às vezes mantêm histórico (várias linhas com
    # o mesmo ``id``). Sem isso, ``t.status`` pode ficar preso em ``open`` enquanto o Zendesk já fechou.
    _dedupe_tickets_cte = f"""tickets_z AS (
    SELECT *
    FROM {schema}.tickets
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY COALESCE(updated_at, created_at) DESC NULLS LAST, id DESC
    ) = 1
),
"""

    return f"""
WITH {_dedupe_tickets_cte}cf_raw AS (
    SELECT
        t.id AS ticket_id,
        get_json_object(cf_item, '$.id') AS cf_id,
        get_json_object(cf_item, '$.value') AS cf_val
    FROM tickets_z t
    LATERAL VIEW EXPLODE(FROM_JSON(t.custom_fields, 'ARRAY<STRING>')) e AS cf_item
    WHERE {ticket_date_predicate}
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
){_cte_trk_fb}
SELECT
    t.id AS ticket_id,
    t.status,
    CAST(t.created_at AS TIMESTAMP) AS created_at,
    CAST(t.updated_at AS TIMESTAMP) AS updated_at,
    COALESCE(g.name, CAST(t.group_id AS STRING)) AS grupo,
    p.bu,
    {_extra_select_sql},
    {total_qtd_sql}
FROM tickets_z t
LEFT JOIN {schema}.groups g ON t.group_id = g.id
LEFT JOIN cf_pivot p ON CAST(p.ticket_id AS STRING) = CAST(t.id AS STRING){_join_trk_fb}
WHERE {ticket_date_predicate}
  AND t.status IN ({status_list})
  AND (
    {(
        f"({grupo_or}){extra_ticket_sql}"
        if data_model == DATA_MODEL_AR
        else f"({grupo_or})\n    OR ({bu_or}){extra_ticket_sql}"
    )}
  )
{tracking_filter_sql}{order_by_sql}
"""


def _ar_item_has_shipment_code(item: object) -> bool:
    """True se o objeto de segmento tem código de rastreio utilizável (alinha ao dashboard).

    Com chave ``code`` no dict (app AR), só conta se ``code`` for texto não vazio — ``id`` não entra.
    Sem ``code``, usa os mesmos fallbacks que o painel, exceto ``id``.
    """
    if not isinstance(item, dict):
        return False
    if "code" in item:
        v = item.get("code")
        if v is None:
            return False
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                return False
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        return bool(t) and t.lower() not in ("null", "none", "nan")
    for key in (
        "trackingNumber",
        "tracking_number",
        "trackingCode",
        "codigo",
        "numero_rastreio",
        "numeroRastreio",
        "rastreio",
    ):
        v = item.get(key)
        if v is None:
            continue
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        if t and t.lower() not in ("null", "none", "nan"):
            return True
    return False


def _ar_segment_count_from_tracking_raw(val: object) -> int:
    """Conta só segmentos com código de rastreio (ex.: ``code`` preenchido); ignora ``id`` e linhas só consulta sem código.

    A SQL usa ``ARRAY<STRING>``; o payload real costuma ser ``{ "correo": [...], ... }``.
    """
    if val is None:
        return 0
    try:
        if pd.api.types.is_scalar(val) and pd.isna(val):
            return 0
    except (TypeError, ValueError):
        pass
    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode("utf-8")
        except Exception:
            return 0
    try:
        import numpy as np

        if isinstance(val, np.generic):
            val = val.item()
    except ImportError:
        pass

    def _count_dict_of_lists(d: dict) -> int:
        n = 0
        for v in d.values():
            if not isinstance(v, list):
                continue
            for it in v:
                if _ar_item_has_shipment_code(it):
                    n += 1
        return n

    if isinstance(val, dict):
        if val and all(isinstance(v, list) for v in val.values()):
            return _count_dict_of_lists(val)
        return 1 if _ar_item_has_shipment_code(val) else 0
    if isinstance(val, list):
        return sum(1 for it in val if _ar_item_has_shipment_code(it))
    s = str(val).strip()
    if not s or s.lower() in ("{}", "[]", "null", "none", "nan"):
        return 0
    try:
        o: object = json.loads(s)
    except (json.JSONDecodeError, TypeError, ValueError):
        return 0
    if isinstance(o, dict):
        if o and all(isinstance(v, list) for v in o.values()):
            return _count_dict_of_lists(o)
        return 1 if _ar_item_has_shipment_code(o) else 0
    if isinstance(o, list):
        return sum(1 for it in o if _ar_item_has_shipment_code(it))
    return 0


def fetch_dataframe(
    *,
    window_start_ts: str | None = None,
    window_end_ts: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    statuses: list[str] | None = None,
    config: dict[str, Any] | None = None,
    only_with_tracking_filled: bool | None = None,
    tab_key: str = "brasil",
) -> pd.DataFrame:
    """Consulta Databricks e devolve um DataFrame novo a cada chamada (sem ``@st.cache_data`` no Streamlit).

    Use ``window_start_ts`` / ``window_end_ts`` (``YYYY-MM-DD HH:mm:ss``) a partir do dashboard.
    Para CLI legado, ``start_date`` / ``end_date`` (``YYYY-MM-DD``) viram meia-noite → fim do dia.
    Não há ``LIMIT`` na SQL: o período define o volume retornado.
    """
    _load_env_into_os()
    token = (os.getenv("databricks_token") or "").strip()
    host = (os.getenv("databricks_host") or "").strip()
    http_path = (os.getenv("databricks_http_path") or "").strip()
    if not token or not host or not http_path:
        raise RuntimeError(
            "Defina databricks_token, databricks_host e databricks_http_path: arquivo .env na raiz "
            "(veja .env.example), ou no Streamlit Community Cloud em Settings → Secrets "
            "(mesmos nomes de chave). Opcional: NUVEM_DOTENV_PATH."
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
    if window_start_ts and window_end_ts:
        w0, w1 = window_start_ts.strip(), window_end_ts.strip()
    elif start_date and end_date:
        w0, w1 = f"{start_date.strip()} 00:00:00", f"{end_date.strip()} 23:59:59"
    else:
        raise ValueError(
            "Informe window_start_ts e window_end_ts, ou start_date e end_date (YYYY-MM-DD)."
        )
    sql = build_sql(
        window_start_ts=w0,
        window_end_ts=w1,
        statuses=st,
        config=cfg,
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
    if (
        str(cfg.get("data_model") or "").strip() == DATA_MODEL_AR
        and "tracking_numbers_data" in df.columns
        and "total_qtd_rastreio" in df.columns
        and not df.empty
    ):
        parsed = df["tracking_numbers_data"].map(_ar_segment_count_from_tracking_raw)
        # Só contagens reais do JSON (campo `code`); não inflar com total da SQL.
        df["total_qtd_rastreio"] = parsed.fillna(0).astype(int)
        df = df.sort_values("total_qtd_rastreio", ascending=False).reset_index(drop=True)
        if only_track:
            allow = set(_ticket_ids_from_env_and_temp_ar(DATA_MODEL_AR, cfg))
            if "ticket_id" in df.columns:
                tid = _normalize_ticket_id_series(df["ticket_id"])
                keep = (df["total_qtd_rastreio"] > 0) | tid.isin(allow)
                df = df.loc[keep].copy().reset_index(drop=True)
    if str(cfg.get("data_model") or "").strip() == DATA_MODEL_AR and not df.empty:
        df = _enforce_ar_tab_row_filter(df, cfg)
    return df


def _normalize_ticket_id_series(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    return out


def _enforce_ar_tab_row_filter(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    """Argentina: mantém só tickets cujo grupo contém algum filtro OU id forçado (cobre vazamento na SQL/engine)."""
    if "ticket_id" not in df.columns or "grupo" not in df.columns:
        return df
    grupos = [str(x).strip().lower() for x in (cfg.get("filtro_grupo_contem") or []) if str(x).strip()]
    if not grupos:
        return df
    allowed = set(_ticket_ids_from_env_and_temp_ar(DATA_MODEL_AR, cfg))
    gcol = df["grupo"].fillna("").astype(str).str.lower()
    mask = pd.Series(False, index=df.index)
    for p in grupos:
        mask |= gcol.str.contains(p, regex=False, na=False)
    if allowed:
        tid = _normalize_ticket_id_series(df["ticket_id"])
        mask |= tid.isin(allowed)
    return df.loc[mask].copy()


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
