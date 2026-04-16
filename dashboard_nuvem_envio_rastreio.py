"""
Dashboard Streamlit — acompanhamento operação Nuvem Envio (campos de rastreio).

Instalação (uma vez):
  pip install -r requirements.txt

Execução (na raiz deste repositório):
  streamlit run dashboard_nuvem_envio_rastreio.py

Antes: preencha zendesk_field_ids em nuvem_envio_rastreio_config.json
(use sql_descobrir_campos_rastreio.sql no Databricks para achar os IDs).
Abas **Brasil** e **Argentina** em nuvem_envio_rastreio_config.json → `tabs`.
Brasil: três transportadoras + status por ticket. Argentina: [AR] Envio Nube, volume e tempo por envio.
Detalhe por ticket: gráfico de status, seletor na aba ou ?ne_codes=1 — tabela achatada (**tracking_numbers_data**;
Brasil: linha por **code**; Argentina: todas as entradas do JSON (incl. ``(sem código)``); **id** interno não é rastreio). Amostra: ?ne_ticket= + ne_tab.
"""
from __future__ import annotations

import hmac
import json
import math
import os
import sys
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_DIR = Path(__file__).resolve().parent
_NE_REPO_ROOT = _DIR
if str(_DIR) not in sys.path:
    sys.path.insert(0, str(_DIR))

import nuvem_envio_rastreio as ne

import base64
import html

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

_NE_ACCENT = "#0050c3"
_NE_ACCENT_HOVER = "#0040a0"
_NE_PAGE_TITLE = "Painel de Rastreamento"
# Exibição de datas na amostra: Zendesk grava em UTC; operação BR costuma ler em Brasília.
_NE_SAMPLE_TZ = ZoneInfo("America/Sao_Paulo")

_NE_PERIOD_CHOICES: tuple[str, ...] = (
    "Últimas 24h",
    "Últimos 7 dias",
    "Últimos 30 dias",
    "Mês atual",
)


def _ne_period_window_timestamps(periodo: str) -> tuple[str, str]:
    """Limites [início, fim] em ``YYYY-MM-DD HH:mm:ss`` (America/Sao_Paulo) para o filtro SQL em ``updated_at``."""
    now = datetime.now(_NE_SAMPLE_TZ).replace(microsecond=0)
    if periodo == "Últimas 24h":
        start = now - timedelta(hours=24)
        end = now
    elif periodo == "Últimos 7 dias":
        end = now
        d0 = now.date() - timedelta(days=6)
        start = datetime(d0.year, d0.month, d0.day, 0, 0, 0, tzinfo=_NE_SAMPLE_TZ)
    elif periodo == "Últimos 30 dias":
        end = now
        d0 = now.date() - timedelta(days=29)
        start = datetime(d0.year, d0.month, d0.day, 0, 0, 0, tzinfo=_NE_SAMPLE_TZ)
    elif periodo == "Mês atual":
        end = now
        start = datetime(now.year, now.month, 1, 0, 0, 0, tzinfo=_NE_SAMPLE_TZ)
    else:
        end = now
        d0 = now.date() - timedelta(days=6)
        start = datetime(d0.year, d0.month, d0.day, 0, 0, 0, tzinfo=_NE_SAMPLE_TZ)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt)
# Login — identidade Nuvemshop (UI dedicada)
_NE_NS_BLUE = "#0045FF"
_NE_NS_BLUE_HOVER = "#0038d6"
_NE_LOGIN_PAGE_BG = "#eef1f6"


def _ne_login_screen_css() -> str:
    """CSS injetado só na tela de login (antes de ``st.stop()``)."""
    bg = html.escape(_NE_LOGIN_PAGE_BG)
    blue = html.escape(_NE_NS_BLUE)
    blue_h = html.escape(_NE_NS_BLUE_HOVER)
    return f"""
<style>
@import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap");
html, body, .stApp, [data-testid="stAppViewContainer"] {{
  font-family: "Inter", "Segoe UI", system-ui, -apple-system, sans-serif !important;
  background: {bg} !important;
}}
[data-testid="stAppViewContainer"] > .main {{
  background: transparent !important;
  min-height: 100vh !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
}}
[data-testid="stSidebar"] {{ display: none !important; }}
[data-testid="stHeader"] {{ display: none !important; }}
[data-testid="stToolbar"] {{ display: none !important; }}
[data-testid="stDecoration"] {{ display: none !important; }}
[data-testid="stFooter"] {{ display: none !important; }}
footer {{ visibility: hidden !important; height: 0 !important; }}
.main .block-container {{
  padding: 1.5rem 1rem 2rem 1rem !important;
  max-width: 100% !important;
  width: 100% !important;
  display: flex !important;
  flex-direction: column !important;
  align-items: center !important;
  justify-content: center !important;
  box-sizing: border-box !important;
}}
[data-testid="stForm"] {{
  background: #ffffff !important;
  border-radius: 14px !important;
  border: 1px solid rgba(148, 163, 184, 0.2) !important;
  box-shadow: 0 16px 48px -12px rgba(15, 23, 42, 0.12), 0 4px 16px -4px rgba(15, 23, 42, 0.06) !important;
  max-width: 400px !important;
  width: 100% !important;
  box-sizing: border-box !important;
  margin: 0 auto !important;
  padding: 30px !important;
}}
.ne-login-title {{
  margin: 0 0 0.5rem 0 !important;
  font-size: 1.6rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.02em !important;
  color: #0f172a !important;
  line-height: 1.3 !important;
  text-align: center !important;
}}
.ne-login-sub {{
  margin: 0 0 1.65rem 0 !important;
  font-size: 0.9375rem !important;
  font-weight: 500 !important;
  color: #64748b !important;
  text-align: center !important;
  line-height: 1.45 !important;
}}
/* Campos: largura total no card + Base Web */
[data-testid="stForm"] [data-testid="stTextInput"],
[data-testid="stForm"] .stTextInput {{
  width: 100% !important;
  max-width: 100% !important;
}}
[data-testid="stForm"] .stTextInput > div,
[data-testid="stForm"] [data-testid="stTextInput"] > div {{
  width: 100% !important;
}}
[data-testid="stForm"] .stTextInput label,
[data-testid="stForm"] [data-testid="stTextInput"] label {{
  font-weight: 500 !important;
  color: #334155 !important;
  font-size: 0.875rem !important;
}}
[data-testid="stForm"] .stTextInput [data-baseweb="input"],
[data-testid="stForm"] [data-testid="stTextInput"] [data-baseweb="input"] {{
  border-radius: 12px !important;
  border-color: #e2e8f0 !important;
  background-color: #fafbfc !important;
}}
[data-testid="stForm"] .stTextInput [data-baseweb="input"]:focus-within,
[data-testid="stForm"] [data-testid="stTextInput"] [data-baseweb="input"]:focus-within {{
  border-color: {blue} !important;
  box-shadow: 0 0 0 1px {blue} !important;
}}
[data-testid="stForm"] div[data-baseweb="input"] > div {{
  border-radius: 12px !important;
  border-color: #e2e8f0 !important;
}}
[data-testid="stForm"] div[data-baseweb="input"]:focus-within > div {{
  border-color: {blue} !important;
  box-shadow: 0 0 0 1px {blue} !important;
}}
/* Dica "Press Enter to submit form" (InputInstructions) — login só precisa do botão Entrar */
[data-testid="stForm"] [data-testid="InputInstructions"] {{
  display: none !important;
  visibility: hidden !important;
  height: 0 !important;
  overflow: hidden !important;
  margin: 0 !important;
  padding: 0 !important;
}}
/* Botão Entrar — Streamlit usa div[data-testid=stForm] + button[data-testid=stBaseButton-primaryFormSubmit] */
[data-testid="stForm"] .stButton,
[data-testid="stForm"] [data-testid="stFormSubmitButton"] {{
  width: 100% !important;
}}
[data-testid="stForm"] [data-baseweb="button"] {{
  border: none !important;
  box-shadow: none !important;
  outline: none !important;
  background: transparent !important;
}}
[data-testid="stForm"] .stButton > button,
[data-testid="stForm"] [data-testid="stFormSubmitButton"] button,
[data-testid="stForm"] button[data-testid="stBaseButton-primaryFormSubmit"],
[data-testid="stForm"] button[kind="primaryFormSubmit"],
[data-testid="stForm"] button[data-testid="baseButton-primary"],
[data-testid="stForm"] button[kind="primary"] {{
  width: 100% !important;
  background-color: {blue} !important;
  background-image: none !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  border: none !important;
  border-width: 0 !important;
  border-color: transparent !important;
  border-radius: 10px !important;
  font-weight: 700 !important;
  font-size: 0.95rem !important;
  padding: 0.7rem 1rem !important;
  margin-top: 0.35rem !important;
  box-shadow: none !important;
}}
/* Rótulo markdown dentro do submit (Streamlit aplica cor de link / primary no texto) */
[data-testid="stForm"] [data-testid="stFormSubmitButton"] [data-testid="stMarkdownContainer"],
[data-testid="stForm"] [data-testid="stFormSubmitButton"] [data-testid="stMarkdownContainer"] p,
[data-testid="stForm"] [data-testid="stFormSubmitButton"] [data-testid="stMarkdownContainer"] span {{
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
}}
[data-testid="stForm"] .stButton > button:hover,
[data-testid="stForm"] [data-testid="stFormSubmitButton"] button:hover,
[data-testid="stForm"] button[kind="primaryFormSubmit"]:hover,
[data-testid="stForm"] button[kind="primary"]:hover {{
  background-color: {blue_h} !important;
  background-image: none !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  border: none !important;
  border-color: transparent !important;
}}
[data-testid="stForm"] .stButton > button:focus,
[data-testid="stForm"] .stButton > button:focus-visible,
[data-testid="stForm"] [data-testid="stFormSubmitButton"] button:focus,
[data-testid="stForm"] [data-testid="stFormSubmitButton"] button:focus-visible,
[data-testid="stForm"] button[kind="primaryFormSubmit"]:focus,
[data-testid="stForm"] button[kind="primaryFormSubmit"]:focus-visible {{
  outline: 2px solid rgba(0, 69, 255, 0.35) !important;
  outline-offset: 2px !important;
  border: none !important;
  border-color: transparent !important;
  box-shadow: none !important;
}}
[data-testid="stForm"] .stButton > button:active,
[data-testid="stForm"] [data-testid="stFormSubmitButton"] button:active,
[data-testid="stForm"] button[kind="primaryFormSubmit"]:active {{
  background-color: {blue_h} !important;
  color: #ffffff !important;
  -webkit-text-fill-color: #ffffff !important;
  border-color: transparent !important;
}}
</style>
"""


def _ne_dashboard_login_expected() -> tuple[str | None, str | None]:
    """(usuário_esperado_ou_None, senha_esperada). (None, None) = login desligado.

    Ordem: variáveis de ambiente (após dotenv) → Streamlit Secrets.
    Usuário é opcional; se definido, o campo usuário deve coincidir.
    """
    ne._load_env_into_os()
    pw = (
        (os.environ.get("NE_DASHBOARD_PASSWORD") or os.environ.get("DASHBOARD_PASSWORD") or "")
        .strip()
    )
    user = (
        (os.environ.get("NE_DASHBOARD_USER") or os.environ.get("DASHBOARD_USER") or "")
        .strip()
        or None
    )
    try:
        sec = st.secrets
        if not pw:
            pw = str(sec.get("dashboard_password") or sec.get("DASHBOARD_PASSWORD") or "").strip()
        if user is None:
            u = str(sec.get("dashboard_user") or sec.get("DASHBOARD_USER") or "").strip()
            if u:
                user = u
    except Exception:
        pass
    if not pw:
        return None, None
    return user, pw


def _ne_ensure_dashboard_auth() -> None:
    """Exige login quando há senha configurada; caso contrário segue sem tela."""
    expected_user, expected_pw = _ne_dashboard_login_expected()
    if not expected_pw:
        return
    if st.session_state.get("ne_auth_ok"):
        return

    st.markdown(_ne_login_screen_css(), unsafe_allow_html=True)

    with st.form("ne_login_form", clear_on_submit=False):
        st.markdown(
            f"""
            <div style="text-align:center;">
                <h1 class="ne-login-title">{html.escape(_NE_PAGE_TITLE)}</h1>
                <p class="ne-login-sub">Acesso restrito</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        typed_user = ""
        if expected_user is not None:
            typed_user = st.text_input("Usuário", key="ne_login_user")
        typed_pw = st.text_input("Senha", type="password", key="ne_login_pw")
        submitted = st.form_submit_button("Entrar", type="primary", use_container_width=True)

    if submitted:
        ok_u = True
        if expected_user is not None:
            ok_u = hmac.compare_digest(
                typed_user.strip().encode("utf-8"),
                expected_user.encode("utf-8"),
            )
        ok_p = hmac.compare_digest(
            typed_pw.encode("utf-8"),
            expected_pw.encode("utf-8"),
        )
        if ok_u and ok_p:
            st.session_state["ne_auth_ok"] = True
            # Evita um frame com st.error de fetch antigo ao entrar no painel.
            for _ek in list(st.session_state.keys()):
                if str(_ek).endswith("_fetch_error"):
                    st.session_state.pop(_ek, None)
            st.rerun()
        st.error("Usuário ou senha incorretos.")

    st.stop()


def _ne_truck_icon_data_uri() -> str:
    """SVG caminhão na cor da marca (para CSS background e tela de carregamento)."""
    hx = _NE_ACCENT.lstrip("#").lower()
    svg = (
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 48 36'>"
        f"<rect x='2' y='14' width='22' height='14' rx='1.5' fill='%23{hx}'/>"
        f"<path d='M24 20h10l5 8v8H24V20z' fill='%23{hx}'/>"
        f"<circle cx='12' cy='30' r='4' fill='%231e293b'/>"
        f"<circle cx='32' cy='30' r='4' fill='%231e293b'/></svg>"
    )
    return "data:image/svg+xml," + urllib.parse.quote(svg)


def _render_full_page_loading_ne() -> None:
    """Tela só com caminhão azul centralizado e texto (consulta Databricks)."""
    uri = _ne_truck_icon_data_uri()
    a = html.escape(_NE_ACCENT)
    st.markdown(
        f"""
        <style>
        [data-testid="stSidebar"] {{ display: none !important; }}
        [data-testid="stHeader"] {{ display: none !important; }}
        [data-testid="stAppViewContainer"] .main .block-container {{
            padding-top: 0 !important;
            max-width: 100% !important;
        }}
        @keyframes ne-load-truck-move {{
            0% {{ transform: translateX(0); }}
            50% {{ transform: translateX(20px); }}
            100% {{ transform: translateX(0); }}
        }}
        .ne-db-loading-wrap {{
            min-height: 85vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            box-sizing: border-box;
        }}
        .ne-db-loading-truck {{
            width: 88px;
            height: 66px;
            background: url("{uri}") center / contain no-repeat;
            animation: ne-load-truck-move 0.95s ease-in-out infinite;
        }}
        .ne-db-loading-text {{
            margin-top: 1.35rem;
            color: {a};
            font-size: 1.12rem;
            font-weight: 600;
            letter-spacing: 0.02em;
        }}
        </style>
        <div class="ne-db-loading-wrap">
            <div class="ne-db-loading-truck" aria-hidden="true"></div>
            <p class="ne-db-loading-text">Consultando dados via Databricks</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _run_pending_ne_fetch(raw_cfg: dict) -> None:
    """Se houver pedido de atualização na sessão, mostra loading, consulta e reinicia o app."""
    tabs = raw_cfg.get("tabs")
    if isinstance(tabs, dict) and "brasil" in tabs and "argentina" in tabs:
        tab_keys: tuple[str, ...] = ("brasil", "argentina")
    else:
        tab_keys = ("brasil",)
    for tab_key in tab_keys:
        sk = f"ne_{tab_key}_pending_fetch"
        if sk not in st.session_state:
            continue
        params: dict = st.session_state.pop(sk)
        sk_df = f"ne_df_{tab_key}"
        sk_meta = f"ne_meta_{tab_key}"
        err_key = f"ne_{tab_key}_fetch_error"
        _render_full_page_loading_ne()
        try:
            # Sessões antigas (start/end + limit): converte para janela timestamp.
            if "window_start_ts" not in params and params.get("start") and params.get("end"):
                params = {
                    **params,
                    "window_start_ts": f"{str(params['start']).strip()} 00:00:00",
                    "window_end_ts": f"{str(params['end']).strip()} 23:59:59",
                    "periodo": str(params.get("periodo") or "legado (datas antigas)"),
                }
            # Sempre raw_cfg + tab_key: o merge por aba fica só em fetch_dataframe (evita config errada).
            df = ne.fetch_dataframe(
                window_start_ts=params["window_start_ts"],
                window_end_ts=params["window_end_ts"],
                statuses=list(params["statuses"]),
                config=raw_cfg,
                only_with_tracking_filled=params.get("somente_rastreio"),
                tab_key=tab_key,
            )
        except Exception as e:
            st.session_state[err_key] = str(e)
        else:
            st.session_state[sk_df] = df
            st.session_state[sk_meta] = {
                "somente_rastreio": bool(params.get("somente_rastreio")),
                "periodo": str(params.get("periodo") or ""),
                "window_start_ts": str(params.get("window_start_ts") or ""),
                "window_end_ts": str(params.get("window_end_ts") or ""),
            }
            st.session_state.pop(err_key, None)
        st.rerun()


_TRACKING_STATUS_ORDER = (
    "Resolvido",
    "Pendente",
    "Solo consulta",
    "Aberto",
    "Sem informação de status",
    "Outros",
    "Só no total geral",
)
_TRACKING_STATUS_COLORS = {
    "Resolvido": "#16a34a",
    "Pendente": "#ca8a04",
    "Solo consulta": "#0891b2",
    "Aberto": "#2563eb",
    "Sem informação de status": "#94a3b8",
    "Outros": "#7c3aed",
    "Só no total geral": "#cbd5e1",
}


def _normalize_tracking_status_value(val: object) -> str:
    """Classifica status (BR `status_rastreamento` ou AR `status` no JSON do app).

    **Argentina — textos oficiais do app:** Solo consulta; Pendiente con merchant / transportista / BO;
    Finalizado. Demais caem nas regras genéricas (PT/EN/ES).
    """
    if val is None:
        return "sem_status"
    s = str(val).strip().lower()
    if not s or s in ("null", "none", "{}", "[]"):
        return "sem_status"
    s_norm = " ".join(s.split())

    if s_norm == "solo consulta":
        return "solo_consulta"
    if s_norm == "finalizado" or s_norm.startswith("finalizado "):
        return "resolvido"
    for phrase in ("pendiente con transportista", "pendiente con merchant", "pendiente con bo"):
        if s_norm == phrase or s_norm.startswith(phrase + " ") or s_norm.startswith(phrase + "—"):
            return "pendente"

    if any(
        k in s_norm
        for k in (
            "resolv",
            "solved",
            "closed",
            "fechad",
            "cerrad",
            "entregue",
            "delivered",
            "complet",
            "finaliz",
        )
    ):
        return "resolvido"
    if "pend" in s_norm or "pendiente" in s_norm:
        return "pendente"
    if any(k in s_norm for k in ("open", "opening", "abert", "abiert", "abierto")):
        return "aberto"
    return "outros"


_DETAIL_STATUS_INTERNAL_LABEL: dict[str, str] = {
    "resolvido": "Resolvido",
    "pendente": "Pendente",
    "solo_consulta": "Solo consulta",
    "aberto": "Aberto",
    "sem_status": "Sem informação de status",
    "outros": "Outros",
}


def _norm_tracking_code_key(code: str) -> str:
    return str(code).strip().lower().replace(" ", "").replace("-", "")


def _coerce_status_rastreamento_dict(raw: object) -> dict[str, object] | None:
    """Interpreta `status_rastreamento` como objeto JSON código → status (string, número, etc.)."""
    if raw is None:
        return None
    try:
        if pd.api.types.is_scalar(raw) and pd.isna(raw):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except Exception:
            return None
    try:
        import numpy as np

        if isinstance(raw, np.generic):
            raw = raw.item()
    except ImportError:
        pass
    if isinstance(raw, dict):
        return {str(k): v for k, v in raw.items()}
    s = str(raw).strip()
    if not s or s.lower() in ("{}", "null", "none", "nan"):
        return None
    try:
        parsed: object = json.loads(s)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError:
            return None
    if not isinstance(parsed, dict):
        return None
    return {str(k): v for k, v in parsed.items()}


def _parse_status_rastreamento_items(raw: object) -> list[tuple[str, str]]:
    """Pares ordenados (código de rastreio, texto de status) vindos do Zendesk `status_rastreamento`."""
    base = _coerce_status_rastreamento_dict(raw)
    if not base:
        return []
    out: list[tuple[str, str]] = []
    for k, v in base.items():
        key = str(k).strip()
        if not key:
            continue
        val = "" if v is None else str(v).strip()
        out.append((key, val))
    return out


def _parse_status_rastreamento_lookup(raw: object) -> tuple[dict[str, str], dict[str, str]]:
    """Extrai do JSON `status_rastreamento` mapas código → texto de status (exato e normalizado)."""
    exact: dict[str, str] = {}
    norm: dict[str, str] = {}
    base = _coerce_status_rastreamento_dict(raw)
    if not base:
        return exact, norm
    for k, v in base.items():
        key = str(k).strip()
        if not key:
            continue
        val = "" if v is None else str(v).strip()
        exact[key] = val
        norm[_norm_tracking_code_key(key)] = val
    return exact, norm


def _lookup_status_rastreamento_value(
    exact: dict[str, str], norm: dict[str, str], code: str
) -> str | None:
    if not code or code.startswith("(sem código"):
        return None
    if code in exact:
        return exact[code]
    return norm.get(_norm_tracking_code_key(code))


def _find_tracking_item_for_code(items: list[dict], scode: str) -> dict | None:
    """Cruza a chave do `status_rastreamento` com um objeto em `tracking_numbers_data`."""
    if not scode or not items:
        return None
    s_exact = str(scode).strip()
    s_norm = _norm_tracking_code_key(s_exact)
    for it in items:
        c = _tracking_display_code(it)
        if not c:
            continue
        if c.strip() == s_exact or _norm_tracking_code_key(c) == s_norm:
            return it
    return None


def _parse_status_rastreamento_json(raw: object) -> dict[str, int]:
    """Conta códigos no JSON {codigo: status} por categoria interna."""
    out = {
        "resolvido": 0,
        "pendente": 0,
        "solo_consulta": 0,
        "aberto": 0,
        "sem_status": 0,
        "outros": 0,
    }
    base = _coerce_status_rastreamento_dict(raw)
    if not base:
        return out
    for _k, v in base.items():
        cat = _normalize_tracking_status_value(v)
        if cat == "resolvido":
            out["resolvido"] += 1
        elif cat == "pendente":
            out["pendente"] += 1
        elif cat == "solo_consulta":
            out["solo_consulta"] += 1
        elif cat == "aberto":
            out["aberto"] += 1
        elif cat == "sem_status":
            out["sem_status"] += 1
        else:
            out["outros"] += 1
    return out


def _tracking_status_buckets_for_row(row: pd.Series) -> dict[str, float]:
    """Por ticket: contagens do JSON + excedente em relação a `total_qtd_rastreio`."""
    counts = _parse_status_rastreamento_json(row.get("status_rastreamento"))
    n_json = sum(counts.values())
    total_req = float(pd.to_numeric(row.get("total_qtd_rastreio"), errors="coerce") or 0)
    extra = max(0.0, total_req - float(n_json))
    return {
        "Resolvido": float(counts["resolvido"]),
        "Pendente": float(counts["pendente"]),
        "Solo consulta": float(counts["solo_consulta"]),
        "Aberto": float(counts["aberto"]),
        "Sem informação de status": float(counts["sem_status"]),
        "Outros": float(counts["outros"]),
        "Só no total geral": extra,
    }


def _tracking_status_buckets_for_row_ar(row: pd.Series) -> dict[str, float]:
    """Argentina: uma fatia por segmento **com código de rastreio** (`code`), classificando pelo `status` do app."""
    if "tracking_numbers_data" not in row.index:
        return _tracking_status_buckets_for_row(row)
    items = _parse_tracking_numbers_app_json(row.get("tracking_numbers_data"))
    if not items:
        return _tracking_status_buckets_for_row(row)
    cres = cpnd = csolo = cabt = csem = cout = 0
    for it in items:
        cat = _tracking_segment_status_category(
            it,
            None,
            ar_app_segment=True,
            prefer_app_status_over_zendesk=True,
        )
        if cat == "resolvido":
            cres += 1
        elif cat == "pendente":
            cpnd += 1
        elif cat == "solo_consulta":
            csolo += 1
        elif cat == "aberto":
            cabt += 1
        elif cat == "sem_status":
            csem += 1
        else:
            cout += 1
    # AR: só o que existe no JSON com `code` — não há fatia “Só no total geral” (excedente SQL).
    return {
        "Resolvido": float(cres),
        "Pendente": float(cpnd),
        "Solo consulta": float(csolo),
        "Aberto": float(cabt),
        "Sem informação de status": float(csem),
        "Outros": float(cout),
        "Só no total geral": 0.0,
    }


def _long_df_tracking_status_by_ticket(
    df: pd.DataFrame, top_n: int, *, for_argentina_tab: bool = False
) -> tuple[pd.DataFrame, list[str]]:
    """Dataframe longo (ticket_id, categoria, qtd, ord) e ordem dos tickets no eixo Y."""
    if "ticket_id" not in df.columns:
        return pd.DataFrame(), []
    work = df.copy()
    if for_argentina_tab:
        work["_tot"] = work.apply(lambda r: float(_ar_codes_per_row_for_metrics(r)), axis=1)
    elif "total_qtd_rastreio" in work.columns:
        work["_tot"] = pd.to_numeric(work["total_qtd_rastreio"], errors="coerce").fillna(0)
    else:
        work["_tot"] = 0.0
    work = work.sort_values("_tot", ascending=False).head(int(top_n))
    ticket_order = work["ticket_id"].astype(str).tolist()
    _ord_map = {c: i for i, c in enumerate(_TRACKING_STATUS_ORDER)}
    rows: list[dict[str, object]] = []
    for _, row in work.iterrows():
        tid = str(row["ticket_id"])
        bucket = (
            _tracking_status_buckets_for_row_ar(row)
            if for_argentina_tab
            else _tracking_status_buckets_for_row(row)
        )
        wrote = False
        for cat in _TRACKING_STATUS_ORDER:
            v = bucket.get(cat, 0.0)
            if v > 0:
                rows.append(
                    {
                        "ticket_id": tid,
                        "categoria": cat,
                        "qtd": float(v),
                        "ord": _ord_map[cat],
                    }
                )
                wrote = True
        if not wrote:
            # Sem fatia > 0 o Altair não desenhava o ticket (parecia “só 4 tickets” com 55 carregados).
            if for_argentina_tab:
                q = max(float(_ar_codes_per_row_for_metrics(row)), 1.0)
            else:
                total_req = float(pd.to_numeric(row.get("total_qtd_rastreio"), errors="coerce") or 0)
                q = max(total_req, 1.0)
            rows.append(
                {
                    "ticket_id": tid,
                    "categoria": "Sem informação de status",
                    "qtd": q,
                    "ord": _ord_map["Sem informação de status"],
                }
            )
    return pd.DataFrame(rows), ticket_order


def _tracking_json_loose_dict_segments(root: object) -> list[dict]:
    """Varre JSON aninhado e devolve dicts que parecem segmento de rastreio (último recurso após parse estrito)."""
    acc: list[dict] = []
    seen: set[int] = set()
    stack: list[object] = [root]

    def _hints_match(d: dict) -> bool:
        if not d:
            return False
        parts: list[str] = [str(k).lower() for k in d.keys()]
        for v in d.values():
            if isinstance(v, (str, int, float)) and not isinstance(v, bool):
                parts.append(str(v).lower()[:400])
        blob = " ".join(parts)[:4000]
        needles = (
            "track",
            "codigo",
            "code",
            "carrier",
            "correo",
            "andreani",
            "epick",
            "epik",
            "rastreio",
            "created",
            "finaliz",
            "completed",
            "shipment",
            "label",
            "package",
            "envio",
            "numero",
            "courier",
            "operadora",
        )
        return any(n in blob for n in needles)

    while stack and len(acc) < 400:
        cur = stack.pop()
        if isinstance(cur, dict):
            cid = id(cur)
            if cid in seen:
                continue
            seen.add(cid)
            if _hints_match(cur):
                if cur and all(isinstance(v, list) for v in cur.values()):
                    flat = [x for v in cur.values() if isinstance(v, list) for x in v]
                    if flat and all(isinstance(x, dict) for x in flat):
                        for v in reversed(list(cur.values())):
                            if isinstance(v, list):
                                for x in reversed(v):
                                    stack.append(x)
                        continue
                acc.append(cur)
                continue
            for v in cur.values():
                stack.append(v)
        elif isinstance(cur, list):
            for v in reversed(cur):
                stack.append(v)
        elif isinstance(cur, str):
            t = cur.strip()
            if (t.startswith("{") and t.endswith("}")) or (t.startswith("[") and t.endswith("]")):
                try:
                    stack.append(json.loads(t))
                except json.JSONDecodeError:
                    pass
    return acc


def _ar_format_preview_payload(raw: object, *, max_chars: int = 24000) -> str:
    """Texto legível do campo de tracking para fallback (coluna Status na tabela)."""
    cap = max(500, min(int(max_chars), 100000))
    try:
        if isinstance(raw, (dict, list)):
            return json.dumps(raw, ensure_ascii=False, indent=2)[:cap]
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    if not s:
        return ""
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            o = json.loads(s)
            if isinstance(o, (dict, list)):
                return json.dumps(o, ensure_ascii=False, indent=2)[:cap]
        except json.JSONDecodeError:
            pass
    return s[:cap]


def _parse_tracking_numbers_app_json(
    raw: object,
    *,
    require_shipment_code: bool = True,
) -> list[dict]:
    """Segmentos/códigos serializados no custom field (JSON no Zendesk).

    **Modelo Argentina ([AR] Envio Nube):** dicionário ``{ "correo": [...], "andreani": [...], ... }``.
    Todas as chaves cujo valor é **lista de objetos** são percorridas em ordem **alfabética da chave**
    (estável) e os itens são **concatenados** — nenhuma transportadora é ignorada.

    Com ``require_shipment_code=True`` (padrão para gráficos/contagens): ficam só segmentos com
    ``code`` de rastreio utilizável. Com ``False`` (detalhe AR): inclui também segmentos sem código
    (ex.: epik só consulta), um objeto por entrada na lista.

    Aceita: lista de objetos; lista de strings JSON; objeto com `cards`/`items`/etc.;
    objeto único com `createdAt` e término (`finalizadoAt` ou `completedAt`).

    O conector Databricks/pandas pode devolver **list** ou **dict** já desserializados;
    nesse caso não usar ``json.loads(str(raw))`` (o ``str`` de uma lista usa aspas simples e quebra o JSON).
    """

    def _filter_segments(segments: list[dict]) -> list[dict]:
        dicts = [d for d in segments if isinstance(d, dict)]
        if not require_shipment_code:
            return dicts
        return [
            d
            for d in dicts
            if bool(str(_tracking_display_code(d) or "").strip())
        ]

    def _coerce_list(obj: object) -> list[object]:
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            for key in (
                "items",
                "trackings",
                "trackingNumbers",
                "tracking_numbers",
                "data",
                "orders",
                "encomendas",
                "packages",
                "cards",
                "segments",
                "shipments",
                "labels",
                "records",
                "rows",
                "results",
                "result",
                "response",
                "payload",
                "content",
                "lista",
                "list",
            ):
                inner = obj.get(key)
                if isinstance(inner, list):
                    return inner
            if len(obj) == 1:
                only_v = next(iter(obj.values()))
                if isinstance(only_v, str):
                    tv = only_v.strip()
                    if tv.startswith("[") or tv.startswith("{"):
                        try:
                            inner = json.loads(tv)
                        except json.JSONDecodeError:
                            inner = None
                        if isinstance(inner, list):
                            return inner
                        if isinstance(inner, dict):
                            return _coerce_list(inner)
            # App AR [Envio Nube]: { "correo": [...], "andreani": [...], "epik": [...] } — todas as listas.
            if obj and all(isinstance(v, list) for v in obj.values()):
                merged: list[object] = []
                for _carrier_key in sorted(obj.keys(), key=lambda x: str(x).casefold()):
                    v = obj.get(_carrier_key)
                    if isinstance(v, list):
                        merged.extend(v)
                if merged and all(isinstance(x, dict) for x in merged):
                    return merged
            if any(
                k in obj
                for k in (
                    "createdAt",
                    "created_at",
                    "completedAt",
                    "completed_at",
                    "finalizadoAt",
                    "finalizado_at",
                )
            ):
                return [obj]
        return []

    def _item_to_dict(x: object) -> dict | None:
        if isinstance(x, dict):
            return x
        if isinstance(x, str):
            t = x.strip()
            if not t or t.lower() in ("null", "none", "{}"):
                return None
            try:
                inner = json.loads(t)
            except json.JSONDecodeError:
                return None
            return inner if isinstance(inner, dict) else None
        return None

    def _finalize_root(data: object) -> list[dict]:
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                return []
        seq = _coerce_list(data)
        out: list[dict] = []
        for x in seq:
            d = _item_to_dict(x)
            if d:
                out.append(d)
        return out

    if raw is None:
        return []
    try:
        if pd.api.types.is_scalar(raw) and pd.isna(raw):
            return []
    except (TypeError, ValueError):
        pass

    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except Exception:
            return []

    try:
        import numpy as np

        if isinstance(raw, np.generic):
            raw = raw.item()
    except ImportError:
        pass

    if isinstance(raw, dict):
        a = _finalize_root(raw)
        return _filter_segments(a) if a else _filter_segments(_tracking_json_loose_dict_segments(raw))

    if isinstance(raw, (list, tuple)):
        lst = list(raw)
        a = _finalize_root(lst)
        return _filter_segments(a) if a else _filter_segments(_tracking_json_loose_dict_segments(lst))

    s = str(raw).strip()
    if not s or s.lower() in ("{}", "null", "none", "nan", "[]"):
        return []
    if len(s) >= 2 and s[0] == s[-1] == '"':
        try:
            inner = json.loads(s)
            if isinstance(inner, str):
                s = inner.strip()
        except json.JSONDecodeError:
            pass
    try:
        parsed: object = json.loads(s)
    except json.JSONDecodeError:
        return []
    a = _finalize_root(parsed)
    return _filter_segments(a) if a else _filter_segments(_tracking_json_loose_dict_segments(parsed))


def _tracking_display_code(it: dict) -> str:
    """Código de rastreio exibido e usado em contagens.

    **App AR ([AR] Envio Nube):** o rastreio é sempre o campo ``code``. A chave ``id`` é só
    identificador interno do formulário — **nunca** deve aparecer como código nem contar como envio.
    Se a chave ``code`` existir no objeto (mesmo vazia), não faz fallback para outras chaves.
    """
    if not isinstance(it, dict):
        return ""
    if "code" in it:
        v = it.get("code")
        if v is None:
            return ""
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                return ""
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        if t and t.lower() not in ("null", "none", "nan"):
            return t
        return ""
    for key in (
        "trackingNumber",
        "tracking_number",
        "trackingCode",
        "codigo",
        "numero_rastreio",
        "numeroRastreio",
        "rastreio",
    ):
        v = it.get(key)
        if v is None:
            continue
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        if t and t.lower() not in ("null", "none", "nan"):
            return t
    return ""


def _tracking_display_carrier(it: dict) -> str:
    for key in ("carrier", "transportadora", "transportador", "courier", "operadora"):
        v = it.get(key)
        if v is None:
            continue
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        if t:
            return t
    return ""


def _tracking_agent_name(it: dict) -> str:
    """Nome do agente/guru que atualizou o código (ex.: agentName no JSON do app)."""
    for key in (
        "agentName",
        "agent_name",
        "guruName",
        "guru_name",
        "updatedBy",
        "updated_by",
        "userName",
        "user_name",
        "authorName",
        "author",
    ):
        v = it.get(key)
        if v is None:
            continue
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        if t and t.lower() not in ("null", "none", "nan", "{}"):
            return t
    return "—"


def _tracking_segment_end_timestamp(it: dict) -> object | None:
    """Instante de término do segmento: app Argentina usa `finalizadoAt`; fallback `completedAt`."""
    for key in ("finalizadoAt", "finalizado_at", "completedAt", "completed_at"):
        v = it.get(key)
        if v is None:
            continue
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        s = str(v).strip()
        if s and s.lower() not in ("null", "none", "nan"):
            return v
    return None


def _tracking_app_duracion_text(it: dict) -> str:
    if not isinstance(it, dict):
        return ""
    for key in ("duracion", "duración", "duration", "Duracion", "Duración"):
        v = it.get(key)
        if v is None:
            continue
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        if t:
            return t
    dur_keys = {"duracion", "duración", "duration"}
    for k, v in it.items():
        lk = str(k).strip().lower()
        if lk not in dur_keys:
            continue
        if v is None:
            continue
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        if t:
            return t
    return ""


# Argentina [AR]: três transportadoras do app + bucket para valores fora do padrão.
_AR_CARRIER_ORDER = ("Correo Argentino", "Andreani", "E-pick")
_AR_CARRIER_OTHER = "Outros / não informado"
_AR_CARRIER_STYLE: dict[str, tuple[str, str, str]] = {
    "Correo Argentino": ("#3d6ea8", "#ffffff", "CA"),
    "Andreani": ("#c41230", "#ffffff", "A"),
    "E-pick": ("#6d28d9", "#ffffff", "E"),
    _AR_CARRIER_OTHER: ("#64748b", "#ffffff", "?"),
}
_AR_CARRIER_CHART_COLORS = {
    "Correo Argentino": "#3d6ea8",
    "Andreani": "#c41230",
    "E-pick": "#6d28d9",
    _AR_CARRIER_OTHER: "#94a3b8",
}


def _ar_canonical_carrier(raw: object) -> str:
    """Normaliza o texto do campo carrier do app AR para Correo Argentino / Andreani / E-pick."""
    s = str(raw or "").strip().lower().replace("_", " ")
    if not s or s in ("—", "-", "null", "none", "nan"):
        return _AR_CARRIER_OTHER
    if "andreani" in s:
        return "Andreani"
    if "e-pick" in s or "epick" in s or "epik" in s or s == "e pick":
        return "E-pick"
    if "correo" in s or "correios" in s:
        return "Correo Argentino"
    return _AR_CARRIER_OTHER


def _ar_carrier_volume_df(df: pd.DataFrame) -> pd.DataFrame:
    """Conta segmentos no JSON por transportadora (Argentina)."""
    vol: dict[str, float] = {lab: 0.0 for lab in _AR_CARRIER_ORDER}
    vol[_AR_CARRIER_OTHER] = 0.0
    if "tracking_numbers_data" not in df.columns:
        keys = list(_AR_CARRIER_ORDER) + [_AR_CARRIER_OTHER]
        return pd.DataFrame([{"transportadora": k, "volume_solicitacoes": 0.0} for k in keys])
    for _, r in df.iterrows():
        for it in _parse_tracking_numbers_app_json(r["tracking_numbers_data"]):
            raw = _tracking_display_carrier(it)
            lab = _ar_canonical_carrier(raw)
            vol[lab] = vol.get(lab, 0.0) + 1.0
    keys = list(_AR_CARRIER_ORDER) + [_AR_CARRIER_OTHER]
    return pd.DataFrame([{"transportadora": k, "volume_solicitacoes": vol[k]} for k in keys])


def _ar_raw_tracking_field_nonempty(raw: object) -> bool:
    """True se o custom field de tracking não está vazio (mesmo quando parse/SQL devolvem 0)."""
    if raw is None:
        return False
    try:
        if pd.api.types.is_scalar(raw) and pd.isna(raw):
            return False
    except (TypeError, ValueError):
        pass
    s = str(raw).strip().lower()
    if not s or s in ("{}", "[]", "null", "none", "nan"):
        return False
    return True


def _ar_codes_per_row_for_metrics(r: pd.Series) -> int:
    """Códigos por ticket (AR): só segmentos com ``code``/rastreio preenchido — sem completar com SQL."""
    if "tracking_numbers_data" not in r.index:
        return 0
    return len(_parse_tracking_numbers_app_json(r.get("tracking_numbers_data")))


def _ar_count_tracking_codes_in_frame(
    frame: pd.DataFrame | None,
    *,
    min_one_per_ticket_if_rastreio: bool = False,
) -> int:
    """Soma de códigos na amostra (AR: só parse com `code`). Com filtro “só com rastreio”, cada linha conta no mínimo 1."""
    if frame is None or frame.empty:
        return 0
    per = [_ar_codes_per_row_for_metrics(r) for _, r in frame.iterrows()]
    if min_one_per_ticket_if_rastreio:
        return int(sum(max(x, 1) for x in per))
    return int(sum(per))


def _ar_max_tracking_codes_one_ticket(
    frame: pd.DataFrame | None,
    *,
    min_one_per_ticket_if_rastreio: bool = False,
) -> int:
    """Maior quantidade de códigos num único ticket (mesma regra que `_ar_codes_per_row_for_metrics`)."""
    if frame is None or frame.empty:
        return 0
    per = [_ar_codes_per_row_for_metrics(r) for _, r in frame.iterrows()]
    if not per:
        return 0
    if min_one_per_ticket_if_rastreio:
        return int(max(max(x, 1) for x in per))
    return int(max(per))


_BR_CARRIER_FILTER_LABELS = ("Correios", "Jadlog", "Loggi")


def _ne_filter_df_br_carriers(df: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    """Mantém tickets com quantidade > 0 em ao menos uma das transportadoras selecionadas."""
    if df.empty or not selected:
        return df
    col_map = {
        "Correios": ("quantidade_rastreio_correios_num", "quantidade_rastreio_correios"),
        "Jadlog": ("quantidade_rastreio_jadlog_num", "quantidade_rastreio_jadlog"),
        "Loggi": ("quantidade_rastreio_loggi_num", "quantidade_rastreio_loggi"),
    }
    mask = pd.Series(False, index=df.index)
    for lab in selected:
        pair = col_map.get(lab)
        if not pair:
            continue
        num, raw = pair
        if num in df.columns:
            mask |= pd.to_numeric(df[num], errors="coerce").fillna(0) > 0
        elif raw in df.columns:
            mask |= pd.to_numeric(df[raw], errors="coerce").fillna(0) > 0
    return df.loc[mask].copy()


def _ne_filter_df_ar_carriers(df: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    """Mantém tickets com ao menos um segmento em `tracking_numbers_data` da transportadora."""
    if df.empty or not selected:
        return df
    if "tracking_numbers_data" not in df.columns:
        return df.iloc[0:0].copy()
    sel = set(selected)

    def _row_ok(row: pd.Series) -> bool:
        items = _parse_tracking_numbers_app_json(row.get("tracking_numbers_data"))
        if not items:
            return _AR_CARRIER_OTHER in sel
        for it in items:
            if not str(_tracking_display_code(it) or "").strip():
                continue
            if _ar_canonical_carrier(_tracking_display_carrier(it)) in sel:
                return True
        return False

    m = df.apply(_row_ok, axis=1)
    return df.loc[m].copy()


def _ne_html_stacked_carriers_volume(
    vol_df: pd.DataFrame,
    carrier_style: dict[str, tuple[str, str, str]],
    logo_files: dict[str, str],
    *,
    total_footer_label: str = "solicitações",
) -> str:
    """Barra horizontal empilhada (HTML) — mesmo padrão visual Brasil/Argentina."""
    logo_dir = _DIR / "assets" / "logos"
    rows = vol_df[vol_df["volume_solicitacoes"] > 0].copy()
    if rows.empty:
        return ""
    total = float(rows["volume_solicitacoes"].sum())
    seg_parts: list[str] = []
    for _, r in rows.iterrows():
        name = str(r["transportadora"])
        v = float(r["volume_solicitacoes"])
        bg, fg, initial = carrier_style.get(name, ("#6b7280", "#ffffff", name[:1]))
        pct = 100.0 * v / total if total else 0.0
        min_w = "min-width:72px;" if pct >= 12 else "min-width:40px;"
        logo_file = logo_files.get(name)
        logo_path = logo_dir / logo_file if logo_file else None
        if logo_path and logo_path.is_file():
            b64 = base64.standard_b64encode(logo_path.read_bytes()).decode("ascii")
            mark = (
                f'<img src="data:image/png;base64,{b64}" '
                'alt="" style="max-height:36px;max-width:80px;object-fit:contain;" />'
            )
        else:
            mark = (
                f'<span style="font:800 22px system-ui,Segoe UI,sans-serif;'
                f"color:{fg};text-shadow:0 1px 2px rgba(0,0,0,.2);\">"
                f"{html.escape(initial)}</span>"
            )
        vol_lbl = html.escape(f"{v:.0f}")
        nm = html.escape(name)
        seg_parts.append(
            f'<div style="flex:{v} 1 0;{min_w}background:{bg};display:flex;'
            "flex-direction:column;align-items:center;justify-content:center;"
            f'gap:6px;padding:8px 4px;">{mark}'
            f'<span style="font:600 12px system-ui;color:{fg};opacity:.95;">{vol_lbl}</span>'
            f'<span style="font:11px system-ui;color:{fg};opacity:.85;">{nm}</span></div>'
        )
    inner = "".join(seg_parts)
    _pct_bits = [
        f"{r['transportadora']}: {100.0 * float(r['volume_solicitacoes']) / total:.1f}%"
        for _, r in rows.iterrows()
    ]
    _pct_line = html.escape(" · ".join(_pct_bits))
    return (
        '<div style="font-family:system-ui,Segoe UI,sans-serif;margin:12px 0 4px 0;">'
        '<div style="display:flex;width:100%;min-height:88px;border-radius:14px;'
        'overflow:hidden;box-shadow:0 2px 10px rgba(0,0,0,.12);">'
        f"{inner}</div>"
        f'<p style="margin:8px 0 0 0;font-size:13px;color:#444;">Total: <b>{total:.0f}</b> {html.escape(total_footer_label)}'
        f" · {_pct_line}</p>"
        "</div>"
    )


def _tracking_app_ttr_hours_resolved(created_at: object, completed_at: object) -> float | None:
    """TTR em horas: `createdAt` → término (`finalizadoAt` ou `completedAt`); só com término válido."""
    if completed_at is None:
        return None
    try:
        if pd.api.types.is_scalar(completed_at) and pd.isna(completed_at):
            return None
    except (TypeError, ValueError):
        pass
    cos = str(completed_at).strip()
    if not cos or cos.lower() in ("null", "none", "nan"):
        return None
    cas = created_at
    if cas is None or (isinstance(cas, str) and not str(cas).strip()):
        cas = completed_at
    try:
        t0 = pd.to_datetime(cas, utc=True, errors="coerce")
        t1 = pd.to_datetime(completed_at, utc=True, errors="coerce")
        if pd.isna(t0) or pd.isna(t1):
            return None
        sec = float((t1 - t0).total_seconds())
        if sec < 0:
            return None
        return sec / 3600.0
    except Exception:
        return None


def _format_ttr_hours_compact(hours: float | None) -> str:
    """TTR legível por código (ex.: 2d 4h, 3h, <1h)."""
    if hours is None:
        return "—"
    try:
        if isinstance(hours, float) and math.isnan(hours):
            return "—"
    except TypeError:
        return "—"
    try:
        total_sec = int(round(float(hours) * 3600.0))
    except (ValueError, OverflowError):
        return "—"
    if total_sec < 0:
        return "—"
    d, rem = divmod(total_sec, 86400)
    h, rem = divmod(rem, 3600)
    m, _sec = divmod(rem, 60)
    parts: list[str] = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m and not d and not h:
        parts.append(f"{m}m")
    if not parts:
        if total_sec == 0:
            return "0"
        return "<1h"
    return " ".join(parts)


def _tracking_item_inline_status_raw(it: dict, *, ar_app_segment: bool = False) -> str:
    """Status textual vindo do próprio objeto do app (se existir).

    **Argentina (`ar_app_segment=True`):** não usa `situation` / `situacion` / `situacao` — no app
    [AR] Envio Nube esses campos costumam refletir etapa de formulário (ex.: “Solo consulta”) enquanto
    o estado do envio está em ``status`` (ex.: ``Finalizado``). Ler ``situacao`` primeiro quebra
    tabela e gráfico quando ``status`` vem vazio no warehouse ou em cópias parciais do JSON.
    """
    if not isinstance(it, dict):
        return ""
    if ar_app_segment:
        keys = (
            "status",
            "Status",
            "estado",
            "Estado",
            "state",
            "State",
            "trackingStatus",
            "tracking_status",
        )
    else:
        keys = (
            "status",
            "estado",
            "state",
            "trackingStatus",
            "tracking_status",
            "situation",
            "situacion",
            "situacao",
        )
    for key in keys:
        v = it.get(key)
        if v is None:
            continue
        try:
            if pd.api.types.is_scalar(v) and pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        t = str(v).strip()
        if t and t.lower() not in ("null", "none", "nan", "{}"):
            return t
    if ar_app_segment:
        rank = {"status": 0, "trackingstatus": 1, "tracking_status": 2, "estado": 3, "state": 4}
        best_r = 99
        best_t = ""
        for k, v in it.items():
            lk = str(k).strip().lower().replace("-", "_")
            r = rank.get(lk, 99)
            if r == 99:
                continue
            if v is None:
                continue
            try:
                if pd.api.types.is_scalar(v) and pd.isna(v):
                    continue
            except (TypeError, ValueError):
                pass
            t = str(v).strip()
            if not t or t.lower() in ("null", "none", "nan", "{}"):
                continue
            if r < best_r:
                best_r = r
                best_t = t
        if best_t:
            return best_t
    return ""


def _tracking_segment_status_category(
    it: dict | None,
    raw_status_zendesk: str | None,
    *,
    ar_app_segment: bool = False,
    prefer_app_status_over_zendesk: bool = False,
) -> str:
    """Categoria interna (chaves de ``_normalize_tracking_status_value`` + ``resolvido`` por TTR).

    Usada na tabela de detalhe e no gráfico AR para não divergir: o mesmo segmento deve contar
    na mesma fatia que aparece na linha do código.
    """
    itd = it if isinstance(it, dict) else {}
    has_segment = bool(itd)
    ca = (itd.get("createdAt") or itd.get("created_at")) if has_segment else None
    co = _tracking_segment_end_timestamp(itd) if has_segment else None
    ttr = _tracking_app_ttr_hours_resolved(ca, co)
    cos = ""
    if co is not None:
        try:
            if not (pd.api.types.is_scalar(co) and pd.isna(co)):
                cos = str(co).strip()
        except (TypeError, ValueError):
            cos = str(co).strip()
    if ttr is not None:
        situacao = "Concluído"
    elif not cos or cos.lower() in ("null", "none", "nan"):
        situacao = "Em aberto"
    else:
        situacao = "Tempo não calculado"
    raw_z = (raw_status_zendesk or "").strip()
    raw_inline = (
        _tracking_item_inline_status_raw(itd, ar_app_segment=ar_app_segment) if has_segment else ""
    )
    if prefer_app_status_over_zendesk:
        raw_for = (raw_inline or raw_z).strip()
    else:
        raw_for = (raw_z or raw_inline).strip()
    if raw_for:
        return _normalize_tracking_status_value(raw_for)
    if situacao == "Concluído":
        return "resolvido"
    if situacao == "Em aberto":
        return "aberto"
    return "sem_status"


def flatten_tracking_numbers_data_detail(
    tracking_raw: object,
    ticket_id: str,
    status_rastreamento_raw: object | None = None,
    *,
    for_argentina_tab: bool = False,
) -> pd.DataFrame:
    """Uma linha por código: **chaves de `status_rastreamento`** (código → status) + enriquecimento do JSON de rastreio.

    Quando `status_rastreamento` existe, cada par (código, status) do Zendesk gera uma linha; o
    `tracking_numbers_data` só cruza para guru/TTR/transportadora quando o código bate.

    Sem `status_rastreamento`, cai no modo antigo (só itens de `tracking_numbers_data`).

    **Argentina:** `for_argentina_tab=True` ignora o ramo por `status_rastreamento` (fluxo BR); só
    `tracking_numbers_data` monta as linhas — evita tabela só com “-” quando o mapa de status não
    casa com o JSON do app AR.
    """
    tid = _norm_ticket_id(ticket_id)
    ex, nx = _parse_status_rastreamento_lookup(status_rastreamento_raw)
    status_pairs = _parse_status_rastreamento_items(status_rastreamento_raw)
    if for_argentina_tab:
        status_pairs = []
        ex, nx = {}, {}
    items = _parse_tracking_numbers_app_json(
        tracking_raw,
        require_shipment_code=not for_argentina_tab,
    )
    rows: list[dict[str, object]] = []

    def _append_row(
        idx: int,
        display_code: str,
        it: dict | None,
        raw_status_zendesk: str | None,
        *,
        ar_detail_row: bool = False,
    ) -> None:
        itd = it if isinstance(it, dict) else {}
        has_segment = bool(itd)
        guru = _tracking_agent_name(itd) if has_segment else "—"
        ca = (itd.get("createdAt") or itd.get("created_at")) if has_segment else None
        co = _tracking_segment_end_timestamp(itd) if has_segment else None
        ttr = _tracking_app_ttr_hours_resolved(ca, co)
        cos = ""
        if co is not None:
            try:
                if not (pd.api.types.is_scalar(co) and pd.isna(co)):
                    cos = str(co).strip()
            except (TypeError, ValueError):
                cos = str(co).strip()
        if ttr is not None:
            situacao = "Concluído"
        elif not cos or cos.lower() in ("null", "none", "nan"):
            situacao = "Em aberto"
        else:
            situacao = "Tempo não calculado"
        raw_z = (raw_status_zendesk or "").strip()
        cat = _tracking_segment_status_category(
            it,
            raw_status_zendesk,
            ar_app_segment=ar_detail_row,
            prefer_app_status_over_zendesk=ar_detail_row,
        )
        op_label = _DETAIL_STATUS_INTERNAL_LABEL.get(cat, "Outros")
        app_status_txt = (
            _tracking_item_inline_status_raw(itd, ar_app_segment=ar_detail_row) if has_segment else ""
        )
        car_raw = _tracking_display_carrier(itd) if has_segment else ""
        car_disp = car_raw if car_raw else "—"
        car_ar = _ar_canonical_carrier(car_raw)
        dtxt = _tracking_app_duracion_text(itd) if has_segment else ""
        ttr_fmt = dtxt if dtxt else _format_ttr_hours_compact(ttr)
        rows.append(
            {
                "ticket_id": tid,
                "encomenda_n": idx,
                "codigo_rastreio": display_code,
                "transportadora": car_disp,
                "transportadora_ar": car_ar,
                "created_at": ca if ca is not None and str(ca).strip() else "—",
                "finalizado_em": co if cos else "—",
                "duracion_app": dtxt or "—",
                "ttr_horas": round(ttr, 2) if ttr is not None else None,
                "situacao": situacao,
                "status_operacional": op_label,
                "status_zendesk": raw_z,
                "status_app_json": app_status_txt,
                "guru": guru,
                "ttr_formatado": ttr_fmt,
            }
        )

    if status_pairs:
        emitted_norm: set[str] = set()
        for i, (scode, sval) in enumerate(status_pairs, start=1):
            it_match = _find_tracking_item_for_code(items, scode)
            display_code = str(scode).strip()
            _append_row(i, display_code, it_match, sval)
            emitted_norm.add(_norm_tracking_code_key(scode))
        next_i = len(rows) + 1
        for it in items:
            code = _tracking_display_code(it) or ""
            if not code:
                continue
            if _norm_tracking_code_key(code) in emitted_norm:
                continue
            _append_row(next_i, code, it, None)
            next_i += 1
    else:
        idx = 0
        for it in items:
            code = _tracking_display_code(it) or ""
            if not for_argentina_tab and not str(code).strip():
                continue
            idx += 1
            disp_code = str(code).strip() if str(code).strip() else "(sem código)"
            raw_map = _lookup_status_rastreamento_value(ex, nx, code)
            _append_row(idx, disp_code, it, raw_map, ar_detail_row=for_argentina_tab)

    if not rows and for_argentina_tab and _ar_raw_tracking_field_nonempty(tracking_raw):
        blob = _ar_format_preview_payload(tracking_raw, max_chars=12000).strip()
        if blob:
            if len(blob) >= 11900:
                blob = blob[:11900] + "\n… (truncado para a tabela; ver campo no Zendesk)"
            rows.append(
                {
                    "ticket_id": tid,
                    "encomenda_n": 1,
                    "codigo_rastreio": "(JSON bruto — app AR)",
                    "transportadora": "—",
                    "transportadora_ar": "—",
                    "created_at": "—",
                    "finalizado_em": "—",
                    "duracion_app": "—",
                    "ttr_horas": None,
                    "situacao": "Em aberto",
                    "status_operacional": "",
                    "status_zendesk": blob,
                    "status_app_json": "",
                    "guru": "—",
                    "ttr_formatado": "—",
                }
            )

    return pd.DataFrame(rows)


_NE_TBL_DASH = "-"


def _ne_dash_cell(val: object) -> str:
    """Texto para células da tabela: traço quando não há informação."""
    if val is None:
        return _NE_TBL_DASH
    try:
        if pd.api.types.is_scalar(val) and pd.isna(val):
            return _NE_TBL_DASH
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if not s or s in ("—", "–", "None", "nan", "NaN"):
        return _NE_TBL_DASH
    return s


def _detail_df_fallback_lines(ticket_id: str, n_lines: int) -> pd.DataFrame:
    """Linhas com '-' quando não há JSON parseável; `n_lines` vem de `total_qtd_rastreio` (mín. 1)."""
    tid = _norm_ticket_id(ticket_id)
    n = max(1, min(int(n_lines), 500))
    rows: list[dict[str, object]] = []
    for i in range(1, n + 1):
        code = f"{_NE_TBL_DASH} ({i}/{n})" if n > 1 else _NE_TBL_DASH
        rows.append(
            {
                "ticket_id": tid,
                "encomenda_n": i,
                "codigo_rastreio": code,
                "transportadora": _NE_TBL_DASH,
                "transportadora_ar": _AR_CARRIER_OTHER,
                "created_at": _NE_TBL_DASH,
                "finalizado_em": _NE_TBL_DASH,
                "duracion_app": _NE_TBL_DASH,
                "ttr_horas": None,
                "situacao": _NE_TBL_DASH,
                "status_operacional": _NE_TBL_DASH,
                "status_zendesk": "",
                "status_app_json": "",
                "guru": _NE_TBL_DASH,
                "ttr_formatado": _NE_TBL_DASH,
            }
        )
    return pd.DataFrame(rows)


def _app_tracking_ttr_stats(df: pd.DataFrame) -> tuple[pd.Series, int, int]:
    """(série horas TTR só concluídos, total de códigos no JSON, quantidade com TTR válido)."""
    if "tracking_numbers_data" not in df.columns:
        return pd.Series(dtype=float), 0, 0
    hours: list[float] = []
    n_lines = 0
    for raw in df["tracking_numbers_data"]:
        for it in _parse_tracking_numbers_app_json(raw):
            n_lines += 1
            ca = it.get("createdAt") or it.get("created_at")
            co = _tracking_segment_end_timestamp(it)
            h = _tracking_app_ttr_hours_resolved(ca, co)
            if h is not None:
                hours.append(h)
    return pd.Series(hours, dtype=float), n_lines, len(hours)


def _norm_ticket_id(raw: object) -> str:
    s = str(raw).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
    except ValueError:
        pass
    return s


def _query_param_first(key: str) -> str | None:
    qp = st.query_params
    if key not in qp:
        return None
    v = qp[key]
    if isinstance(v, list):
        return str(v[0]).strip() if v else None
    return str(v).strip() if v is not None else None


@st.cache_data(show_spinner=False, ttl=900)
def _cached_amostra_ticket_map(json_abs_path: str, file_mtime: float) -> dict[str, dict]:
    """Índice ticket_id normalizado → objeto ticket do JSON da amostra.

    file_mtime entra na chave de cache para refletir edições no arquivo sem reiniciar o app.
    **TTL 15 min:** dados do Databricks vêm em ``session_state`` (sem ``cache_data``); este cache
    cobre só o JSON de **amostra** local. Use **Limpar cache Streamlit** na barra lateral para forçar releitura.
    """
    p = Path(json_abs_path)
    if not p.is_file():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    tickets = data.get("tickets")
    if not isinstance(tickets, list):
        return {}
    out: dict[str, dict] = {}
    for t in tickets:
        if not isinstance(t, dict):
            continue
        tid = t.get("ticket_id")
        if tid is None:
            continue
        k = _norm_ticket_id(tid)
        out[k] = t
        out[str(tid).strip()] = t
    return out


def _resolve_amostra_json_path(cfg: dict) -> Path | None:
    rel = str(cfg.get("amostra_json_path") or "").strip()
    if not rel:
        return None
    p = (_NE_REPO_ROOT / rel).resolve()
    return p if p.is_file() else None


def _chart_ticket_href(ticket_id: str, cfg: dict, tab_key: str) -> str | None:
    """Link ao clicar na barra: mesma app (?ne_ticket=&ne_tab=) se houver amostra configurada; senão Zendesk.

    Se o clique não funcionar no deploy (iframe), defina **dashboard_base_url** no JSON de config
    (ex.: https://seu-app.streamlit.app) para gerar URL absoluta.
    """
    tid = _norm_ticket_id(ticket_id)
    rel = str(cfg.get("amostra_json_path") or "").strip()
    if rel:
        q = "?" + urllib.parse.urlencode({"ne_ticket": tid, "ne_tab": tab_key})
        base = str(cfg.get("dashboard_base_url") or "").strip().rstrip("/")
        if base:
            return f"{base}{q}"
        return q
    tmpl = str(cfg.get("zendesk_ticket_url_template") or "").strip()
    if tmpl and "{ticket_id}" in tmpl:
        return tmpl.format(ticket_id=tid)
    return None


def _ticket_id_from_vega_selection(evt: object | None, param_name: str) -> str | None:
    """Lê ticket_id do retorno de st.altair_chart(..., on_select='rerun')."""
    if evt is None:
        return None
    try:
        sel = evt.selection if hasattr(evt, "selection") else evt.get("selection")  # type: ignore[union-attr]
    except Exception:
        return None
    if sel is None:
        return None
    try:
        blk = getattr(sel, param_name, None)
        if blk is None and hasattr(sel, "get"):
            blk = sel.get(param_name)  # type: ignore[union-attr]
    except Exception:
        blk = None
    if blk is None:
        return None
    if isinstance(blk, list):
        if not blk:
            return None
        row0 = blk[0]
        if isinstance(row0, dict) and row0.get("ticket_id") is not None:
            v = row0["ticket_id"]
            if isinstance(v, list) and v:
                return _norm_ticket_id(v[0])
            return _norm_ticket_id(v)
        return None
    if isinstance(blk, dict):
        v = blk.get("ticket_id")
        if v is None:
            return None
        if isinstance(v, list):
            if not v:
                return None
            return _norm_ticket_id(v[0])
        return _norm_ticket_id(v)
    return None


def _format_messages_preview(ticket: dict, max_chars: int = 12_000) -> str:
    msgs = ticket.get("messages")
    if not isinstance(msgs, list):
        return "(sem mensagens no JSON)"
    parts: list[str] = []
    for i, m in enumerate(msgs):
        if not isinstance(m, dict):
            continue
        author = str(m.get("author") or "—")
        role = str(m.get("role") or "")
        created = str(m.get("created_at") or "")
        text = str(m.get("text") or "")
        parts.append(f"--- [{i + 1}] {created} | {author} ({role})\n{text}")
    blob = "\n\n".join(parts)
    if len(blob) > max_chars:
        return blob[:max_chars] + "\n\n… (texto truncado; abra o JSON completo se precisar)"
    return blob


def _render_amostra_ticket_panel(raw_cfg: dict, ticket_id: str) -> None:
    """Painel ‘visão geral’ do ticket a partir da amostra (ne_ticket + ne_tab na URL)."""
    tid = _norm_ticket_id(ticket_id)
    tab = _query_param_first("ne_tab") or "brasil"
    try:
        cfg = ne.effective_config_for_tab(raw_cfg, tab)
    except (KeyError, ValueError):
        cfg = raw_cfg
    path = _resolve_amostra_json_path(cfg)
    st.markdown(
        f'<div style="border-left:4px solid {_NE_ACCENT};padding:0.75rem 1rem;margin:0.5rem 0 1rem 0;'
        f'background:#f6f9ff;border-radius:8px;">'
        f"<strong>Visão geral na amostra</strong> · ticket <code>{html.escape(tid)}</code></div>",
        unsafe_allow_html=True,
    )
    _, c2 = st.columns([4, 1])
    with c2:
        if st.button("Fechar", key="ne_close_ticket_panel", help="Remove o filtro do ticket na URL"):
            for key in ("ne_ticket", "ne_codes"):
                if key in st.query_params:
                    del st.query_params[key]
            st.rerun()
    if path is None:
        rel = str(cfg.get("amostra_json_path") or "").strip()
        st.warning(
            "Configure **amostra_json_path** em `nuvem_envio_rastreio_config.json` "
            f"(caminho relativo à raiz deste projeto). Valor atual: `{rel or '(vazio)'}`."
        )
        zurl = str(cfg.get("zendesk_ticket_url_template") or "").strip()
        if zurl and "{ticket_id}" in zurl:
            st.link_button("Abrir no Zendesk (agente)", zurl.format(ticket_id=tid))
        return

    tmap = _cached_amostra_ticket_map(str(path), path.stat().st_mtime)
    ticket = tmap.get(tid) or tmap.get(str(ticket_id).strip())
    if not ticket:
        st.error(
            f"Ticket {tid!r} não está nesta amostra ({path.name!r}). "
            "Confira a semana/arquivo ou o ID."
        )
        zurl = str(cfg.get("zendesk_ticket_url_template") or "").strip()
        if zurl and "{ticket_id}" in zurl:
            st.link_button("Abrir no Zendesk (agente)", zurl.format(ticket_id=tid))
        return

    meta = {k: ticket.get(k) for k in ("Amostra", "Country", "Tema", "guru_name", "Group") if ticket.get(k)}
    if meta:
        st.markdown(
            " · ".join(f"**{html.escape(str(k))}:** {html.escape(str(v))}" for k, v in meta.items()),
            unsafe_allow_html=True,
        )
    nmsg = len(ticket["messages"]) if isinstance(ticket.get("messages"), list) else 0
    st.caption(f"{nmsg} mensagem(ns) no JSON da amostra.")

    preview = _format_messages_preview(ticket)
    st.text_area(
        "Transcrição (amostra)",
        value=preview,
        height=min(520, 120 + preview.count("\n") * 18),
        disabled=True,
        key=f"ne_amostra_tx_{tab}_{tid}",
        label_visibility="visible",
    )
    zurl = str(cfg.get("zendesk_ticket_url_template") or "").strip()
    if zurl and "{ticket_id}" in zurl:
        st.link_button("Abrir ticket no Zendesk (agente)", zurl.format(ticket_id=tid))


def _df_row_for_ticket_id(df: pd.DataFrame | None, tid: str) -> pd.Series | None:
    if df is None or df.empty or "ticket_id" not in df.columns:
        return None
    want = _norm_ticket_id(tid)
    norm = df["ticket_id"].astype(str).map(_norm_ticket_id)
    m = norm == want
    if not bool(m.any()):
        return None
    return df.loc[m].iloc[0]


_NE_DETAIL_SEL_NONE = "— Nenhum painel —"


def _ne_debug_write_tracking_raw(raw: object, *, max_chars: int = 100_000) -> None:
    """Debug: mostra o valor exato de ``tracking_numbers_data`` na linha do DataFrame (Databricks)."""
    if raw is None:
        st.caption("`tracking_numbers_data` é **null** nesta linha.")
        return
    try:
        if pd.api.types.is_scalar(raw) and pd.isna(raw):
            st.caption("`tracking_numbers_data` é **NaN** nesta linha.")
            return
    except (TypeError, ValueError):
        pass
    try:
        if isinstance(raw, (dict, list)):
            st.json(raw)
            return
    except Exception as exc:
        st.caption(f"Não foi possível usar ``st.json`` ({exc!s}); segue texto.")
    try:
        blob = _ar_format_preview_payload(raw, max_chars=max_chars).strip()
        if blob.startswith("{") or blob.startswith("["):
            st.code(blob, language="json")
        else:
            st.write(blob if len(blob) <= 8000 else blob[:8000] + "\n… (truncado)")
    except Exception:
        st.write(str(raw)[:max_chars])


def _ne_status_display_cells(detail_df: pd.DataFrame, *, tab: str) -> list[str]:
    """Texto da coluna Status na tabela do painel; Argentina prioriza o ``status`` literal do JSON do app."""
    merged: list[str] = []
    for _z, _o in zip(
        detail_df["status_zendesk"].tolist(),
        detail_df["status_operacional"].tolist(),
    ):
        _zs = _ne_dash_cell(_z)
        _os = _ne_dash_cell(_o)
        if _zs == _NE_TBL_DASH and _os == _NE_TBL_DASH:
            merged.append(_NE_TBL_DASH)
        elif _zs == _NE_TBL_DASH:
            merged.append(_os)
        elif _os == _NE_TBL_DASH or _zs == _os:
            merged.append(_zs)
        else:
            merged.append(f"{_zs} | {_os}")
    if tab != "argentina" or "status_app_json" not in detail_df.columns:
        return merged
    out: list[str] = []
    for app_s, m in zip(
        detail_df["status_app_json"].fillna("").astype(str).str.strip().tolist(),
        merged,
    ):
        low = app_s.lower()
        if app_s and low not in ("null", "none", "nan", "{}"):
            out.append(app_s)
        else:
            out.append(m)
    return out


def _ticket_ids_with_tracking(df: pd.DataFrame | None) -> list[str]:
    """Tickets com detalhe possível: `status_rastreamento` (código→status) e/ou `tracking_numbers_data`."""
    if df is None or df.empty or "ticket_id" not in df.columns:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for _, r in df.iterrows():
        has_tr = False
        if "tracking_numbers_data" in r.index:
            has_tr = bool(_parse_tracking_numbers_app_json(r.get("tracking_numbers_data")))
        has_st = False
        if "status_rastreamento" in r.index:
            has_st = bool(_parse_status_rastreamento_items(r.get("status_rastreamento")))
        if not has_tr and not has_st:
            continue
        x = _norm_ticket_id(r["ticket_id"])
        if x in seen:
            continue
        seen.add(x)
        out.append(x)

    def _sort_key(z: str) -> tuple[int, str]:
        if z.isdigit():
            return (0, z.zfill(24))
        return (1, z)

    return sorted(out, key=_sort_key)


def _ticket_ids_for_detail_select(df: pd.DataFrame | None, tab_key: str) -> list[str]:
    """Lista de tickets no seletor / painel de códigos: na Argentina, todos os da tabela (df); no Brasil, só com parse."""
    if df is None or df.empty or "ticket_id" not in df.columns:
        return []
    if tab_key == "argentina":
        seen: set[str] = set()
        out: list[str] = []
        for _, r in df.iterrows():
            x = _norm_ticket_id(r["ticket_id"])
            if not x or x in seen:
                continue
            seen.add(x)
            out.append(x)

        def _sk(z: str) -> tuple[int, str]:
            if z.isdigit():
                return (0, z.zfill(24))
            return (1, z)

        return sorted(out, key=_sk)
    return _ticket_ids_with_tracking(df)


def _render_ticket_codes_guru_panel(raw_cfg: dict, ticket_id: str, tab_key: str) -> None:
    """Painel após clique no gráfico de status: códigos + guru (agentName) por linha do JSON."""
    tid = _norm_ticket_id(ticket_id)
    tab = tab_key if tab_key in ("brasil", "argentina") else "brasil"
    try:
        cfg = ne.effective_config_for_tab(raw_cfg, tab)
    except (KeyError, ValueError):
        tab = "brasil"
        cfg = raw_cfg

    st.markdown(
        f'<div style="border-left:4px solid {_NE_ACCENT};padding:0.75rem 1rem;margin:0.5rem 0 1rem 0;'
        f'background:#f0fdf4;border-radius:8px;">'
        f"<strong>Códigos de rastreio no ticket</strong> · <code>{html.escape(tid)}</code> "
        f"· aba <strong>{html.escape(tab)}</strong></div>",
        unsafe_allow_html=True,
    )
    _, c2 = st.columns([4, 1])
    with c2:
        if st.button(
            "Fechar",
            key=f"ne_close_codes_{tab}",
            help="Fecha o painel sem recarregar a página inteira (mantém login e dados carregados).",
        ):
            st.session_state.pop("ne_codes_ticket", None)
            st.session_state.pop("ne_codes_tab", None)
            st.session_state[f"ne_chart_reset_{tab}"] = int(
                st.session_state.get(f"ne_chart_reset_{tab}", 0)
            ) + 1
            for _t in ("brasil", "argentina"):
                st.session_state[f"ne_ticket_codes_select_{_t}"] = _NE_DETAIL_SEL_NONE
            st.rerun()

    sk_df = f"ne_df_{tab}"
    df = st.session_state.get(sk_df)
    row = _df_row_for_ticket_id(df, tid)
    if row is None:
        st.warning(
            "Este ticket **não está** nos dados já carregados nesta aba. "
            "Abra a aba correta (**Brasil** ou **Argentina**), clique em **Atualizar dados** e clique de novo na barra do gráfico."
        )
        zurl = str(cfg.get("zendesk_ticket_url_template") or "").strip()
        if zurl and "{ticket_id}" in zurl:
            st.link_button("Abrir no Zendesk (agente)", zurl.format(ticket_id=tid))
        return

    if "tracking_numbers_data" not in row.index and "status_rastreamento" not in row.index:
        st.error("Não há **tracking_numbers_data** nem **status_rastreamento** neste recorte.")
        return

    _tr_raw = row.get("tracking_numbers_data") if "tracking_numbers_data" in row.index else None
    _st_raw = row.get("status_rastreamento") if "status_rastreamento" in row.index else None
    detail_df = flatten_tracking_numbers_data_detail(
        _tr_raw,
        tid,
        _st_raw,
        for_argentina_tab=(tab == "argentina"),
    )
    _n_fallback = 0
    if "total_qtd_rastreio" in row.index:
        try:
            _n_fallback = int(pd.to_numeric(row.get("total_qtd_rastreio"), errors="coerce") or 0)
        except (TypeError, ValueError):
            _n_fallback = 0
    if detail_df.empty:
        detail_df = _detail_df_fallback_lines(tid, _n_fallback if _n_fallback > 0 else 1)
        st.caption(
            "Não foi possível ler **tracking_numbers_data** / **status_rastreamento**: "
            "a tabela abaixo usa **-** nos campos sem dado. "
            + (
                f"Quantidade de linhas alinhada a **total_qtd_rastreio** ({_n_fallback})."
                if _n_fallback > 0
                else "Uma linha única com traços (sem total no recorte)."
            )
        )

    _cand = _ticket_ids_for_detail_select(df, tab)
    if len(_cand) > 1:
        _psk = f"ne_panel_ticket_switch_{tab}"
        try:
            _pidx = _cand.index(tid)
        except ValueError:
            _pidx = 0
        _alt = st.selectbox(
            "Trocar ticket (mesmos dados carregados)",
            options=_cand,
            index=_pidx,
            key=_psk,
            help="Tickets com **status_rastreamento** (código→status) e/ou **tracking_numbers_data** nesta aba.",
        )
        if _norm_ticket_id(str(_alt)) != tid:
            st.session_state["ne_codes_ticket"] = _norm_ticket_id(str(_alt))
            st.session_state["ne_codes_tab"] = tab
            st.session_state["_ne_sync_pick_widget"] = tab
            st.rerun()

    if "tracking_numbers_data" in row.index:
        with st.expander(
            "Debug: JSON bruto (`tracking_numbers_data` nesta linha do Databricks)",
            expanded=False,
        ):
            st.caption(
                "Compare com o custom field no Zendesk. A consulta SQL **não** passa por "
                "``@st.cache_data``; o que importa para dados “velhos” é o **replicado no lakehouse**."
            )
            _ne_debug_write_tracking_raw(_tr_raw)

    with st.expander("Tabela: código, guru, TTR, transportadora, status", expanded=True):
        _status_cells = _ne_status_display_cells(detail_df, tab=tab)
        _disp = pd.DataFrame(
            {
                "Código": detail_df["codigo_rastreio"].map(_ne_dash_cell),
                "Guru": detail_df["guru"].map(_ne_dash_cell),
                "TTR": detail_df["ttr_formatado"].map(_ne_dash_cell),
                "Transportadora": detail_df["transportadora"].map(_ne_dash_cell),
                "Status": _status_cells,
            }
        )
        # Só texto — evita falha na serialização Arrow; sem ``disabled=`` (não existe na API atual).
        _disp = _disp.astype(str)
        st.dataframe(
            _disp,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Código": st.column_config.TextColumn("Código", width="large"),
                "Guru": st.column_config.TextColumn("Guru", width="medium"),
                "TTR": st.column_config.TextColumn(
                    "TTR",
                    help="Tempo até resolução quando há datas no JSON de rastreio.",
                ),
                "Transportadora": st.column_config.TextColumn("Transportadora", width="medium"),
                "Status": st.column_config.TextColumn(
                    "Status",
                    help="Valor do Zendesk e/ou categoria agrupada.",
                    width="large",
                ),
            },
        )

    zurl = str(cfg.get("zendesk_ticket_url_template") or "").strip()
    if zurl and "{ticket_id}" in zurl:
        st.link_button("Abrir ticket no Zendesk (agente)", zurl.format(ticket_id=tid))


def _css_header_toolbar_ne() -> str:
    """Deploy, menu ⋯ e ícones do header Streamlit (fundo branco)."""
    a = _NE_ACCENT
    return f"""
        [data-testid="stHeader"] button,
        [data-testid="stHeader"] [data-baseweb="button"],
        [data-testid="stHeader"] [role="button"] {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
            background-color: transparent !important;
            border: 1px solid #d1dceb !important;
        }}
        [data-testid="stHeader"] button:hover,
        [data-testid="stHeader"] [data-baseweb="button"]:hover,
        [data-testid="stHeader"] [role="button"]:hover {{
            background-color: #f0f6ff !important;
            border-color: {a} !important;
        }}
        [data-testid="stHeader"] a {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        [data-testid="stHeader"] svg,
        [data-testid="stHeader"] svg path,
        [data-testid="stHeader"] svg line,
        [data-testid="stHeader"] svg polyline,
        [data-testid="stHeader"] svg circle {{
            fill: {a} !important;
            stroke: {a} !important;
        }}
        [data-testid="stToolbar"] button,
        [data-testid="stToolbar"] [data-baseweb="button"] {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        [data-testid="stToolbar"] svg,
        [data-testid="stToolbar"] svg path {{
            fill: {a} !important;
            stroke: {a} !important;
        }}
    """


def _css_running_truck_ne() -> str:
    """Substitui o boneco 'Running…' do header e o anel do st.spinner por caminhão animado (azul marca)."""
    uri = _ne_truck_icon_data_uri()
    return f"""
        @keyframes ne-truck-drive {{
            0% {{ margin-left: 0; }}
            50% {{ margin-left: 14px; }}
            100% {{ margin-left: 0; }}
        }}
        /* Header Streamlit: ícone Running */
        [data-testid="stStatusWidget"] {{
            position: relative !important;
            min-width: 2.1rem;
            min-height: 2.1rem;
        }}
        [data-testid="stStatusWidget"] img {{
            opacity: 0 !important;
            pointer-events: none;
        }}
        [data-testid="stStatusWidget"]::after {{
            content: "";
            position: absolute;
            left: 0.05rem;
            top: 50%;
            transform: translateY(-50%);
            width: 1.45rem;
            height: 1.1rem;
            background: url("{uri}") center / contain no-repeat;
            animation: ne-truck-drive 0.95s ease-in-out infinite;
            pointer-events: none;
        }}
        /* st.spinner: anel → caminhão no mesmo espaço */
        [data-testid="stSpinner"] > div {{
            position: relative;
        }}
        [data-testid="stSpinner"] > div > div:first-of-type {{
            opacity: 0 !important;
            border-color: transparent !important;
            border-top-color: transparent !important;
            animation: none !important;
            background: transparent !important;
        }}
        [data-testid="stSpinner"] > div > div:first-of-type::after {{
            content: "";
            opacity: 1 !important;
            position: absolute;
            left: 0;
            top: 50%;
            margin-top: -0.55rem;
            width: 1.35rem;
            height: 1.05rem;
            background: url("{uri}") center / contain no-repeat;
            animation: ne-truck-drive 0.95s ease-in-out infinite;
            pointer-events: none;
        }}
    """


def _css_filter_labels_ne() -> str:
    """Seletores para rótulos de filtros (Chrome / Streamlit 1.4x — .main muitas vezes não envolve os widgets)."""
    a = _NE_ACCENT
    return f"""
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stWidgetLabel"],
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stWidgetLabel"] p,
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stWidgetLabel"] span,
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stWidgetLabel"] label,
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stWidgetLabel"] div,
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stWidgetLabel"] small,
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stWidgetLabel"] strong,
        html body [data-testid="stAppViewContainer"] section[data-testid="stMain"] [data-testid="stWidgetLabel"],
        html body [data-testid="stAppViewContainer"] section[data-testid="stMain"] [data-testid="stWidgetLabel"] p,
        html body [data-testid="stAppViewContainer"] section[data-testid="stMain"] [data-testid="stWidgetLabel"] span,
        html body [data-testid="stAppViewContainer"] section[data-testid="stMain"] [data-testid="stWidgetLabel"] label,
        html body [data-testid="stAppViewContainer"] section[data-testid="stMain"] [data-testid="stWidgetLabel"] div,
        html body [data-testid="stAppViewContainer"] [data-testid="stMain"] [data-testid="stWidgetLabel"],
        html body [data-testid="stAppViewContainer"] [data-testid="stMain"] [data-testid="stWidgetLabel"] p,
        html body [data-testid="stAppViewContainer"] [data-testid="stMain"] [data-testid="stWidgetLabel"] span,
        html body [data-testid="stAppViewContainer"] [data-testid="stMain"] [data-testid="stWidgetLabel"] label,
        html body [data-testid="stAppViewContainer"] [data-testid="stMain"] [data-testid="stWidgetLabel"] div,
        html body [data-testid="stAppViewContainer"] section.main [data-testid="stWidgetLabel"],
        html body [data-testid="stAppViewContainer"] section.main [data-testid="stWidgetLabel"] p,
        html body [data-testid="stAppViewContainer"] section.main [data-testid="stWidgetLabel"] span,
        html body [data-testid="stAppViewContainer"] section.main [data-testid="stWidgetLabel"] label,
        html body [data-testid="stAppViewContainer"] section.main [data-testid="stWidgetLabel"] div,
        html body [data-testid="stAppViewContainer"] main [data-testid="stWidgetLabel"],
        html body [data-testid="stAppViewContainer"] main [data-testid="stWidgetLabel"] p,
        html body [data-testid="stAppViewContainer"] main [data-testid="stWidgetLabel"] span,
        html body [data-testid="stAppViewContainer"] main [data-testid="stWidgetLabel"] label,
        html body [data-testid="stAppViewContainer"] main [data-testid="stWidgetLabel"] div,
        html body [data-testid="stAppViewContainer"] [data-testid="column"] [data-testid="stWidgetLabel"],
        html body [data-testid="stAppViewContainer"] [data-testid="column"] [data-testid="stWidgetLabel"] p,
        html body [data-testid="stAppViewContainer"] [data-testid="column"] [data-testid="stWidgetLabel"] span,
        html body [data-testid="stAppViewContainer"] [data-testid="column"] [data-testid="stWidgetLabel"] label,
        html body [data-testid="stAppViewContainer"] [data-testid="column"] [data-testid="stWidgetLabel"] div {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
            caret-color: {a} !important;
            font-weight: 600 !important;
            opacity: 1 !important;
        }}
        /* Qualquer texto dentro do rótulo (Chrome / markdown no label) */
        html body [data-testid="stAppViewContainer"] [data-testid="stWidgetLabel"] *:not(svg):not(path):not(circle):not(rect) {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
    """


def _css_all_text_blue_ne() -> str:
    """Quase todo o texto da app em azul tema; exceção: botão primário (branco sobre azul)."""
    a = _NE_ACCENT
    return f"""
        [data-testid="stAppViewContainer"],
        .stApp {{
            --st-text-color: {a} !important;
        }}
        html body [data-testid="stAppViewContainer"] .block-container,
        html body [data-testid="stAppViewContainer"] .block-container p,
        html body [data-testid="stAppViewContainer"] .block-container span,
        html body [data-testid="stAppViewContainer"] .block-container li,
        html body [data-testid="stAppViewContainer"] .block-container div,
        html body [data-testid="stAppViewContainer"] .block-container label,
        html body [data-testid="stAppViewContainer"] .block-container td,
        html body [data-testid="stAppViewContainer"] .block-container th,
        html body [data-testid="stAppViewContainer"] .block-container caption,
        html body [data-testid="stAppViewContainer"] .block-container a,
        html body [data-testid="stAppViewContainer"] .block-container h1,
        html body [data-testid="stAppViewContainer"] .block-container h2,
        html body [data-testid="stAppViewContainer"] .block-container h3,
        html body [data-testid="stAppViewContainer"] .block-container h4,
        html body [data-testid="stAppViewContainer"] .block-container small,
        html body [data-testid="stAppViewContainer"] .block-container strong,
        html body [data-testid="stAppViewContainer"] .block-container em,
        section[data-testid="stSidebar"] .block-container,
        section[data-testid="stSidebar"] .block-container p,
        section[data-testid="stSidebar"] .block-container span,
        section[data-testid="stSidebar"] .block-container li,
        section[data-testid="stSidebar"] .block-container div,
        section[data-testid="stSidebar"] .block-container label {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        [data-testid="stMetricValue"],
        [data-testid="stMetricDelta"] {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        [data-testid="stCaptionContainer"] p,
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] span,
        [data-testid="stMarkdownContainer"] li,
        [data-testid="stMarkdownContainer"] a {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        .block-container input,
        .block-container textarea,
        .block-container [data-baseweb="input"] input,
        .block-container [data-baseweb="select"] div[class*="value"] {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        .block-container input::placeholder,
        .block-container textarea::placeholder {{
            color: {a} !important;
            opacity: 0.5 !important;
            -webkit-text-fill-color: {a} !important;
        }}
        [data-baseweb="popover"] li,
        [data-baseweb="popover"] span,
        [data-baseweb="menu"] li,
        [data-baseweb="calendar"] button,
        [data-baseweb="calendar"] span {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        div[data-testid="stAlert"] p,
        div[data-testid="stAlert"] span,
        div[data-testid="stAlert"] div {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        .streamlit-expanderContent p,
        .streamlit-expanderContent li,
        .streamlit-expanderContent span {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        /* Gráficos Altair: rótulos em SVG */
        .block-container .vega-embed text {{
            fill: {a} !important;
        }}
        /* Botão primário: branco (especificidade acima de .block-container div/p/span) */
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stButton"] button[kind="primary"],
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stButton"] button[kind="primary"] *,
        html body [data-testid="stAppViewContainer"] .block-container .stButton > button[kind="primary"],
        html body [data-testid="stAppViewContainer"] .block-container .stButton > button[kind="primary"] *,
        html body [data-testid="stAppViewContainer"] .main [data-testid="stButton"] button[kind="primary"],
        html body [data-testid="stAppViewContainer"] .main [data-testid="stButton"] button[kind="primary"] * {{
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }}
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stButton"] button[kind="primary"] svg,
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stButton"] button[kind="primary"] svg path,
        html body [data-testid="stAppViewContainer"] .block-container .stButton > button[kind="primary"] svg,
        html body [data-testid="stAppViewContainer"] .block-container .stButton > button[kind="primary"] svg path,
        html body [data-testid="stAppViewContainer"] .main [data-testid="stButton"] button[kind="primary"] svg,
        html body [data-testid="stAppViewContainer"] .main [data-testid="stButton"] button[kind="primary"] svg path {{
            fill: #ffffff !important;
            stroke: #ffffff !important;
        }}
    """


def _css_checkbox_ne() -> str:
    """Filtro 'só com rastreio': visual compacto e discreto (slate / cinza-claro)."""
    a = _NE_ACCENT
    muted = "#64748b"
    bg = "#f1f5f9"
    line = "#e2e8f0"
    return f"""
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] {{
            background-color: {bg} !important;
            padding: 0.28rem 0.5rem !important;
            border-radius: 6px !important;
            border: 1px solid {line} !important;
            box-shadow: none !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] label {{
            background-color: transparent !important;
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
            gap: 0.35rem !important;
            min-height: 0 !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stElementContainer"]:has([data-testid="stCheckbox"]),
        html body [data-testid="stAppViewContainer"] [data-testid="stVerticalBlock"]:has([data-testid="stCheckbox"]) {{
            background-color: transparent !important;
            background: transparent !important;
            box-shadow: none !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] label,
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] label *,
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] [data-testid="stMarkdownContainer"],
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] [data-testid="stMarkdownContainer"] p,
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] [data-testid="stMarkdownContainer"] span,
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] [data-testid="stMarkdownContainer"] li,
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] [data-testid="stMarkdownContainer"] div {{
            color: {muted} !important;
            -webkit-text-fill-color: {muted} !important;
            font-weight: 400 !important;
            font-size: 0.72rem !important;
            line-height: 1.25 !important;
            opacity: 1 !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] label a {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
            text-decoration: underline !important;
            font-size: 0.72rem !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stCheckbox"] [data-baseweb="checkbox"] {{
            transform: scale(0.78) !important;
            transform-origin: left center !important;
        }}
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"] > div {{
            border: 1px solid #cbd5e1 !important;
            background-color: #ffffff !important;
            box-shadow: none !important;
        }}
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"] input:focus + div,
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"]:focus-within > div {{
            border-color: #94a3b8 !important;
            box-shadow: 0 0 0 1px rgba(148, 163, 184, 0.35) !important;
        }}
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"] input:checked + div,
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"] [data-state="checked"] > div {{
            background-color: rgba(0, 80, 195, 0.1) !important;
            border-color: {a} !important;
        }}
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"] svg,
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"] svg path {{
            fill: {a} !important;
            stroke: {a} !important;
        }}
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"] input:not(:checked) ~ div svg,
        html body [data-testid="stCheckbox"] [data-baseweb="checkbox"] input:not(:checked) ~ div svg path {{
            opacity: 0 !important;
        }}
    """


def _css_filter_blocks_white_ne() -> str:
    """Chips do multiselect (blocos) com texto branco; campos de filtro legíveis no painel claro."""
    a = _NE_ACCENT
    return f"""
        /* Tags / chips do Status: fundo azul tema + escrita branca */
        .block-container [data-baseweb="tag"],
        [data-testid="stMultiSelect"] [data-baseweb="tag"] {{
            background-color: {a} !important;
            border-color: rgba(255,255,255,0.35) !important;
            color: #ffffff !important;
        }}
        .block-container [data-baseweb="tag"] span,
        .block-container [data-baseweb="tag"] p,
        [data-testid="stMultiSelect"] [data-baseweb="tag"] span {{
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }}
        .block-container [data-baseweb="tag"] [aria-label="Delete"],
        .block-container [data-baseweb="tag"] [aria-label="delete"],
        [data-testid="stMultiSelect"] [data-baseweb="tag"] [aria-label="Delete"] {{
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }}
        .block-container [data-baseweb="tag"] svg,
        .block-container [data-baseweb="tag"] svg path,
        [data-testid="stMultiSelect"] [data-baseweb="tag"] svg,
        [data-testid="stMultiSelect"] [data-baseweb="tag"] svg path {{
            fill: #ffffff !important;
            stroke: #ffffff !important;
        }}
        /* Área dos filtros: fundo claro + texto azul (datas, número, multiselect vazio) */
        .block-container [data-testid="stDateInput"] [data-baseweb="input"] > div,
        .block-container [data-testid="stNumberInput"] [data-baseweb="input"] > div,
        .block-container [data-testid="stMultiSelect"] [data-baseweb="select"] > div {{
            background-color: #ffffff !important;
            border-color: #b8cce8 !important;
        }}
        .block-container [data-testid="stDateInput"] input,
        .block-container [data-testid="stNumberInput"] input,
        .block-container [data-testid="stNumberInput"] [data-baseweb="input"] input {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
            background-color: transparent !important;
        }}
        /* Texto azul no select só em spans/ps que não estão dentro de chip (tag) */
        .block-container [data-testid="stMultiSelect"] [data-baseweb="select"] span:not(:is([data-baseweb="tag"] *)),
        .block-container [data-testid="stMultiSelect"] [data-baseweb="select"] p:not(:is([data-baseweb="tag"] *)) {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        .block-container [data-testid="stMultiSelect"] [data-baseweb="select"] input {{
            color: {a} !important;
            -webkit-text-fill-color: {a} !important;
        }}
        /* Reforço: chips com texto branco (ganha de .block-container … select span) */
        html body [data-testid="stAppViewContainer"] [data-testid="stMultiSelect"] [data-baseweb="tag"],
        html body [data-testid="stAppViewContainer"] [data-testid="stMultiSelect"] [data-baseweb="tag"] *,
        html body [data-testid="stAppViewContainer"] [data-testid="stMultiSelect"] [data-baseweb="tag"] span,
        html body [data-testid="stAppViewContainer"] [data-testid="stMultiSelect"] [data-baseweb="tag"] p,
        html body [data-testid="stAppViewContainer"] [data-testid="stMultiSelect"] [data-baseweb="tag"] div {{
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stMultiSelect"] [data-baseweb="tag"] svg,
        html body [data-testid="stAppViewContainer"] [data-testid="stMultiSelect"] [data-baseweb="tag"] svg path {{
            fill: #ffffff !important;
            stroke: #ffffff !important;
        }}
        /* Botões +/- do número: ícones azul tema */
        .block-container [data-testid="stNumberInput"] button,
        .block-container [data-testid="stNumberInput"] button svg,
        .block-container [data-testid="stNumberInput"] button svg path {{
            color: {a} !important;
            fill: {a} !important;
            stroke: {a} !important;
        }}
    """


def _inject_filter_widget_labels_priority() -> None:
    """Repete CSS depois dos widgets (cascata vs. Streamlit no head)."""
    st.markdown(
        f'<style id="ne-ui-priority">{_css_filter_labels_ne()}{_css_header_toolbar_ne()}{_css_running_truck_ne()}{_css_all_text_blue_ne()}{_css_checkbox_ne()}{_css_filter_blocks_white_ne()}{_css_country_tabs_ne()}</style>',
        unsafe_allow_html=True,
    )


def _inject_ne_theme() -> None:
    st.markdown(
        f"""
        <style>
        [data-testid="stAppViewContainer"] > .main {{
            background-color: #ffffff;
        }}
        [data-testid="stAppViewContainer"] {{
            background-color: #ffffff;
        }}
        [data-testid="stHeader"] {{
            background-color: rgba(255,255,255,0.96);
        }}
        {_css_header_toolbar_ne()}
        section[data-testid="stSidebar"] {{
            background-color: #ffffff;
            border-right: 1px solid #e8eef7;
        }}
        .block-container {{
            padding-top: 1.25rem;
            max-width: 100%;
        }}
        /* Score cards / métricas */
        [data-testid="stMetric"] {{
            background: #ffffff !important;
            border: 1px solid #e8eef7 !important;
            border-left: 4px solid {_NE_ACCENT} !important;
            border-radius: 12px !important;
            padding: 0.9rem 1rem !important;
            box-shadow: 0 2px 12px rgba(0, 80, 195, 0.07) !important;
        }}
        [data-testid="stMetricLabel"],
        [data-testid="stMetricLabel"] p {{
            color: {_NE_ACCENT} !important;
            font-weight: 600 !important;
        }}
        [data-testid="stMetricValue"] {{
            font-weight: 700 !important;
        }}
        [data-testid="stMetricDelta"] {{
            color: {_NE_ACCENT} !important;
        }}
        /* Botão primário (rótulo legível: mesma especificidade alta que o tema de texto azul) */
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stButton"] button[kind="primary"],
        html body [data-testid="stAppViewContainer"] .block-container .stButton > button[kind="primary"],
        html body [data-testid="stAppViewContainer"] .main [data-testid="stButton"] button[kind="primary"] {{
            background-color: {_NE_ACCENT} !important;
            border-color: {_NE_ACCENT} !important;
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
            font-weight: 600 !important;
        }}
        html body [data-testid="stAppViewContainer"] .block-container [data-testid="stButton"] button[kind="primary"]:hover,
        html body [data-testid="stAppViewContainer"] .block-container .stButton > button[kind="primary"]:hover,
        html body [data-testid="stAppViewContainer"] .main [data-testid="stButton"] button[kind="primary"]:hover {{
            background-color: {_NE_ACCENT_HOVER} !important;
            border-color: {_NE_ACCENT_HOVER} !important;
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }}
        /* st.form_submit_button(type="primary"): widget é stFormSubmitButton, NÃO stButton — kind no DOM é primaryFormSubmit */
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] button[kind="primaryFormSubmit"],
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] button[data-testid="stBaseButton-primaryFormSubmit"] {{
            background-color: {_NE_NS_BLUE} !important;
            border-color: {_NE_NS_BLUE} !important;
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
            font-weight: 700 !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] button[kind="primaryFormSubmit"]:hover,
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] button[data-testid="stBaseButton-primaryFormSubmit"]:hover {{
            background-color: {_NE_NS_BLUE_HOVER} !important;
            border-color: {_NE_NS_BLUE_HOVER} !important;
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] button[kind="primaryFormSubmit"]:active,
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] button[data-testid="stBaseButton-primaryFormSubmit"]:active {{
            background-color: {_NE_NS_BLUE_HOVER} !important;
            border-color: {_NE_NS_BLUE_HOVER} !important;
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }}
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] [data-testid="stMarkdownContainer"],
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] [data-testid="stMarkdownContainer"] p,
        html body [data-testid="stAppViewContainer"] [data-testid="stFormSubmitButton"] [data-testid="stMarkdownContainer"] span {{
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }}
        /* st.subheader → h3 */
        .main h3 {{
            color: {_NE_ACCENT} !important;
            font-weight: 700 !important;
            border-bottom: 2px solid #e8eef7;
            padding-bottom: 0.4rem;
            margin-top: 1.25rem;
        }}
        /* Alertas em tom da marca */
        div[data-testid="stSuccess"] {{
            background-color: #f0f6ff !important;
            border-left: 4px solid {_NE_ACCENT} !important;
        }}
        /* Rótulos de filtros (repetidos após os widgets via _inject_filter_widget_labels_priority) */
        {_css_filter_labels_ne()}
        /* Campos Base Web: borda azul clara, foco #0050c3 */
        .main div[data-baseweb="input"] > div,
        .main div[data-baseweb="select"] > div {{
            border-color: #b8cce8 !important;
            border-radius: 8px !important;
        }}
        .main div[data-baseweb="input"]:focus-within > div,
        .main div[data-baseweb="select"]:focus-within > div {{
            border-color: {_NE_ACCENT} !important;
            box-shadow: 0 0 0 1px {_NE_ACCENT} !important;
        }}
        /* Chips / campos de filtro: ver _css_filter_blocks_white_ne() */
        /* Expander / download: títulos alinhados à paleta */
        .main .streamlit-expanderHeader {{
            color: {_NE_ACCENT} !important;
            font-weight: 600 !important;
        }}
        .main .streamlit-expanderHeader svg {{
            fill: {_NE_ACCENT} !important;
        }}
        {_css_running_truck_ne()}
        {_css_all_text_blue_ne()}
        {_css_checkbox_ne()}
        {_css_filter_blocks_white_ne()}
        {_css_country_tabs_ne()}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _css_country_tabs_ne() -> str:
    """Abas Brasil / Argentina: centralizadas, tipografia maior, bandeiras no rótulo (emoji)."""
    a = html.escape(_NE_ACCENT)
    return f"""
        [data-testid="stAppViewContainer"] [data-testid="stTabs"] {{
            width: 100% !important;
            max-width: 100% !important;
        }}
        [data-testid="stAppViewContainer"] [data-testid="stTabs"] [data-baseweb="tab-list"],
        [data-testid="stAppViewContainer"] [data-testid="stTabs"] [role="tablist"] {{
            display: flex !important;
            justify-content: center !important;
            align-items: flex-end !important;
            flex-wrap: wrap !important;
            gap: 0.5rem 2.75rem !important;
            width: 100% !important;
            margin: 0 auto !important;
            padding: 0.75rem 1rem 1.15rem 1rem !important;
            box-sizing: border-box !important;
            border-bottom: 2px solid #e2e8f0 !important;
        }}
        [data-testid="stAppViewContainer"] [data-testid="stTabs"] [data-baseweb="tab"],
        [data-testid="stAppViewContainer"] [data-testid="stTabs"] [role="tab"] {{
            font-size: clamp(1.35rem, 4.2vw, 2.35rem) !important;
            font-weight: 800 !important;
            letter-spacing: 0.02em !important;
            padding: 0.55rem 0.35rem 0.85rem 0.35rem !important;
            min-height: 3.5rem !important;
            line-height: 1.25 !important;
        }}
        [data-testid="stAppViewContainer"] [data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"],
        [data-testid="stAppViewContainer"] [data-testid="stTabs"] [role="tab"][aria-selected="true"] {{
            color: {a} !important;
        }}
    """


def _ne_pick_qty_col(df: pd.DataFrame, base: str) -> str | None:
    """Prefere coluna *_num (inteiro) quando existir."""
    num = f"{base}_num"
    if num in df.columns:
        return num
    if base in df.columns:
        return base
    return None


def _ne_sample_column_order(df: pd.DataFrame, is_ar: bool) -> list[str]:
    """Colunas da tabela amostra: foco em ticket + rastreio, sem BU nem colunas técnicas extras."""
    out: list[str] = []
    for c in ("ticket_id", "status", "grupo"):
        if c in df.columns:
            out.append(c)
    if "total_qtd_rastreio" in df.columns:
        out.append("total_qtd_rastreio")
    if is_ar:
        for c in ("tracking_numbers_data", "status_rastreamento"):
            if c in df.columns:
                out.append(c)
    else:
        for base in (
            "quantidade_rastreio_correios",
            "quantidade_rastreio_jadlog",
            "quantidade_rastreio_loggi",
        ):
            picked = _ne_pick_qty_col(df, base)
            if picked:
                out.append(picked)
        for c in ("status_rastreamento", "tracking_numbers_data"):
            if c in df.columns:
                out.append(c)
    for c in ("created_at", "updated_at"):
        if c in df.columns:
            out.append(c)
    return out


def _ne_truncate_sample_text(val: object, max_len: int = 180) -> str:
    try:
        if val is None or (pd.api.types.is_scalar(val) and pd.isna(val)):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null", "{}"):
        return ""
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _ne_format_ticket_id_sample(val: object) -> str:
    """Ticket id estável na amostra (evita ``7284741.0`` e semelhantes)."""
    if val is None:
        return "—"
    try:
        if pd.api.types.is_scalar(val) and pd.isna(val):
            return "—"
    except (TypeError, ValueError):
        pass
    return _norm_ticket_id(val)


def _ne_format_grupo_sample(val: object) -> str:
    """Zendesk costuma devolver ``group_id`` como ``-1`` quando não há grupo; evita confusão na amostra."""
    try:
        if val is None or (pd.api.types.is_scalar(val) and pd.isna(val)):
            return "—"
    except (TypeError, ValueError):
        pass
    t = str(val).strip()
    if not t or t.lower() in ("nan", "none", "-1", "-1.0"):
        return "—"
    return t


def _ne_ts_series_brasilia(s: pd.Series) -> pd.Series:
    """Converte timestamps do lakehouse para America/Sao_Paulo (naive → assume UTC)."""
    ts = pd.to_datetime(s, utc=True, errors="coerce")
    try:
        return ts.dt.tz_convert(_NE_SAMPLE_TZ)
    except (TypeError, AttributeError):
        return ts


def _ne_prepare_sample_display_df(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    use = [c for c in cols if c in df.columns]
    # Evita serialização gigante no browser; CSV completo segue disponível no download.
    _cap = min(3000, max(400, len(df)))
    sub = df.loc[:, use].head(_cap).copy()
    if "ticket_id" in sub.columns:
        sub["ticket_id"] = sub["ticket_id"].map(_ne_format_ticket_id_sample)
    if "grupo" in sub.columns:
        sub["grupo"] = sub["grupo"].map(_ne_format_grupo_sample)
    for col in ("created_at", "updated_at"):
        if col in sub.columns:
            sub[col] = _ne_ts_series_brasilia(sub[col])
    for col in ("tracking_numbers_data", "status_rastreamento"):
        if col in sub.columns:
            sub[col] = sub[col].map(_ne_truncate_sample_text)
    return sub


def _ne_sample_column_config(cols: list[str]) -> dict[str, object]:
    """Rótulos amigáveis e tipos para a tabela amostra."""
    labels: dict[str, str] = {
        "ticket_id": "Ticket",
        "status": "Status do ticket",
        "grupo": "Grupo",
        "total_qtd_rastreio": "Total de códigos",
        "quantidade_rastreio_correios": "Correios",
        "quantidade_rastreio_correios_num": "Correios",
        "quantidade_rastreio_jadlog": "Jadlog",
        "quantidade_rastreio_jadlog_num": "Jadlog",
        "quantidade_rastreio_loggi": "Loggi",
        "quantidade_rastreio_loggi_num": "Loggi",
        "status_rastreamento": "Situação por código (JSON)",
        "tracking_numbers_data": "Dados de rastreio (JSON)",
        "created_at": "Criado em (Brasília)",
        "updated_at": "Atualizado em (Brasília)",
    }
    cfg: dict[str, object] = {}
    for c in cols:
        label = labels.get(c, c.replace("_", " ").title())
        if c in ("created_at", "updated_at"):
            cfg[c] = st.column_config.DatetimeColumn(
                label,
                format="DD/MM/YYYY HH:mm",
            )
        elif c == "total_qtd_rastreio" or c.endswith("_num") or "quantidade_rastreio" in c:
            cfg[c] = st.column_config.NumberColumn(
                label, format="%d", help="Quantidade na amostra"
            )
        elif c in ("tracking_numbers_data", "status_rastreamento"):
            cfg[c] = st.column_config.TextColumn(
                label,
                width="large",
                help="Texto abreviado na tela; exporte o CSV para o conteúdo completo.",
            )
        elif c == "ticket_id":
            cfg[c] = st.column_config.TextColumn(label, width="small")
        else:
            cfg[c] = st.column_config.TextColumn(label)
    return cfg


def _render_ne_country_tab(raw_cfg: dict, tab_key: str) -> None:
    """Conteúdo de uma aba (Brasil ou Argentina): filtros, métricas, gráficos, tabela, CSV."""
    cfg = ne.effective_config_for_tab(raw_cfg, tab_key)
    # Aba Argentina sempre usa métricas/JSON AR (rótulo Tracking Numbers, etc.), mesmo se
    # `data_model` no JSON estiver incorreto — antes caía no ramo Brasil ("Total de solicitações").
    is_ar = tab_key == "argentina"
    k = tab_key
    sk_df = f"ne_df_{k}"
    sk_meta = f"ne_meta_{k}"
    _err_key = f"ne_{k}_fetch_error"
    if _err_key in st.session_state:
        st.error(st.session_state.pop(_err_key))

    zf = cfg.get("zendesk_field_ids") or {}
    if is_ar:
        _req = ("tracking_numbers_data",)
    else:
        _req = ne.BR_ZENDESK_FIELD_KEYS
    missing = [x for x in _req if not str(zf.get(x) or "").strip()]
    if missing:
        st.warning(
            f"**[{k}]** Preencha os IDs em `nuvem_envio_rastreio_config.json` → `tabs.{k}` → "
            f"`zendesk_field_ids` para: {', '.join(missing)}."
        )

    if is_ar:
        _cf_opts = list(_AR_CARRIER_ORDER) + [_AR_CARRIER_OTHER]
        _cf_help = (
            "Após carregar: exibe só tickets com **pelo menos um** código dessa transportadora "
            "(JSON do app). Todas marcadas = sem filtro."
        )
    else:
        _cf_opts = list(_BR_CARRIER_FILTER_LABELS)
        _cf_help = (
            "Após carregar: exibe só tickets com **quantidade > 0** na transportadora. "
            "Várias = ticket entra se tiver volume em **qualquer uma**. Todas = sem filtro."
        )

    _sk_carrier = f"ne_{k}_carrier_filter"
    col1, col2, col3 = st.columns([1.05, 1.25, 1.45])
    with col1:
        periodo = st.selectbox(
            "Período",
            options=list(_NE_PERIOD_CHOICES),
            index=1,
            key=f"ne_{k}_periodo",
            help=(
                "Janela sobre **updated_at** do ticket no lakehouse, em **horário de Brasília**. "
                "“Mês atual” = do dia 1 até agora."
            ),
        )
    with col2:
        status_opts = ["new", "open", "pending", "hold", "solved", "closed"]
        statuses = st.multiselect(
            "Status do ticket",
            status_opts,
            default=status_opts,
            key=f"ne_{k}_statuses",
        )
    with col3:
        st.multiselect(
            "Transportadora",
            options=_cf_opts,
            default=list(_cf_opts),
            key=_sk_carrier,
            help=_cf_help,
        )

    _chk_help = (
        "Mantém só tickets que já têm dados de rastreio preenchidos (Argentina)."
        if is_ar
        else (
            "Mantém só tickets que já têm pedido de rastreio (transportadora ou situação de rastreio preenchidos)."
        )
    )
    _row_pad, _row_chk, _row_cache = st.columns([3.65, 1.05, 1.1])
    with _row_chk:
        somente_rastreio = st.checkbox(
            "Só com rastreio",
            value=bool(cfg.get("somente_com_rastreio_preenchido", True)),
            help=_chk_help,
            key=f"ne_{k}_somente",
        )
    with _row_cache:
        if st.button(
            "Limpar cache",
            key=f"ne_{k}_clear_cache",
            help=(
                "Chama ``st.cache_data.clear()`` (ex.: índice da **amostra JSON** local). "
                "Dados do Databricks dependem de **Atualizar dados**, não deste cache."
            ),
        ):
            st.cache_data.clear()
            st.success("Cache Streamlit limpo. Recarregue a amostra ou **Atualizar dados** se precisar.")

    _sk_pending = f"ne_{k}_pending_fetch"
    if st.button("Atualizar dados", type="primary", key=f"ne_{k}_btn_refresh"):
        if not statuses:
            st.error("Selecione ao menos um status.")
        else:
            _w0, _w1 = _ne_period_window_timestamps(periodo)
            st.session_state[_sk_pending] = {
                "window_start_ts": _w0,
                "window_end_ts": _w1,
                "periodo": periodo,
                "statuses": list(statuses),
                "somente_rastreio": somente_rastreio,
            }
            st.rerun()

    df_loaded = st.session_state.get(sk_df)
    if df_loaded is None or df_loaded.empty:
        st.info(
            "Escolha o **Período**, os filtros desejados e clique em **Atualizar dados** para carregar do Databricks "
            "(sem limite de linhas na consulta — o volume depende só do intervalo e dos status marcados)."
        )
        return

    _meta = st.session_state.get(sk_meta) or {}
    _per_lbl = str(_meta.get("periodo") or "").strip()
    _per_extra = f" · período carregado: **{_per_lbl}**" if _per_lbl else ""
    filtro_txt = " + só com rastreio preenchido" if _meta.get("somente_rastreio") else ""
    _country_lbl = "Argentina · [AR] Envio Nube" if is_ar else "Brasil · Nuvem Envio"

    _carrier_pick = st.session_state.get(_sk_carrier, list(_cf_opts))
    _eff_carriers = _carrier_pick if _carrier_pick else list(_cf_opts)
    if len(_eff_carriers) == len(_cf_opts):
        df = df_loaded
    else:
        df = (
            _ne_filter_df_ar_carriers(df_loaded, _eff_carriers)
            if is_ar
            else _ne_filter_df_br_carriers(df_loaded, _eff_carriers)
        )

    if len(df) == len(df_loaded):
        st.success(
            f"**{len(df)}** tickets ({_country_lbl}{filtro_txt}){_per_extra}. "
            "Ordenados por **total** (maior primeiro)."
        )
    else:
        st.success(
            f"**{len(df)}** tickets exibidos (de **{len(df_loaded)}** carregados) "
            f"com filtro de transportadora · {_country_lbl}{filtro_txt}{_per_extra}. "
            "Ordenados por **total** (maior primeiro)."
        )

    _lag_msg: list[str] = []
    if "updated_at" in df_loaded.columns:
        _u_mx = pd.to_datetime(df_loaded["updated_at"], utc=True, errors="coerce").max()
        if pd.notna(_u_mx):
            _lag_msg.append(
                f"Maior **updated_at** neste lote (após deduplicar por ticket): "
                f"**{_u_mx.tz_convert(_NE_SAMPLE_TZ).strftime('%d/%m/%Y %H:%M')} (Brasília)**."
            )
    try:
        _now_br = datetime.now(_NE_SAMPLE_TZ).strftime("%d/%m/%Y %H:%M")
        _lag_msg.append(
            f"Referência: **{_now_br} (Brasília)**. Os valores vêm só do **Databricks** no clique em "
            "**Atualizar dados** — o Zendesk pode estar minutos à frente até o pipeline replicar."
        )
    except OSError:
        pass
    if _lag_msg:
        st.caption(" ".join(_lag_msg))

    def _total_tres_transportadoras(frame: pd.DataFrame) -> float:
        s = 0.0
        for _lbl, raw, num in (
            ("Correios", "quantidade_rastreio_correios", "quantidade_rastreio_correios_num"),
            ("Jadlog", "quantidade_rastreio_jadlog", "quantidade_rastreio_jadlog_num"),
            ("Loggi", "quantidade_rastreio_loggi", "quantidade_rastreio_loggi_num"),
        ):
            if num in frame.columns:
                s += float(pd.to_numeric(frame[num], errors="coerce").fillna(0).sum())
            elif raw in frame.columns:
                s += float(pd.to_numeric(frame[raw], errors="coerce").fillna(0).sum())
        return s

    _total_track_series = (
        pd.to_numeric(df["total_qtd_rastreio"], errors="coerce").fillna(0)
        if "total_qtd_rastreio" in df.columns
        else pd.Series([0.0] * len(df))
    )
    _total_3 = _total_tres_transportadoras(df)

    _app_ttr_ser, _app_n_lines, _app_n_ttr = _app_tracking_ttr_stats(df)
    _ttr_medio_str = f"{float(_app_ttr_ser.mean()):.1f}" if _app_n_ttr else "—"

    _vol_df_ar = pd.DataFrame()
    if is_ar:
        _vol_df_ar = _ar_carrier_volume_df(df)

    def _sum_br_carrier(frame: pd.DataFrame, raw: str, num: str) -> float:
        if num in frame.columns:
            return float(pd.to_numeric(frame[num], errors="coerce").fillna(0).sum())
        if raw in frame.columns:
            return float(pd.to_numeric(frame[raw], errors="coerce").fillna(0).sum())
        return float("nan")

    def _max_codigos_um_ticket_br(frame: pd.DataFrame) -> str:
        if frame.empty:
            return "—"
        if "total_qtd_rastreio" in frame.columns:
            v = int(pd.to_numeric(frame["total_qtd_rastreio"], errors="coerce").fillna(0).max())
            return str(v)
        parts: list[pd.Series] = []
        for raw, num in (
            ("quantidade_rastreio_correios", "quantidade_rastreio_correios_num"),
            ("quantidade_rastreio_jadlog", "quantidade_rastreio_jadlog_num"),
            ("quantidade_rastreio_loggi", "quantidade_rastreio_loggi_num"),
        ):
            if num in frame.columns:
                parts.append(pd.to_numeric(frame[num], errors="coerce").fillna(0))
            elif raw in frame.columns:
                parts.append(pd.to_numeric(frame[raw], errors="coerce").fillna(0))
        if not parts:
            return "—"
        return str(int(pd.concat(parts, axis=1).sum(axis=1).max()))

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Tickets", len(df))

    if is_ar:

        def _ar_vol_label(lab: str) -> str:
            r = _vol_df_ar[_vol_df_ar["transportadora"] == lab]
            if len(r):
                return str(int(r["volume_solicitacoes"].sum()))
            return "0"

        _min1 = bool(is_ar and _meta.get("somente_rastreio", True))
        _tn_loaded = _ar_count_tracking_codes_in_frame(
            df_loaded, min_one_per_ticket_if_rastreio=_min1
        )
        _tn_visible = _ar_count_tracking_codes_in_frame(df, min_one_per_ticket_if_rastreio=_min1)
        c2.metric(
            "Tracking Numbers",
            _tn_loaded if len(df_loaded) else "—",
            help="Soma de **códigos de rastreio** (`code` preenchido no JSON do app AR; `id` não conta). "
            "Só o parse de **tracking_numbers_data** (a coluna **total_qtd_rastreio** na amostra já vem igual a isso após o fetch). "
            "Se a consulta foi com **só com rastreio**, cada ticket entra com **no mínimo 1** no somatório do card "
            "(alinha ao filtro quando há ticket forçado). "
            "Cards por operadora somam só segmentos com código. "
            f"Na amostra filtrada por transportadora: **{_tn_visible}**.",
        )
        c3.metric(
            "Máx. códigos em 1 ticket",
            _ar_max_tracking_codes_one_ticket(
                df_loaded, min_one_per_ticket_if_rastreio=_min1
            )
            if len(df_loaded)
            else "—",
            help="Maior número de códigos (`code` / rastreio) num único ticket, só pelo JSON parseado; "
            "com filtro **só com rastreio**, no mínimo 1 por ticket no somatório.",
        )
        c4.metric("Correo Argentino", _ar_vol_label("Correo Argentino"))
        c5.metric("Andreani", _ar_vol_label("Andreani"))
        c6.metric("E-pick", _ar_vol_label("E-pick"))
        c7.metric(
            "Tempo médio (h)",
            _ttr_medio_str,
            help="Horas em média até o envio ser concluído (só os já finalizados).",
        )
    else:
        _tot_sol = int(_total_3) if _total_3 == _total_3 else None
        c2.metric(
            "Total de solicitações",
            _tot_sol if _tot_sol is not None else "—",
            help="Soma Correios + Jadlog + Loggi na amostra.",
        )
        c3.metric("Máx. códigos em 1 ticket", _max_codigos_um_ticket_br(df))
        s_co = _sum_br_carrier(df, "quantidade_rastreio_correios", "quantidade_rastreio_correios_num")
        s_jd = _sum_br_carrier(df, "quantidade_rastreio_jadlog", "quantidade_rastreio_jadlog_num")
        s_lg = _sum_br_carrier(df, "quantidade_rastreio_loggi", "quantidade_rastreio_loggi_num")
        c4.metric("Correios", int(s_co) if s_co == s_co else "—")
        c5.metric("Jadlog", int(s_jd) if s_jd == s_jd else "—")
        c6.metric("Loggi", int(s_lg) if s_lg == s_lg else "—")
        c7.metric(
            "Tempo médio (h)",
            _ttr_medio_str,
            help="Horas em média até concluir cada envio, quando essa informação existe na amostra.",
        )

    if _app_n_lines > 0 and "tracking_numbers_data" in df.columns:
        st.caption("O **tempo médio** só inclui envios já marcados como concluídos.")

    _n = len(df)

    if "ticket_id" not in df.columns:
        st.warning("Não há identificador de ticket nos dados; os gráficos não podem ser montados.")
    else:
        if "status_rastreamento" in df.columns:
            st.subheader("Status de rastreamento por ticket")
            st.caption(
                "Cada linha é um ticket. A barra mostra **quantos códigos de rastreio** há em cada tipo de situação "
                "(resolvido, pendente, etc.). Os tickets estão ordenados pelos que têm **mais códigos no total**. "
                + (
                    "**Argentina:** cada segmento com **code** de rastreio preenchido (listas `correo` / `andreani` / `epik`); "
                    "**id** interno não conta. A cor vem do **status** do app (PT/ES). "
                    "Não há fatia excedente: o total do gráfico é só o parse do JSON. "
                    if is_ar
                    else "Tickets **sem** chave em `status_rastreamento` aparecem como faixa **Sem informação de status**; "
                    "a largura usa o total de códigos quando existir, senão **1** só para o ticket aparecer no eixo (não significa “1 código” real). "
                )
                + "**Clique numa barra** para abrir o detalhe (sem sair da página — mantém sessão e dados carregados): "
                "cada **code** em **tracking_numbers_data** e o **agente** (**agentName**). "
                "Com muitos tickets no período, use este controle para manter o gráfico fluido (a tabela e o CSV usam o lote completo)."
            )
            _top_default = min(80, max(15, _n))
            _top_max = min(250, max(1, _n))
            _top_status = st.number_input(
                "Quantidade de tickets neste gráfico",
                min_value=1,
                max_value=_top_max,
                value=min(_top_default, _top_max),
                step=1,
                key=f"ne_{k}_status_stack_top_n",
                help="Só o eixo Y do gráfico é limitado; métricas e export usam todos os tickets carregados.",
            )
            _long_status, _ticket_order_status = _long_df_tracking_status_by_ticket(
                df, _top_status, for_argentina_tab=is_ar
            )
            if _long_status.empty:
                st.info("Não há barras para exibir: neste recorte não há códigos com situação para mostrar.")
            else:
                _long_status = _long_status.copy()
                _h_status = max(260, min(720, len(_ticket_order_status) * 26))
                _pick = alt.selection_point(
                    fields=["ticket_id"],
                    name="ne_status_bar_pick",
                )
                _enc_status = dict(
                    x=alt.X("qtd:Q", stack="zero", title="Quantidade de códigos de rastreio"),
                    y=alt.Y("ticket_id:N", title="Ticket", sort=_ticket_order_status),
                    color=alt.Color(
                        "categoria:N",
                        scale=alt.Scale(
                            domain=list(_TRACKING_STATUS_ORDER),
                            range=[_TRACKING_STATUS_COLORS[c] for c in _TRACKING_STATUS_ORDER],
                        ),
                        legend=alt.Legend(title="Situação"),
                    ),
                    order=alt.Order("ord:Q", sort="ascending"),
                    fillOpacity=alt.condition(_pick, alt.value(1.0), alt.value(0.68)),
                    tooltip=[
                        alt.Tooltip("ticket_id:N", title="Ticket"),
                        alt.Tooltip("categoria:N", title="Situação"),
                        alt.Tooltip("qtd:Q", title="Quantidade", format=".0f"),
                    ],
                )
                _status_chart = (
                    alt.Chart(_long_status)
                    .mark_bar(cornerRadiusEnd=2, cursor="pointer")
                    .encode(**_enc_status)
                    .add_params(_pick)
                    .properties(height=_h_status)
                )
                _chart_reset = int(st.session_state.get(f"ne_chart_reset_{k}", 0))
                _evt = st.altair_chart(
                    _status_chart,
                    key=f"ne_status_chart_{k}_{_chart_reset}",
                    on_select="rerun",
                    width="stretch",
                )
                _clicked_tid = _ticket_id_from_vega_selection(_evt, "ne_status_bar_pick")
                if _clicked_tid:
                    _cur = st.session_state.get("ne_codes_ticket")
                    _cur_tab = st.session_state.get("ne_codes_tab")
                    if _norm_ticket_id(str(_cur or "")) != _clicked_tid or _cur_tab != k:
                        st.session_state["ne_codes_ticket"] = _clicked_tid
                        st.session_state["ne_codes_tab"] = k
                        st.session_state["_ne_sync_pick_widget"] = k
                        st.rerun()

            if "tracking_numbers_data" in df.columns:
                _tk_opts = _ticket_ids_for_detail_select(df, k)
                if _tk_opts:
                    _sel_key = f"ne_ticket_codes_select_{k}"
                    if st.session_state.get("_ne_sync_pick_widget") == k:
                        st.session_state.pop("_ne_sync_pick_widget", None)
                        _w = st.session_state.get("ne_codes_ticket")
                        if _w and _norm_ticket_id(str(_w)) in _tk_opts:
                            st.session_state[_sel_key] = _norm_ticket_id(str(_w))
                    if _sel_key not in st.session_state:
                        st.session_state[_sel_key] = _NE_DETAIL_SEL_NONE
                    _all_opts = [_NE_DETAIL_SEL_NONE] + _tk_opts
                    if st.session_state.get(_sel_key) not in _all_opts:
                        st.session_state[_sel_key] = _NE_DETAIL_SEL_NONE
                    _picked = st.selectbox(
                        "Ticket para painel de detalhe (códigos, status, guru, TTR)",
                        options=_all_opts,
                        key=_sel_key,
                        help="Abre o painel no topo da página. **Nenhum painel** fecha o detalhe. "
                        "Clicar numa barra do gráfico também seleciona o ticket.",
                    )
                    _want_tid = st.session_state.get("ne_codes_ticket")
                    _want_tab = st.session_state.get("ne_codes_tab")
                    if _picked == _NE_DETAIL_SEL_NONE:
                        if _want_tab == k and _want_tid:
                            st.session_state.pop("ne_codes_ticket", None)
                            st.session_state.pop("ne_codes_tab", None)
                            st.session_state[f"ne_chart_reset_{k}"] = int(
                                st.session_state.get(f"ne_chart_reset_{k}", 0)
                            ) + 1
                            st.rerun()
                    else:
                        _pn = _norm_ticket_id(str(_picked))
                        if _norm_ticket_id(str(_want_tid or "")) != _pn or _want_tab != k:
                            st.session_state["ne_codes_ticket"] = _pn
                            st.session_state["ne_codes_tab"] = k
                            st.rerun()

    if is_ar:
        st.subheader("Volume por transportadora (Argentina)")
        st.caption(
            "Conta só objetos com **code** de rastreio preenchido (entradas só “Solo consulta” sem código não entram). "
            "**Correo Argentino**, **Andreani** e **E-pick** na amostra **filtrada** "
            "(o card **Tracking Numbers** acima soma toda a consulta). A barra inteira é o total; cada cor é uma transportadora."
        )
        _ar_logo_files = {
            "Correo Argentino": "correo_argentino.png",
            "Andreani": "andreani.png",
            "E-pick": "epick.png",
        }
        if _vol_df_ar["volume_solicitacoes"].sum() > 0:
            _html_ar = _ne_html_stacked_carriers_volume(
                _vol_df_ar,
                _AR_CARRIER_STYLE,
                _ar_logo_files,
                total_footer_label="Tracking Numbers",
            )
            components.html(_html_ar, height=230, scrolling=False)
        else:
            st.info("Não há transportadora identificada nos dados desta amostra.")

    else:
        st.subheader("Volume por transportadora")
        st.caption(
            "Mostra **quantas solicitações** há para **Correios**, **Jadlog** e **Loggi** nesta amostra. "
            "A barra inteira é o total; cada cor é uma transportadora."
        )
        _vol_rows: list[dict[str, object]] = []
        for label, raw, num in (
            ("Correios", "quantidade_rastreio_correios", "quantidade_rastreio_correios_num"),
            ("Jadlog", "quantidade_rastreio_jadlog", "quantidade_rastreio_jadlog_num"),
            ("Loggi", "quantidade_rastreio_loggi", "quantidade_rastreio_loggi_num"),
        ):
            if num in df.columns:
                v = float(pd.to_numeric(df[num], errors="coerce").fillna(0).sum())
            elif raw in df.columns:
                v = float(pd.to_numeric(df[raw], errors="coerce").fillna(0).sum())
            else:
                v = 0.0
            _vol_rows.append({"transportadora": label, "volume_solicitacoes": v})
        _vol_df = pd.DataFrame(_vol_rows)
        _br_carrier_style: dict[str, tuple[str, str, str]] = {
            "Correios": ("#F7D117", "#1a1a1a", "C"),
            "Jadlog": ("#D74012", "#ffffff", "J"),
            "Loggi": ("#00A859", "#ffffff", "L"),
        }
        _br_logo_files = {
            "Correios": "correios.png",
            "Jadlog": "jadlog.png",
            "Loggi": "loggi.png",
        }
        if _vol_df["volume_solicitacoes"].sum() > 0:
            _html_stack = _ne_html_stacked_carriers_volume(_vol_df, _br_carrier_style, _br_logo_files)
            components.html(_html_stack, height=230, scrolling=False)
        else:
            st.info("Não há solicitações por transportadora nesta amostra.")

    if is_ar and "tracking_numbers_data" in df.columns and _app_n_lines == 0 and len(df) > 0:
        st.info(
            "Os dados de envio desta amostra não puderam ser lidos no formato esperado. "
            "Se o problema continuar, envie um exemplo anonimizado para ajustarmos a leitura."
        )

    st.subheader("Amostra de tickets")
    st.caption(
        "Ordem pensada para leitura: **ticket → totais → transportadoras / JSON de rastreio → datas**. "
        "Campos JSON longos vêm **abreviados** na tela; o botão de CSV traz o texto completo. "
        "**Status do ticket** vem da coluna nativa do Zendesk na tabela do lakehouse; a consulta usa "
        "**uma linha por `id`** (a mais recente por `updated_at`), para não exibir `open` de uma carga antiga "
        "quando o caso já está `closed`."
    )
    _sam_cols = _ne_sample_column_order(df, is_ar)
    if not _sam_cols:
        st.info("Não há colunas para montar a amostra.")
    else:
        _disp = _ne_prepare_sample_display_df(df, _sam_cols)
        _cc = _ne_sample_column_config(list(_disp.columns))
        st.dataframe(
            _disp,
            use_container_width=True,
            hide_index=True,
            column_config=_cc,
            height=min(720, 320 + min(len(_disp), 80) * 28),
        )

    with st.expander("Ideias de medição e acompanhamento"):
        st.markdown(
            """
- **Volume por transportadora:** o gráfico acima já mostra a soma na amostra; para série semanal, exporte o CSV em períodos fixos ou leve o mesmo cálculo ao Looker.
- **Pedidos “pesados”:** acompanhe tickets com muitos códigos no mesmo caso (gráfico de status ou CSV da amostra).
- **Argentina:** TTR no painel por ticket e na amostra exportável em CSV.
- **Qualidade:** cruzar situação do rastreio com volume quando fizer sentido para a operação.
- **Séries no tempo:** exporte o CSV por períodos fixos ou use o Looker para outras visões.
- **Alertas:** limiar de tickets/dia acima do histórico (ex.: p95 da semana anterior) para escalar capacidade.
"""
        )

    _n_dl = len(df)
    _dl_cap = 200_000
    _dl_df = df if _n_dl <= _dl_cap else df.head(_dl_cap)
    if _n_dl > _dl_cap:
        st.caption(
            f"Export CSV limitado às primeiras **{_dl_cap:,}** linhas (total no período: **{_n_dl:,}**). "
            "Para extrair tudo, use o job no Databricks ou reduza o período."
        )
    st.download_button(
        f"Baixar CSV ({_n_dl:,} linhas)" if _n_dl <= _dl_cap else f"Baixar CSV (primeiras {_dl_cap:,} de {_n_dl:,})",
        _dl_df.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"nuvem_envio_rastreio_{k}.csv",
        mime="text/csv",
        key=f"ne_{k}_dl_csv",
    )


st.set_page_config(page_title=_NE_PAGE_TITLE, layout="wide")
# Auth antes do tema pesado: na tela de login não injeta _inject_ne_theme (menos FOUC / conflito de CSS).
_ne_ensure_dashboard_auth()
# Links antigos (?ne_codes=1): sincroniza sessão e dá rerun ANTES de pintar cabeçalho/abas/dados
# (evita um frame com URL legada + layout principal visível).
_ne_qp_ticket = _query_param_first("ne_ticket")
_ne_qp_codes = _query_param_first("ne_codes")
_ne_open_codes_panel = str(_ne_qp_codes or "").strip().lower() in ("1", "true", "yes")
if _ne_qp_ticket and _ne_open_codes_panel:
    st.session_state["ne_codes_ticket"] = _norm_ticket_id(_ne_qp_ticket)
    st.session_state["ne_codes_tab"] = _query_param_first("ne_tab") or "brasil"
    st.session_state["_ne_sync_pick_widget"] = st.session_state["ne_codes_tab"]
    for _qk in ("ne_ticket", "ne_codes"):
        if _qk in st.query_params:
            del st.query_params[_qk]
    st.rerun()

_inject_ne_theme()
with st.sidebar:
    if st.session_state.get("ne_auth_ok"):
        if st.button("Sair da sessão", key="ne_logout_btn", help="Encerra o login neste navegador."):
            st.session_state.pop("ne_auth_ok", None)
            st.rerun()
raw_cfg = ne.load_config()
_run_pending_ne_fetch(raw_cfg)

_ne_codes_tid = st.session_state.get("ne_codes_ticket")
_ne_codes_tab = st.session_state.get("ne_codes_tab")
if _ne_codes_tid and _ne_codes_tab:
    _render_ticket_codes_guru_panel(
        raw_cfg, str(_ne_codes_tid), str(_ne_codes_tab)
    )
elif _ne_qp_ticket:
    _render_amostra_ticket_panel(raw_cfg, _ne_qp_ticket)

st.markdown(
    f"""
    <div style="text-align:center;padding:0.5rem 0 1.25rem 0;margin-bottom:0.25rem;">
        <h1 style="margin:0;color:{_NE_ACCENT};font-size:clamp(1.75rem,4vw,2.65rem);\
font-weight:800;letter-spacing:-0.03em;line-height:1.15;">
            {_NE_PAGE_TITLE}
        </h1>
        <p style="margin:0.85rem auto 0 auto;color:{_NE_ACCENT};font-size:1.05rem;max-width:42rem;\
line-height:1.45;text-align:center;">
            Dados via Databricks (Zendesk). Credenciais no arquivo <code>.env</code> na raiz do projeto.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

_tabs = raw_cfg.get("tabs")
if isinstance(_tabs, dict) and "brasil" in _tabs and "argentina" in _tabs:
    tab_br, tab_ar = st.tabs(["\U0001f1e7\U0001f1f7  Brasil", "\U0001f1e6\U0001f1f7  Argentina"])
    with tab_br:
        _render_ne_country_tab(raw_cfg, "brasil")
    with tab_ar:
        _render_ne_country_tab(raw_cfg, "argentina")
else:
    st.warning(
        "Adicione **tabs.brasil** e **tabs.argentina** em nuvem_envio_rastreio_config.json. "
        "Enquanto isso, exibindo só Brasil (config legada)."
    )
    _render_ne_country_tab(raw_cfg, "brasil")

_inject_filter_widget_labels_priority()
