"""
Dashboard Streamlit — acompanhamento operação Nuvem Envio (campos de rastreio).

Instalação (uma vez):
  pip install -r requirements.txt

Execução (na raiz deste repositório):
  streamlit run dashboard_nuvem_envio_rastreio.py

Antes: preencha zendesk_field_ids em nuvem_envio_rastreio_config.json
(use sql_descobrir_campos_rastreio.sql no Databricks para achar os IDs).
Abas **Brasil** e **Argentina** em nuvem_envio_rastreio_config.json → `tabs`.
Brasil: três transportadoras + status por ticket. Argentina: [AR] Envio Nube, volume e tempo por envio. Clique: ?ne_ticket= e ?ne_tab=brasil|argentina.
"""
from __future__ import annotations

import hmac
import json
import os
import sys
import urllib.parse
from datetime import date, timedelta
from pathlib import Path

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

    st.markdown(
        f"""
        <div style="max-width:420px;margin:3rem auto 1.5rem auto;text-align:center;">
            <h1 style="color:{html.escape(_NE_ACCENT)};font-size:1.65rem;font-weight:800;margin:0;">
                {_NE_PAGE_TITLE}
            </h1>
            <p style="color:#475569;margin-top:0.75rem;font-size:1rem;">Acesso restrito</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.form("ne_login_form", clear_on_submit=False):
        typed_user = ""
        if expected_user is not None:
            typed_user = st.text_input("Usuário", key="ne_login_user")
        typed_pw = st.text_input("Senha", type="password", key="ne_login_pw")
        submitted = st.form_submit_button("Entrar", type="primary")

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
        cfg = ne.effective_config_for_tab(raw_cfg, tab_key)
        sk_df = f"ne_df_{tab_key}"
        sk_meta = f"ne_meta_{tab_key}"
        err_key = f"ne_{tab_key}_fetch_error"
        _render_full_page_loading_ne()
        try:
            df = ne.fetch_dataframe(
                start_date=params["start"],
                end_date=params["end"],
                statuses=list(params["statuses"]),
                config=cfg,
                limit=params.get("limit"),
                only_with_tracking_filled=params.get("somente_rastreio"),
            )
        except Exception as e:
            st.session_state[err_key] = str(e)
        else:
            st.session_state[sk_df] = df
            st.session_state[sk_meta] = {"somente_rastreio": bool(params.get("somente_rastreio"))}
            st.session_state.pop(err_key, None)
        st.rerun()


_TRACKING_STATUS_ORDER = (
    "Resolvido",
    "Pendente",
    "Aberto",
    "Sem informação de status",
    "Outros",
    "Só no total geral",
)
_TRACKING_STATUS_COLORS = {
    "Resolvido": "#16a34a",
    "Pendente": "#ca8a04",
    "Aberto": "#2563eb",
    "Sem informação de status": "#94a3b8",
    "Outros": "#7c3aed",
    "Só no total geral": "#cbd5e1",
}


def _normalize_tracking_status_value(val: object) -> str:
    """Classifica o valor de status de um código no JSON `status_rastreamento`."""
    if val is None:
        return "sem_status"
    s = str(val).strip().lower()
    if not s or s in ("null", "none", "{}", "[]"):
        return "sem_status"
    if any(
        k in s
        for k in (
            "resolv",
            "solved",
            "closed",
            "fechad",
            "cerrad",
            "entregue",
            "delivered",
            "complet",
        )
    ):
        return "resolvido"
    if "pend" in s or "pendiente" in s:
        return "pendente"
    if any(k in s for k in ("open", "opening", "abert", "abiert", "abierto")):
        return "aberto"
    return "outros"


def _parse_status_rastreamento_json(raw: object) -> dict[str, int]:
    """Conta códigos no JSON {codigo: status} por categoria interna."""
    out = {"resolvido": 0, "pendente": 0, "aberto": 0, "sem_status": 0, "outros": 0}
    if raw is None:
        return out
    try:
        if pd.api.types.is_scalar(raw) and pd.isna(raw):
            return out
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    if not s or s.lower() in ("{}", "null", "none", "nan"):
        return out
    try:
        parsed: object = json.loads(s)
    except json.JSONDecodeError:
        return out
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError:
            return out
    if not isinstance(parsed, dict):
        return out
    for _k, v in parsed.items():
        cat = _normalize_tracking_status_value(v)
        if cat == "resolvido":
            out["resolvido"] += 1
        elif cat == "pendente":
            out["pendente"] += 1
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
        "Aberto": float(counts["aberto"]),
        "Sem informação de status": float(counts["sem_status"]),
        "Outros": float(counts["outros"]),
        "Só no total geral": extra,
    }


def _long_df_tracking_status_by_ticket(df: pd.DataFrame, top_n: int) -> tuple[pd.DataFrame, list[str]]:
    """Dataframe longo (ticket_id, categoria, qtd, ord) e ordem dos tickets no eixo Y."""
    if "ticket_id" not in df.columns:
        return pd.DataFrame(), []
    work = df.copy()
    if "total_qtd_rastreio" in work.columns:
        work["_tot"] = pd.to_numeric(work["total_qtd_rastreio"], errors="coerce").fillna(0)
    else:
        work["_tot"] = 0.0
    work = work.sort_values("_tot", ascending=False).head(int(top_n))
    ticket_order = work["ticket_id"].astype(str).tolist()
    _ord_map = {c: i for i, c in enumerate(_TRACKING_STATUS_ORDER)}
    rows: list[dict[str, object]] = []
    for _, row in work.iterrows():
        tid = str(row["ticket_id"])
        bucket = _tracking_status_buckets_for_row(row)
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
    return pd.DataFrame(rows), ticket_order


def _parse_tracking_numbers_app_json(raw: object) -> list[dict]:
    """Segmentos/códigos serializados no custom field (JSON no Zendesk).

    **Modelo Argentina ([AR] Envio Nube):** app grava `createdAt`, `finalizadoAt`, `duracion`, etc.

    Aceita: lista de objetos; lista de strings JSON; objeto com `cards`/`items`/etc.;
    objeto único com `createdAt` e término (`finalizadoAt` ou `completedAt`).
    """

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
            ):
                inner = obj.get(key)
                if isinstance(inner, list):
                    return inner
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

    if raw is None:
        return []
    try:
        if pd.api.types.is_scalar(raw) and pd.isna(raw):
            return []
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    if not s or s.lower() in ("{}", "null", "none", "nan", "[]"):
        return []
    try:
        data: object = json.loads(s)
    except json.JSONDecodeError:
        return []
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


def _tracking_display_code(it: dict) -> str:
    for key in (
        "trackingNumber",
        "tracking_number",
        "trackingCode",
        "code",
        "codigo",
        "numero_rastreio",
        "numeroRastreio",
        "rastreio",
        "id",
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
    for key in ("duracion", "duración", "duration"):
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
    if "e-pick" in s or "epick" in s or s == "e pick":
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


def _ne_html_stacked_carriers_volume(
    vol_df: pd.DataFrame,
    carrier_style: dict[str, tuple[str, str, str]],
    logo_files: dict[str, str],
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
        f'<p style="margin:8px 0 0 0;font-size:13px;color:#444;">Total: <b>{total:.0f}</b> solicitações'
        f" · {_pct_line}</p>"
        "</div>"
    )


def _build_encomendas_ttr_detail_df(df: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por encomenda/código dentro de `tracking_numbers_data`, com TTR quando houver término."""
    if "tracking_numbers_data" not in df.columns or "ticket_id" not in df.columns:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    for _, r in df.iterrows():
        tid = _norm_ticket_id(r["ticket_id"])
        raw = r["tracking_numbers_data"]
        items = _parse_tracking_numbers_app_json(raw)
        for i, it in enumerate(items, start=1):
            ca = it.get("createdAt") or it.get("created_at")
            co = _tracking_segment_end_timestamp(it)
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
            code = _tracking_display_code(it)
            car_raw = _tracking_display_carrier(it)
            car_disp = car_raw if car_raw else "—"
            car_ar = _ar_canonical_carrier(car_raw)
            dtxt = _tracking_app_duracion_text(it)
            rows.append(
                {
                    "ticket_id": tid,
                    "encomenda_n": i,
                    "codigo_rastreio": code or "—",
                    "transportadora": car_disp,
                    "transportadora_ar": car_ar,
                    "created_at": ca if ca is not None and str(ca).strip() else "—",
                    "finalizado_em": co if cos else "—",
                    "duracion_app": dtxt or "—",
                    "ttr_horas": round(ttr, 2) if ttr is not None else None,
                    "situacao": situacao,
                }
            )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    return out


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


@st.cache_data(show_spinner=False)
def _cached_amostra_ticket_map(json_abs_path: str, file_mtime: float) -> dict[str, dict]:
    """Índice ticket_id normalizado → objeto ticket do JSON da amostra.

    file_mtime entra na chave de cache para refletir edições no arquivo sem reiniciar o app.
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
            if "ne_ticket" in st.query_params:
                del st.query_params["ne_ticket"]
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


def _ne_prepare_sample_display_df(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    use = [c for c in cols if c in df.columns]
    sub = df.loc[:, use].head(500).copy()
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
        "created_at": "Criado em",
        "updated_at": "Atualizado em",
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
    is_ar = cfg.get("data_model") == ne.DATA_MODEL_AR
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

    col1, col2, col3, col4 = st.columns(4)
    today = date.today()
    default_start = today - timedelta(days=14)
    with col1:
        start = st.date_input("Atualizado desde", value=default_start, key=f"ne_{k}_start")
    with col2:
        end = st.date_input("Atualizado até", value=today, key=f"ne_{k}_end")
    with col3:
        status_opts = ["new", "open", "pending", "hold", "solved", "closed"]
        statuses = st.multiselect(
            "Status do ticket",
            status_opts,
            default=status_opts,
            key=f"ne_{k}_statuses",
        )
    with col4:
        max_tickets = st.number_input(
            "Máx. tickets",
            min_value=0,
            max_value=100_000,
            value=10,
            step=1,
            key=f"ne_{k}_max_t",
            help="0 = sem limite (cuidado: consulta pesada). Para teste use 10.",
        )

    _chk_help = (
        "Mantém só tickets que já têm dados de rastreio preenchidos (Argentina)."
        if is_ar
        else (
            "Mantém só tickets que já têm pedido de rastreio (transportadora ou situação de rastreio preenchidos)."
        )
    )
    _row_pad, _row_chk = st.columns([4.2, 1])
    with _row_chk:
        somente_rastreio = st.checkbox(
            "Só com rastreio",
            value=bool(cfg.get("somente_com_rastreio_preenchido", True)),
            help=_chk_help,
            key=f"ne_{k}_somente",
        )

    _sk_pending = f"ne_{k}_pending_fetch"
    if st.button("Atualizar dados", type="primary", key=f"ne_{k}_btn_refresh"):
        if not statuses:
            st.error("Selecione ao menos um status.")
        else:
            _lim = int(max_tickets) if max_tickets and max_tickets > 0 else None
            st.session_state[_sk_pending] = {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "statuses": list(statuses),
                "limit": _lim,
                "somente_rastreio": somente_rastreio,
            }
            st.rerun()

    df = st.session_state.get(sk_df)
    if df is None or df.empty:
        st.info("Clique em **Atualizar dados** para carregar. Período padrão: últimas 2 semanas.")
        return

    _meta = st.session_state.get(sk_meta) or {}
    filtro_txt = " + só com rastreio preenchido" if _meta.get("somente_rastreio") else ""
    _country_lbl = "Argentina · [AR] Envio Nube" if is_ar else "Brasil · Nuvem Envio"
    st.success(
        f"**{len(df)}** tickets ({_country_lbl}{filtro_txt}). "
        "Ordenados por **total** (maior primeiro)."
    )

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

        c2.metric(
            "Total de solicitações",
            int(_total_track_series.sum()) if len(df) else "—",
            help="Soma de todos os códigos de rastreio nesta amostra.",
        )
        c3.metric(
            "Máx. códigos em 1 ticket",
            int(_total_track_series.max()) if len(df) and "total_qtd_rastreio" in df.columns else "—",
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
    _ar_ttr_det = pd.DataFrame()
    if is_ar and "tracking_numbers_data" in df.columns:
        _ar_ttr_det = _build_encomendas_ttr_detail_df(df)

    if "ticket_id" not in df.columns:
        st.warning("Não há identificador de ticket nos dados; os gráficos não podem ser montados.")
    else:
        if "status_rastreamento" in df.columns:
            st.subheader("Status de rastreamento por ticket")
            st.caption(
                "Cada linha é um ticket. A barra mostra **quantos códigos de rastreio** há em cada tipo de situação "
                "(resolvido, pendente, etc.). Os tickets estão ordenados pelos que têm **mais códigos no total**."
            )
            _top_status = st.number_input(
                "Quantidade de tickets neste gráfico",
                min_value=1,
                max_value=max(1, _n),
                value=min(100, _n),
                step=1,
                key=f"ne_{k}_status_stack_top_n",
                help="Mostra os primeiros tickets nessa ordem; reduza se o gráfico ficar apertado.",
            )
            _long_status, _ticket_order_status = _long_df_tracking_status_by_ticket(df, _top_status)
            if _long_status.empty:
                st.info("Não há barras para exibir: neste recorte não há códigos com situação para mostrar.")
            else:
                _long_status = _long_status.copy()
                _long_status["ne_href"] = (
                    _long_status["ticket_id"].astype(str).map(
                        lambda tid: _chart_ticket_href(tid, cfg, k) or ""
                    )
                )
                _h_status = max(280, min(900, len(_ticket_order_status) * 28))
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
                    tooltip=[
                        alt.Tooltip("ticket_id:N", title="Ticket"),
                        alt.Tooltip("categoria:N", title="Situação"),
                        alt.Tooltip("qtd:Q", title="Quantidade", format=".0f"),
                    ],
                )
                if _long_status["ne_href"].astype(str).str.len().gt(0).any():
                    _status_chart = (
                        alt.Chart(_long_status)
                        .mark_bar(cornerRadiusEnd=2, cursor="pointer")
                        .encode(
                            **_enc_status,
                            href=alt.Href("ne_href:N"),
                        )
                        .properties(height=_h_status)
                    )
                else:
                    _status_chart = (
                        alt.Chart(_long_status)
                        .mark_bar(cornerRadiusEnd=2)
                        .encode(**_enc_status)
                        .properties(height=_h_status)
                    )
                st.altair_chart(_status_chart, use_container_width=True)

    if is_ar:
        st.subheader("Volume por transportadora (Argentina)")
        st.caption(
            "Mostra **quantas solicitações** há para **Correo Argentino**, **Andreani** e **E-pick** nesta amostra. "
            "A barra inteira é o total; cada cor é uma transportadora."
        )
        _ar_logo_files = {
            "Correo Argentino": "correo_argentino.png",
            "Andreani": "andreani.png",
            "E-pick": "epick.png",
        }
        if _vol_df_ar["volume_solicitacoes"].sum() > 0:
            _html_ar = _ne_html_stacked_carriers_volume(_vol_df_ar, _AR_CARRIER_STYLE, _ar_logo_files)
            components.html(_html_ar, height=230, scrolling=False)
        else:
            st.info("Não há transportadora identificada nos dados desta amostra.")

        if not _ar_ttr_det.empty:
            st.subheader("Tempo por código de rastreio (Argentina)")
            st.caption(
                "Só aparecem envios **já concluídos**. Cada barra horizontal é um **código de rastreio**; o comprimento "
                "é **quantas horas** levou até concluir."
            )
            _ar_color_domain = list(_AR_CARRIER_ORDER) + [_AR_CARRIER_OTHER]
            _ar_color_range = [_AR_CARRIER_CHART_COLORS[d] for d in _ar_color_domain]

            _ttr_done = _ar_ttr_det[_ar_ttr_det["ttr_horas"].notna()].copy()
            if not _ttr_done.empty:
                _mean_by_c = (
                    _ttr_done.groupby("transportadora_ar", as_index=False)["ttr_horas"]
                    .mean()
                    .rename(columns={"ttr_horas": "ttr_medio_h"})
                )
                st.caption("Tempo médio de resolução **por transportadora** (só envios já finalizados).")
                _mean_chart = (
                    alt.Chart(_mean_by_c)
                    .mark_bar(cornerRadiusEnd=4)
                    .encode(
                        x=alt.X(
                            "transportadora_ar:N",
                            title="Transportadora",
                            sort=_ar_color_domain,
                        ),
                        y=alt.Y("ttr_medio_h:Q", title="Horas em média"),
                        color=alt.Color(
                            "transportadora_ar:N",
                            legend=None,
                            scale=alt.Scale(domain=_ar_color_domain, range=_ar_color_range),
                        ),
                        tooltip=[
                            alt.Tooltip("transportadora_ar:N", title="Transportadora"),
                            alt.Tooltip("ttr_medio_h:Q", title="Horas em média", format=".2f"),
                        ],
                    )
                    .properties(height=280)
                )
                st.altair_chart(_mean_chart, use_container_width=True)

            _top_ttr_tickets = st.number_input(
                "Quantos tickets incluir (começando pelos com mais códigos)",
                min_value=1,
                max_value=max(1, _n),
                value=min(15, _n),
                step=1,
                key=f"ne_{k}_ar_ttr_chart_top_tickets",
            )
            _max_seg_chart = st.number_input(
                "Máximo de barras no gráfico",
                min_value=10,
                max_value=400,
                value=120,
                step=10,
                key=f"ne_{k}_ar_ttr_chart_max_seg",
            )
            if "ticket_id" in df.columns and "total_qtd_rastreio" in df.columns:
                _tid_order = (
                    df.assign(_tid=df["ticket_id"].astype(str).map(_norm_ticket_id))
                    .sort_values("total_qtd_rastreio", ascending=False)
                    .drop_duplicates("_tid")
                    .head(int(_top_ttr_tickets))["_tid"]
                    .tolist()
                )
            else:
                _g2 = df.assign(_tid=df["ticket_id"].astype(str).map(_norm_ticket_id)).drop_duplicates("_tid")
                if "updated_at" in _g2.columns:
                    _g2 = _g2.sort_values("updated_at", ascending=False)
                _tid_order = _g2.head(int(_top_ttr_tickets))["_tid"].tolist()
            _set_tid = set(_tid_order)
            _plot_ttr = _ar_ttr_det[
                _ar_ttr_det["ticket_id"].isin(_set_tid) & _ar_ttr_det["ttr_horas"].notna()
            ].copy()
            if _plot_ttr.empty:
                st.info(
                    "Nenhum envio concluído entre os tickets escolhidos — o tempo só aparece quando o caso está finalizado."
                )
            else:
                _plot_ttr["ord_ticket"] = _plot_ttr["ticket_id"].map(
                    {t: i for i, t in enumerate(_tid_order)}
                )
                _plot_ttr["ord_ticket"] = _plot_ttr["ord_ticket"].fillna(9999)
                _plot_ttr = _plot_ttr.sort_values(
                    ["ord_ticket", "encomenda_n", "ttr_horas"],
                    ascending=[True, True, False],
                ).head(int(_max_seg_chart))
                _plot_ttr["rotulo"] = (
                    _plot_ttr["ticket_id"].astype(str)
                    + " · #"
                    + _plot_ttr["encomenda_n"].astype(str)
                    + " · "
                    + _plot_ttr["codigo_rastreio"].astype(str).str.slice(0, 22)
                )
                _h_bar = max(320, min(1200, 20 + 22 * len(_plot_ttr)))
                _per_code = (
                    alt.Chart(_plot_ttr)
                    .mark_bar(cornerRadiusEnd=2)
                    .encode(
                        y=alt.Y(
                            "rotulo:N",
                            title="",
                            sort=alt.EncodingSortField(field="ttr_horas", order="descending"),
                        ),
                        x=alt.X("ttr_horas:Q", title="Horas até concluir"),
                        color=alt.Color(
                            "transportadora_ar:N",
                            title="Transportadora",
                            scale=alt.Scale(domain=_ar_color_domain, range=_ar_color_range),
                        ),
                        tooltip=[
                            alt.Tooltip("ticket_id:N", title="Ticket"),
                            alt.Tooltip("codigo_rastreio:N", title="Código de rastreio"),
                            alt.Tooltip("transportadora_ar:N", title="Transportadora"),
                            alt.Tooltip("ttr_horas:Q", title="Horas", format=".2f"),
                            alt.Tooltip("duracion_app:N", title="Duração informada"),
                        ],
                    )
                    .properties(height=_h_bar)
                )
                st.altair_chart(_per_code, use_container_width=True)
                if len(_ar_ttr_det[_ar_ttr_det["ttr_horas"].notna()]) > int(_max_seg_chart):
                    st.caption(
                        f"Mostrando no máximo **{int(_max_seg_chart)}** barras. Aumente o limite acima se quiser ver mais."
                    )
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

    if is_ar and "tracking_numbers_data" in df.columns:
        if not _ar_ttr_det.empty:
            st.subheader("Planilha — tempo por envio (Argentina)")
            st.caption(
                "Cada linha é **um envio/código**. A tabela mostra datas, **horas até concluir** e a **situação**. "
                "Use o botão abaixo para baixar em CSV."
            )
            _ttr_tab = _ar_ttr_det.drop(columns=["transportadora_ar"], errors="ignore")
            _ttr_view = _ttr_tab.rename(
                columns={
                    "ticket_id": "Ticket",
                    "encomenda_n": "Nº no ticket",
                    "codigo_rastreio": "Código de rastreio",
                    "transportadora": "Transportadora",
                    "created_at": "Aberto em",
                    "finalizado_em": "Concluído em",
                    "duracion_app": "Duração informada",
                    "ttr_horas": "Horas até concluir",
                    "situacao": "Situação",
                }
            )
            st.dataframe(
                _ttr_view.head(5000),
                use_container_width=True,
                hide_index=True,
            )
            st.download_button(
                "Baixar CSV — tempo por envio (AR)",
                _ar_ttr_det.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"nuvem_envio_ttr_encomendas_{k}.csv",
                mime="text/csv",
                key=f"ne_{k}_dl_ttr_encomendas",
            )
        elif _app_n_lines == 0 and len(df) > 0:
            st.info(
                "Os dados de envio desta amostra não puderam ser lidos no formato esperado. "
                "Se o problema continuar, envie um exemplo anonimizado para ajustarmos a leitura."
            )

    st.subheader("Amostra de tickets")
    st.caption(
        "Ordem pensada para leitura: **ticket → totais → transportadoras / JSON de rastreio → datas**. "
        "Campos JSON longos vêm **abreviados** na tela; o botão de CSV traz o texto completo."
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
        )

    with st.expander("Ideias de medição e acompanhamento"):
        st.markdown(
            """
- **Volume por transportadora:** o gráfico acima já mostra a soma na amostra; para série semanal, exporte o CSV em períodos fixos ou leve o mesmo cálculo ao Looker.
- **Pedidos “pesados”:** acompanhe tickets com muitos códigos no mesmo caso (use a planilha ou exporte o CSV).
- **Argentina:** tempo por envio na aba Argentina (tabela e gráficos).
- **Qualidade:** cruzar situação do rastreio com volume quando fizer sentido para a operação.
- **Séries no tempo:** exporte o CSV por períodos fixos ou use o Looker para outras visões.
- **Alertas:** limiar de tickets/dia acima do histórico (ex.: p95 da semana anterior) para escalar capacidade.
"""
        )

    st.download_button(
        "Baixar CSV (até 50k linhas)",
        df.head(50000).to_csv(index=False).encode("utf-8-sig"),
        file_name=f"nuvem_envio_rastreio_{k}.csv",
        mime="text/csv",
        key=f"ne_{k}_dl_csv",
    )


st.set_page_config(page_title=_NE_PAGE_TITLE, layout="wide")
_inject_ne_theme()
_ne_ensure_dashboard_auth()
with st.sidebar:
    if st.session_state.get("ne_auth_ok"):
        if st.button("Sair da sessão", key="ne_logout_btn", help="Encerra o login neste navegador."):
            st.session_state.pop("ne_auth_ok", None)
            st.rerun()
raw_cfg = ne.load_config()
_run_pending_ne_fetch(raw_cfg)
_ne_qp_ticket = _query_param_first("ne_ticket")
if _ne_qp_ticket:
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
