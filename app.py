"""
Fund Comparator Report - Web App
Convertido de Google Colab a Flask para deployment en Railway
"""

import os
import io
import time
import re
import traceback
import uuid
import threading
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # Backend sin GUI — obligatorio en servidor

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
import matplotlib.ticker as mtick
import yfinance as yf
try:
    from fredapi import Fred as _Fred
    _FRED_AVAILABLE = True
except ImportError:
    _FRED_AVAILABLE = False
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.gridspec import GridSpec
from adjustText import adjust_text
import requests
from bs4 import BeautifulSoup

from flask import (
    Flask, request, jsonify, send_file,
    render_template, url_for
)

# ─────────────────────────────────────────────
# App setup
# ─────────────────────────────────────────────
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024  # 20 MB max upload

JOBS = {}           # job_id → {"status", "pdf_bytes", "error", "progress"}
JOBS_LOCK = threading.Lock()

# ─────────────────────────────────────────────
# Estilos globales matplotlib
# ─────────────────────────────────────────────
palette = [
    "#C0F0D8", "#06C264", "#C0CDCC", "#406666",
    "darkred", "#003334", "darkorange", "darkmagenta",
    "mediumturquoise", "red", "royalblue", "greenyellow"
]
header_bg = palette[1]
header_txt = "#F1F1F1"

sns.set_style("whitegrid")
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.titlesize": 14,
    "axes.labelsize": 11,
    "legend.fontsize": 9,
    "figure.titlesize": 16,
    "axes.linewidth": 1.2,
    "grid.alpha": 0.3,
    "axes.edgecolor": "#333333",
    "axes.axisbelow": True,
})


# ─────────────────────────────────────────────
# Helpers de FT / FX
# ─────────────────────────────────────────────
def get_gbp_usd_rate():
    try:
        ticker = yf.Ticker("GBPUSD=X")
        hist = ticker.history(period="5d")
        if not hist.empty:
            return float(hist["Close"].dropna().iloc[-1])
    except Exception:
        pass
    return 1.28


def parse_fund_size(text):
    patterns = [
        r"([\d,\.]+)\s*(bn|billion|m|million|k|thousand)\s*(GBP|gbp)",
        r"([\d,\.]+)\s*(GBP|gbp)\s*(bn|billion|m|million|k|thousand)",
        r"([\d,\.]+)(bn|m|k)\s*<[^>]*>GBP",
        r"([\d,\.]+)\s*(bn|m|k)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                value = float(m.group(1).replace(",", ""))
                mult_key = m.group(2).lower() if len(m.groups()) >= 2 else "m"
                mults = {"bn": 1000, "billion": 1000, "m": 1, "million": 1, "k": 0.001, "thousand": 0.001}
                return value * mults.get(mult_key, 1) * get_gbp_usd_rate()
            except ValueError:
                continue
    return None


def get_ft_data(isin):
    try:
        ratings_url = f"https://markets.ft.com/data/funds/tearsheet/ratings?s={isin}:USD"
        resp = requests.get(ratings_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(resp.text, "html.parser")

        ms_stars = category = None
        ms_app = soup.find("div", {"data-module-name": "MorningstarRatingApp"})
        if ms_app:
            highlighted = ms_app.select_one("span[data-mod-stars-highlighted]")
            ms_stars = len(highlighted.find_all("i", class_="mod-icon--star--filled")) if highlighted else None
            cat_div = ms_app.find("div", class_="mod-morningstar-rating-app__category")
            if cat_div:
                spans = cat_div.find_all("span")
                category = spans[1].get_text(strip=True) if len(spans) > 1 else None

        lr_rating = None
        lr_app = soup.find("div", {"data-module-name": "LipperRatingApp"})
        if lr_app:
            tbl = lr_app.find("table", class_="mod-ui-table")
            if tbl and tbl.thead and tbl.tbody:
                headers = [th.get_text(strip=True) for th in tbl.thead.find_all("th")]
                if "Total return" in headers:
                    col_idx = headers.index("Total return")
                    for row in tbl.tbody.find_all("tr"):
                        lc = row.find("td", class_="mod-ui-table__cell--text")
                        if lc and lc.get_text(strip=True) == "Overall rating":
                            cells = row.find_all("td")
                            if len(cells) > col_idx:
                                icon = cells[col_idx].find("i")
                                if icon and icon.get("class"):
                                    for c in icon["class"]:
                                        if c.startswith("mod-sprite-lipper-") and c.split("-")[-1].isdigit():
                                            lr_rating = int(c.split("-")[-1])
                                            break
                            break

        summary_url = f"https://markets.ft.com/data/funds/tearsheet/summary?s={isin}:USD"
        resp2 = requests.get(summary_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        fund_size_usd = launch_date = None
        for tbl in soup2.find_all("table", class_="mod-ui-table--two-column"):
            for row in tbl.find_all("tr"):
                th = row.find("th")
                if th:
                    hdr = th.get_text(strip=True).lower()
                    td = row.find("td")
                    if td:
                        content = td.get_text(strip=True)
                        if "fund size" in hdr:
                            fund_size_usd = parse_fund_size(content)
                        elif hdr == "launch date":
                            launch_date = content
                            break

        return ms_stars, category, lr_rating, fund_size_usd, launch_date
    except Exception as e:
        print(f"[FT] Error ISIN {isin}: {e}")
        return None, None, None, None, None


# ─────────────────────────────────────────────
# Funciones de cálculo
# ─────────────────────────────────────────────
def compute_stats(s, start, end):
    sub = s[start:end]
    if len(sub) < 2:
        return np.nan, np.nan, np.nan, np.nan
    total = sub.iloc[-1] / sub.iloc[0] - 1
    years = (sub.index[-1] - sub.index[0]).days / 365.25
    ann = (1 + total) ** (1 / years) - 1 if years > 0 else np.nan
    dr = sub.pct_change().dropna()
    vol = dr.std() * np.sqrt(252)
    neg = dr[dr < 0]
    dd = neg.std() * np.sqrt(252) if len(neg) > 0 else np.nan
    sharpe = ann / vol if vol else np.nan
    sortino = ann / dd if dd else np.nan
    return ann, vol, sharpe, sortino


def apply_chart_style(ax, title):
    ax.set_title(title, weight="bold", fontsize=14, pad=20, color="#2C3E50")
    for spine in ax.spines.values():
        spine.set_linewidth(1.2)
        spine.set_color("#555555")
    ax.grid(True, alpha=0.3, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(labelsize=9, colors="#333333")


def format_table_data(ratings_table):
    ratings_table = ratings_table.fillna("-")
    for col in ["MS Stars", "Lipper Rating"]:
        ratings_table[col] = ratings_table[col].apply(
            lambda x: f"{int(x)}" if pd.notna(x) and x != "-" else "-"
        )

    def fmt_size(v):
        if pd.isna(v) or v == "-":
            return "-"
        try:
            return f"{v:,.1f}"
        except Exception:
            return "-"

    ratings_table["Fund Size"] = ratings_table["Fund Size"].apply(fmt_size)
    return ratings_table


def calculate_dynamic_widths(data, headers, num_funds):
    max_fn = max((len(str(r[0])) for r in data if r[0] is not None), default=0)
    max_cat = max((len(str(r[3])) for r in data if len(r) > 3 and r[3] is not None), default=0)
    fondo_w = 0.25 if max_fn <= 25 else (0.30 if max_fn <= 35 else 0.35)
    cat_w = 0.18 if max_cat <= 20 else 0.22
    return [fondo_w, 0.14, 0.08, cat_w, 0.06, 0.08, 0.11]


# ─────────────────────────────────────────────
# Página de Drawdown y Recuperación
# ─────────────────────────────────────────────
def generate_drawdown_pages(pdf, page, df, fund_codes, name_map):
    bm_col = fund_codes[0]
    funds = fund_codes[1:]
    years = sorted(df.index.year.unique())

    all_results = []
    recovery_matrix = {f: {} for f in funds}

    for fund in funds:
        fund_data = df[fund].dropna()
        if len(fund_data) == 0:
            continue
        running_max = fund_data.cummax()
        drawdown_series = (fund_data - running_max) / running_max

        for year in years:
            ym = fund_data.index.year == year
            year_data = fund_data[ym]
            if len(year_data) == 0:
                continue
            year_dd = drawdown_series[ym]
            mdd_value = year_dd.min()
            if pd.isna(mdd_value) or abs(mdd_value) < 0.001:
                recovery_matrix[fund][year] = 0
                continue

            mdd_date = year_dd.idxmin()
            nav_at_mdd = fund_data.loc[mdd_date]
            peak_value = running_max.loc[mdd_date]

            peak_candidates = fund_data[fund_data.index <= mdd_date][fund_data >= peak_value * 0.9999]
            peak_date = peak_candidates.index[-1] if len(peak_candidates) > 0 else fund_data[fund_data.index <= mdd_date].idxmax()
            if len(peak_candidates) == 0:
                peak_value = fund_data.loc[peak_date]

            post_mdd = fund_data[fund_data.index > mdd_date]
            rec_mask = post_mdd >= peak_value
            if rec_mask.any():
                recovery_date = post_mdd[rec_mask].index[0]
                recovery_days = (recovery_date - mdd_date).days
                nav_at_recovery = fund_data.loc[recovery_date]
            else:
                recovery_date = None
                recovery_days = None
                nav_at_recovery = None

            recovery_matrix[fund][year] = recovery_days
            all_results.append({
                "Fondo_Code": fund,
                "Fondo": name_map.get(fund[:7], fund),
                "Año": year,
                "Peak_Date": peak_date,
                "Peak_NAV": peak_value,
                "MDD_Date": mdd_date,
                "MDD_NAV": nav_at_mdd,
                "DD_Pct": mdd_value * 100,
                "Recovery_Date": recovery_date,
                "Recovery_NAV": nav_at_recovery,
                "Recovery_Days": recovery_days,
            })

    if not all_results:
        return page + 1

    results_df = pd.DataFrame(all_results)
    recovery_df_temp = pd.DataFrame(recovery_matrix).T
    recovery_df_temp.index = [name_map.get(f[:7], f) for f in recovery_df_temp.index]
    recovery_df = recovery_df_temp.copy()
    recovery_df["Mediana"] = recovery_df.apply(
        lambda row: row.replace({None: 9999}).median(), axis=1
    )

    peak_groups = {}
    ref_counter = 1
    for _, row in results_df.iterrows():
        pk = row["Peak_Date"].strftime("%Y-%m")
        if pk not in peak_groups:
            peak_groups[pk] = {"ref": ref_counter, "peak_date": row["Peak_Date"],
                               "description": f"Peak {row['Peak_Date'].strftime('%b-%Y')}"}
            ref_counter += 1

    ref_map = {}
    for _, row in results_df.iterrows():
        pk = row["Peak_Date"].strftime("%Y-%m")
        fn = row["Fondo"]
        ref_map.setdefault(fn, {})[row["Año"]] = peak_groups[pk]["ref"]

    # ── Página 4A: resumen ──
    num_funds = len(funds)
    fig = plt.figure(figsize=(14, min(12, max(8, num_funds * 0.8 + 4))))
    gs = GridSpec(3, 1, figure=fig, height_ratios=[0.6, 0.3, 0.1], hspace=0.15)

    ax1 = fig.add_subplot(gs[0])
    ax1.axis("off")

    table_data = []
    display_names_local = [name_map.get(f[:7], f) for f in funds]

    for fn in display_names_local:
        row_days = [fn]
        for year in years:
            if year in recovery_df.columns:
                raw = recovery_df.loc[fn, year]
                # .loc can return a scalar or Series — normalize to scalar
                val = raw.iloc[0] if hasattr(raw, "iloc") else raw
            else:
                val = None
            if val is None or (isinstance(val, float) and np.isnan(val)):
                row_days.append("-")
            else:
                row_days.append(f"{int(val)}")
        med_raw = recovery_df.loc[fn, "Mediana"]
        med = med_raw.iloc[0] if hasattr(med_raw, "iloc") else med_raw
        row_days.append(f"{int(med)}" if (med is not None and not (isinstance(med, float) and np.isnan(med))) else "-")
        row_refs = [""] + [f"[{ref_map.get(fn,{}).get(y,'')}]" if ref_map.get(fn, {}).get(y) else "" for y in years] + [""]
        table_data.append(row_days)
        table_data.append(row_refs)

    col_headers = ["Fondo"] + [str(y) for y in years] + ["Mediana"]
    base_font = max(6, 10 - num_funds // 3)
    num_year_cols = len(years)
    col_widths = [0.35] + [0.45 / num_year_cols] * num_year_cols + [0.12]

    tbl = ax1.table(cellText=table_data, colLabels=col_headers,
                    cellLoc="center", loc="center",
                    colColours=[header_bg] * len(col_headers),
                    bbox=[0.02, 0.1, 0.96, 0.85])
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(base_font)
    tbl.scale(1, 1.8)

    for (i, j), cell in tbl.get_celld().items():
        cell.set_width(col_widths[j])
        if i == 0:
            cell.set_facecolor(header_bg)
            cell.get_text().set_color(header_txt)
            cell.get_text().set_weight("bold")
            cell.get_text().set_fontsize(base_font + 1)
            cell.set_height(0.08)
        else:
            fi = (i - 1) // 2
            cell.set_facecolor("#FFFFFF" if fi % 2 == 0 else "#F8F9FA")
            cell.set_height(0.045 if i % 2 == 1 else 0.03)
            if i % 2 == 1:
                cell.get_text().set_weight("bold")
                if j > 0 and j < len(col_headers) - 1:
                    try:
                        v = int(table_data[i - 1][j]) if table_data[i - 1][j] != "-" else None
                        if v is not None:
                            cell.set_facecolor("#D4EDDA" if v <= 100 else ("#FFF3CD" if v <= 500 else "#F8D7DA"))
                    except Exception:
                        pass
                if j == len(col_headers) - 1:
                    cell.set_facecolor("#E8F4FD")
            else:
                cell.get_text().set_fontsize(base_font - 1)
                cell.get_text().set_color("#666666")
                cell.get_text().set_style("italic")
            if j == 0:
                cell.get_text().set_ha("left")
            cell.set_edgecolor("#E0E0E0")
            cell.set_linewidth(0.5)
        cell.get_text().set_va("center")

    fig.text(0.5, 0.96, "RESILIENCIA: Días para Recuperar Peak Histórico tras MDD del Año",
             ha="center", fontsize=14, fontweight="bold", color="#2C3E50")

    ax2 = fig.add_subplot(gs[1])
    ax2.axis("off")
    ref_text = "REFERENCIAS - PEAKS HISTÓRICOS\n" + "─" * 70 + "\n\n"
    for pk, pi in sorted(peak_groups.items(), key=lambda x: x[1]["ref"]):
        ref_text += f"[{pi['ref']}] {pi['description']}    "
        if pi["ref"] % 4 == 0:
            ref_text += "\n"
    ax2.text(0.05, 0.95, ref_text, transform=ax2.transAxes, ha="left", va="top",
             fontsize=9, family="monospace",
             bbox=dict(boxstyle="round,pad=0.5", facecolor="white",
                       edgecolor="#06C264", linewidth=1.5, alpha=0.95))

    ax3 = fig.add_subplot(gs[2])
    ax3.axis("off")
    ax3.text(0.5, 0.5,
             "Verde = recuperación rápida (<100d) | Amarillo = moderada (100-500d) | Rojo = lenta (>500d)",
             transform=ax3.transAxes, ha="center", va="center",
             fontsize=9, style="italic", color="#555555")

    fig.text(0.5, 0.02, f"Página {page}A", ha="center", fontsize=9, color="#666666", style="italic")
    pdf.savefig(fig, bbox_inches="tight", facecolor="white", dpi=200)
    plt.close(fig)

    # ── Página 4B: detalle ──
    detail_data = []
    current_fund = None
    for _, row in results_df.iterrows():
        if row["Fondo"] != current_fund:
            detail_data.append([row["Fondo"], "", "", "", "", "", "", ""])
            current_fund = row["Fondo"]
        detail_data.append([
            f"  {row['Año']}",
            f"{row['DD_Pct']:.2f}%",
            row["Peak_Date"].strftime("%d-%b-%y"),
            f"{row['Peak_NAV']:.2f}",
            row["MDD_Date"].strftime("%d-%b-%y"),
            f"{row['MDD_NAV']:.2f}",
            row["Recovery_Date"].strftime("%d-%b-%y") if pd.notna(row["Recovery_Date"]) else "No recup.",
            f"{int(row['Recovery_Days'])}" if pd.notna(row["Recovery_Days"]) else "-",
        ])

    fig2 = plt.figure(figsize=(16, min(20, max(11, len(detail_data) * 0.35 + 2))))
    ax = fig2.add_subplot(111)
    ax.axis("off")
    col_headers2 = ["Fondo/Año", "DD %", "Peak Hist.\nFecha", "Peak\nNAV",
                    "MDD\nFecha", "MDD\nNAV", "Recovery\nFecha", "Días\nRecup"]
    col_w2 = [0.28, 0.08, 0.10, 0.10, 0.10, 0.10, 0.12, 0.08]

    dtbl = ax.table(cellText=detail_data, colLabels=col_headers2,
                    cellLoc="center", loc="center",
                    colColours=[header_bg] * len(col_headers2),
                    bbox=[0.02, 0.05, 0.96, 0.92])
    dtbl.auto_set_font_size(False)
    dtbl.set_fontsize(7)
    total_rows = len(detail_data) + 1
    dtbl.scale(1, 0.87 / total_rows * 12)

    for (i, j), cell in dtbl.get_celld().items():
        cell.set_width(col_w2[j])
        if i == 0:
            cell.set_facecolor(header_bg)
            cell.get_text().set_color(header_txt)
            cell.get_text().set_weight("bold")
            cell.get_text().set_fontsize(7)
        else:
            is_hdr = detail_data[i - 1][1] == ""
            if is_hdr:
                cell.set_facecolor("#E8F4FD")
                cell.get_text().set_weight("bold")
                cell.get_text().set_fontsize(7 if len(str(detail_data[i - 1][j])) <= 30 else 6)
            else:
                cell.set_facecolor("#FFFFFF" if i % 2 == 0 else "#F8F9FA")
                cell.get_text().set_fontsize(7)
                if j == 1:
                    try:
                        dv = float(detail_data[i - 1][j].replace("%", ""))
                        cell.set_facecolor("#F8D7DA" if dv <= -40 else ("#FFF3CD" if dv <= -25 else "#D4EDDA"))
                    except Exception:
                        pass
            cell.get_text().set_ha("left" if j == 0 else "center")
            cell.set_edgecolor("#E0E0E0")
            cell.set_linewidth(0.5)
        cell.get_text().set_va("center")

    fig2.text(0.5, 0.97, "Detalles Completos: Drawdown y Recuperación",
              ha="center", fontsize=14, fontweight="bold", color="#2C3E50")
    fig2.text(0.5, 0.02, f"Página {page}B", ha="center", fontsize=9, color="#666666", style="italic")
    pdf.savefig(fig2, bbox_inches="tight", facecolor="white", dpi=200)
    plt.close(fig2)

    return page + 1


# ─────────────────────────────────────────────
# Generador principal del PDF
# ─────────────────────────────────────────────
def generate_report(excel_bytes, progress_cb=None):
    def _prog(msg):
        if progress_cb:
            progress_cb(msg)

    _prog("Leyendo Excel…")
    df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=0)
    meta = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=1)

    df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
    df.set_index(df.columns[0], inplace=True)
    df.sort_index(inplace=True)
    df.ffill(inplace=True)

    fund_codes = df.columns.tolist()
    name_map = dict(zip(meta["Codigo"].astype(str).str[:7], meta["Nombre"]))
    isin_map = {}
    if "ISIN" in meta.columns:
        for _, row in meta.iterrows():
            isin_map[str(row["Codigo"])[:7]] = row["ISIN"]

    display_names = [fund_codes[0]] + [name_map.get(c[:7], c) for c in fund_codes[1:]]
    bm_col = fund_codes[0]
    funds = fund_codes[1:]
    fund_names = display_names[1:]

    _prog("Obteniendo datos de Financial Times…")
    ratings_data = []
    for fc in funds:
        ck = fc[:7]
        fn = name_map.get(ck, fc)
        isin = isin_map.get(ck)
        if isin:
            ms, cat, lr, fs, ld = get_ft_data(isin)
            time.sleep(0.5)
        else:
            ms = cat = lr = fs = ld = None
        ratings_data.append({"Fondo": fn, "ISIN": isin, "MS Stars": ms,
                              "Categoría": cat, "Lipper Rating": lr,
                              "Fund Size": fs, "Share Class Inception": ld})
    ratings_df = pd.DataFrame(ratings_data)

    _prog("Calculando métricas…")
    fecha_fin = df.index[-1]
    start_3y = fecha_fin - pd.DateOffset(years=3)
    start_5y = fecha_fin - pd.DateOffset(years=5)

    growth = df.divide(df.iloc[0]).multiply(100)
    growth_ret = growth - 100
    daily = df.pct_change().dropna()
    annual_rets = df.resample("YE").last().pct_change().dropna()
    annual_rets.index = annual_rets.index.year
    drawdown = (df - df.cummax()) / df.cummax()

    daily_bm = daily[bm_col]
    cap_data = {"Up Capture": [], "Down Capture": []}
    for code in funds:
        r = daily[code]
        up_bm = daily_bm[daily_bm > 0]
        dn_bm = daily_bm[daily_bm < 0]
        cap_data["Up Capture"].append(r[daily_bm > 0].mean() / up_bm.mean() if len(up_bm) > 0 else np.nan)
        cap_data["Down Capture"].append(r[daily_bm < 0].mean() / dn_bm.mean() if len(dn_bm) > 0 else np.nan)
    cap_df = pd.DataFrame(cap_data, index=fund_names).round(4)

    rr3, rr5 = [], []
    for code, name in zip(fund_codes, display_names):
        a3, v3, _, _ = compute_stats(df[code], start_3y, fecha_fin)
        a5, v5, _, _ = compute_stats(df[code], start_5y, fecha_fin)
        rr3.append((v3, a3))
        rr5.append((v5, a5))
    rr3_df = pd.DataFrame(rr3, index=display_names, columns=["Volatilidad", "Retorno"])
    rr5_df = pd.DataFrame(rr5, index=display_names, columns=["Volatilidad", "Retorno"])

    corr_m = daily.corr().round(4)

    _prog("Obteniendo tasa libre de riesgo (FRED)…")
    try:
        fred_key = os.environ.get("FRED_API_KEY", "")
        if fred_key and _FRED_AVAILABLE:
            from fredapi import Fred
            fred = Fred(api_key=fred_key)
            dgs5_series = fred.get_series("DGS5", observation_start=df.index[0], observation_end=df.index[-1])
            rf_daily = dgs5_series.dropna().div(100).div(252).reindex(daily.index, method="ffill")
        else:
            raise ValueError("No FRED key — using flat fallback")
    except Exception:
        rf_daily = pd.Series(0.04 / 252, index=daily.index)

    ann_bm = rr5_df.loc[display_names[0], "Retorno"]
    metrics_ext = {}
    for code, name in zip(fund_codes, display_names):
        r = daily[code]
        ann = rr5_df.loc[name, "Retorno"]
        rf_ann = rf_daily.mean() * 252
        vol = r.std() * np.sqrt(252)
        neg = r[r < 0]
        dd_dev = neg.std() * np.sqrt(252) if len(neg) > 0 else np.nan
        sharpe = (ann - rf_ann) / vol if vol else np.nan
        active = ann - ann_bm
        te = (r - daily[bm_col]).std() * np.sqrt(252)
        ir = active / te if te else np.nan
        sortino = (ann - rf_ann) / dd_dev if dd_dev else np.nan
        reg = pd.concat([daily[bm_col] - rf_daily, r - rf_daily], axis=1).dropna()
        if reg.shape[0] > 2 and reg.iloc[:, 0].nunique() > 1:
            slope, intercept = np.polyfit(reg.iloc[:, 0], reg.iloc[:, 1], 1)
            beta, alpha = slope, intercept * 252
        else:
            beta, alpha = np.nan, np.nan
        metrics_ext[name] = {
            "Retorno Ann": ann, "RF Rate": rf_ann, "Volatilidad Ann": vol,
            "Downside Dev": dd_dev, "Sharpe": sharpe, "Info Ratio": ir,
            "Sortino": sortino, "Beta": beta, "Alpha": alpha,
        }
    metrics_ext_df = pd.DataFrame(metrics_ext).T.round(6)

    # ─── Generar PDF en memoria ───
    _prog("Generando PDF…")
    buf = io.BytesIO()

    with PdfPages(buf) as pdf:
        page = 1

        def footer(fig):
            fig.text(0.5, 0.02, f"Página {page}", ha="center", fontsize=9,
                     color="#666666", style="italic")

        # ── PÁG 1: Tabla de Ratings ──
        _prog("Página 1: Ratings…")
        fig = plt.figure(figsize=(14, 8.27))
        ax = fig.add_subplot(111)
        ax.axis("off")
        rt = format_table_data(ratings_df.copy())
        td = rt.values.tolist()
        ch = rt.columns.tolist()
        cw = calculate_dynamic_widths(td, ch, len(rt))

        nf = len(rt)
        th = min(0.7, 0.15 + nf * 0.08)
        ty = 0.85 - th
        tbl = ax.table(cellText=td, colLabels=ch, cellLoc="center", loc="center",
                       colColours=[header_bg] * len(ch), bbox=[0.01, ty, 0.98, th])
        bfs = 9 if nf <= 5 else (8 if nf <= 8 else 7)
        hfs = bfs - 1
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(bfs)
        ch_h = max(0.08, 0.6 / (nf + 1))
        tbl.scale(1, ch_h * 25)

        hdr_adj = {2: "Morningstar\nStars", 4: "Lipper\nRating",
                   5: "Fund Size\n(USD Millones)", 6: "Share Class\nInception"}
        align_map = {0: ("left", "semibold"), 1: ("center", "monospace"),
                     2: ("center", "normal"), 3: ("left", None),
                     4: ("center", "normal"), 5: ("right", "normal"), 6: ("center", "normal")}

        for (i, j), cell in tbl.get_celld().items():
            cell.set_width(cw[j])
            if i == 0:
                cell.set_facecolor(header_bg)
                cell.get_text().set_color(header_txt)
                cell.get_text().set_weight("bold")
                cell.set_height(ch_h * 1.2)
                if j in hdr_adj:
                    cell.get_text().set_text(hdr_adj[j])
                    cell.get_text().set_fontsize(hfs - 1)
                else:
                    cell.get_text().set_fontsize(hfs)
                cell.set_edgecolor(header_bg)
                cell.set_linewidth(0)
            else:
                cell.set_facecolor("#FFFFFF" if i % 2 == 0 else "#F8F9FA")
                cell.get_text().set_fontsize(bfs)
                cell.set_height(ch_h)
                cell.set_edgecolor("#E0E0E0")
                cell.set_linewidth(0.5)
            if j in align_map:
                algn, wgt = align_map[j]
                cell.get_text().set_ha(algn)
                if i > 0 and wgt:
                    if wgt == "monospace":
                        cell.get_text().set_fontfamily("monospace")
                    else:
                        cell.get_text().set_fontweight(wgt)
                if j == 0 and i > 0:
                    fn_len = len(str(td[i - 1][j]))
                    if fn_len > 25:
                        cell.get_text().set_fontsize(max(6, bfs - 1))
                if j == 3 and i > 0 and len(str(td[i - 1][j])) > 30:
                    cell.get_text().set_fontsize(bfs - 1)
            cell.get_text().set_va("center")

        fig.text(0.5, 0.92, "Fondos y Características", ha="center",
                 fontsize=20, fontweight="bold", color="#2C3E50",
                 bbox=dict(boxstyle="round,pad=0.5", facecolor="#F8F9FA",
                           edgecolor=header_bg, linewidth=1.5))
        fig.text(0.5, 0.87, "Ratings, Clasificaciones y Características Principales",
                 ha="center", fontsize=12, color="#555555", style="italic")
        fig.add_artist(plt.Line2D([0.1, 0.9], [0.84, 0.84],
                                  color=header_bg, linewidth=2, transform=fig.transFigure))
        fig.text(0.5, 0.08, "Fuente: Financial Times | Morningstar Stars: 1-5 | Lipper Rating: 1-5 (5 = mejor)",
                 ha="center", fontsize=10, style="italic", color="#555555",
                 bbox=dict(boxstyle="round,pad=0.4", facecolor="#F8F9FA",
                           edgecolor=header_bg, alpha=0.9, linewidth=1))
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", facecolor="white", dpi=200)
        plt.close(fig)
        page += 1

        # ── PÁG 2: Total Return ──
        _prog("Página 2: Total Return…")
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        for i, (code, name) in enumerate(zip(fund_codes, display_names)):
            ls = ":" if code == bm_col else "-"
            lw = 1.5 if code == bm_col else 1.0
            ax.plot(growth_ret.index, growth_ret[code], label=name,
                    color=palette[i % len(palette)], linestyle=ls, linewidth=lw, alpha=0.85)
        apply_chart_style(ax, "Total Return")
        ax.set_ylabel("Retorno", fontweight="bold")
        ax.yaxis.set_major_formatter(mtick.FormatStrFormatter("%.0f%%"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(DateFormatter("%b-%y"))
        fig.autofmt_xdate(rotation=30, ha="right")
        ax.legend(fontsize=8, frameon=False)
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", facecolor="white", dpi=200)
        plt.close(fig)
        page += 1

        # ── PÁG 3: Retornos Anuales ──
        _prog("Página 3: Retornos Anuales…")
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        annual_rets.plot.bar(ax=ax,
                             color=[palette[i % len(palette)] for i in range(len(annual_rets.columns))],
                             width=0.75, alpha=0.85, edgecolor="white", linewidth=1.2)
        apply_chart_style(ax, "Retornos Anuales")
        ax.set_ylabel("Retorno", fontweight="bold")
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=1))
        handles, _ = ax.get_legend_handles_labels()
        ax.legend(handles, display_names, fontsize=8, frameon=False)
        for p in ax.patches:
            h = p.get_height()
            if abs(h) > 0.001:
                ax.text(p.get_x() + p.get_width() / 2,
                        h + 0.005 * np.sign(h),
                        f"{h * 100:.1f}%", ha="center",
                        va="bottom" if h >= 0 else "top",
                        fontsize=6, fontweight="bold", color="#333333")
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", facecolor="white", dpi=200)
        plt.close(fig)
        page += 1

        # ── PÁG 4: Retornos Comparativos por Período ──
        _prog("Página 4: Tabla de Retornos…")
        fecha_fin_t = df.index[-1]
        ep_month = pd.Timestamp(fecha_fin_t.year, fecha_fin_t.month, 1) - pd.DateOffset(days=1)
        ep_quarter = pd.Timestamp(fecha_fin_t.year, ((fecha_fin_t.month - 1) // 3) * 3 + 1, 1) - pd.DateOffset(days=1)
        ep_year = pd.Timestamp(fecha_fin_t.year, 1, 1) - pd.DateOffset(days=1)

        def simp_ret(s, start, end):
            sub = s[start:end]
            return (sub.iloc[-1] / sub.iloc[0] - 1) * 100 if len(sub) >= 2 else 0

        def ann_ret(s, start, end):
            sub = s[start:end]
            if len(sub) < 2:
                return 0
            total = sub.iloc[-1] / sub.iloc[0] - 1
            yrs = (sub.index[-1] - sub.index[0]).days / 365.25
            return ((1 + total) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0

        ret_rows = []
        for code, name in zip(fund_codes, display_names):
            ret_rows.append({
                "Fondo": name,
                "MTD": simp_ret(df[code], ep_month, fecha_fin_t),
                "QTD": simp_ret(df[code], ep_quarter, fecha_fin_t),
                "YTD": simp_ret(df[code], ep_year, fecha_fin_t),
                "1Y": ann_ret(df[code], fecha_fin_t - pd.DateOffset(years=1), fecha_fin_t),
                "3Y": ann_ret(df[code], fecha_fin_t - pd.DateOffset(years=3), fecha_fin_t),
                "5Y": ann_ret(df[code], fecha_fin_t - pd.DateOffset(years=5), fecha_fin_t),
            })
        ret_df = pd.DataFrame(ret_rows)

        nf2 = len(ret_df)
        fig = plt.figure(figsize=(14, min(11, max(8, nf2 * 0.8 + 3))))
        ax = fig.add_subplot(111)
        ax.axis("off")
        td2 = [[r["Fondo"], f"{r['MTD']:.2f}%", f"{r['QTD']:.2f}%", f"{r['YTD']:.2f}%",
                f"{r['1Y']:.2f}%", f"{r['3Y']:.2f}%", f"{r['5Y']:.2f}%"]
               for _, r in ret_df.iterrows()]
        ch2 = ["Fondo", "MTD", "QTD", "YTD", "1 Año", "3 Años", "5 Años"]
        h2c = {"MTD": "MTD", "QTD": "QTD", "YTD": "YTD", "1 Año": "1Y", "3 Años": "3Y", "5 Años": "5Y"}

        tbl2 = ax.table(cellText=td2, colLabels=ch2, cellLoc="center", loc="center",
                        colColours=[header_bg] * len(ch2),
                        bbox=[0.02, 0.15, 0.96, 0.75])
        bfs2 = max(8, 11 - nf2 // 3)
        tbl2.auto_set_font_size(False)
        tbl2.set_fontsize(bfs2)
        tbl2.scale(1, 2.5)
        cw2 = [0.35, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10]

        for (i, j), cell in tbl2.get_celld().items():
            cell.set_width(cw2[j])
            if i == 0:
                cell.set_facecolor(header_bg)
                cell.get_text().set_color(header_txt)
                cell.get_text().set_weight("bold")
                cell.set_height(0.08)
                cell.set_edgecolor(header_bg)
                cell.set_linewidth(0)
            else:
                if j == 0:
                    cell.set_facecolor("#FFFFFF" if i % 2 == 0 else "#F8F9FA")
                    cell.get_text().set_ha("left")
                    cell.get_text().set_weight("semibold")
                else:
                    col_name = h2c[ch2[j]]
                    col_vals = ret_df[col_name].values
                    cur_val = ret_df.iloc[i - 1][col_name]
                    if cur_val == max(col_vals):
                        cell.set_facecolor("#D4EDDA")
                    elif cur_val == min(col_vals):
                        cell.set_facecolor("#F8D7DA")
                    else:
                        cell.set_facecolor("#FFFFFF")
                    cell.get_text().set_weight("bold")
                cell.set_height(0.06)
                cell.set_edgecolor("#E0E0E0")
                cell.set_linewidth(0.5)
            cell.get_text().set_va("center")

        fig.text(0.5, 0.94, "Retornos Comparativos por Período",
                 ha="center", fontsize=16, fontweight="bold", color="#2C3E50")
        fig.text(0.5, 0.90, "MTD, QTD, YTD: Retornos totales  |  1Y, 3Y, 5Y: Retornos anualizados",
                 ha="center", fontsize=11, style="italic", color="#555555")
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", facecolor="white", dpi=200)
        plt.close(fig)
        page += 1

        # ── PÁGS 5A/5B: Drawdown ──
        _prog("Páginas 5A/5B: Drawdown y Recuperación…")
        page = generate_drawdown_pages(pdf, page, df, fund_codes, name_map)

        # ── PÁG 6: Drawdown Series ──
        _prog("Página 6: Drawdown Series…")
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        for i, (code, name) in enumerate(zip(fund_codes, display_names)):
            ls = ":" if code == bm_col else "-"
            ax.plot(drawdown.index, drawdown[code], label=name,
                    color=palette[i % len(palette)], linestyle=ls, linewidth=1.2)
        apply_chart_style(ax, "Drawdown Series")
        ax.set_ylabel("Drawdown")
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=1))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
        ax.xaxis.set_major_formatter(DateFormatter("%b-%y"))
        fig.autofmt_xdate(rotation=30, ha="right")
        ax.legend(fontsize=8, frameon=False)
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", dpi=200)
        plt.close(fig)
        page += 1

        # ── PÁG 7: Risk-Reward ──
        _prog("Página 7: Risk-Reward…")
        bm_a3, bm_v3, _, _ = compute_stats(df[bm_col], start_3y, fecha_fin)
        bm_a5, bm_v5, _, _ = compute_stats(df[bm_col], start_5y, fecha_fin)
        fig, axs = plt.subplots(ncols=2, figsize=(14, 6))
        for ax, rr_df, bm_v, bm_a, lbl in [
            (axs[0], rr3_df, bm_v3, bm_a3, "3 Años"),
            (axs[1], rr5_df, bm_v5, bm_a5, "5 Años"),
        ]:
            texts = []
            for i, f in enumerate(rr_df.index):
                x, y = rr_df.loc[f, ["Volatilidad", "Retorno"]]
                ax.scatter(x, y, color=palette[i % len(palette)], s=80)
                texts.append(ax.text(x, y, f, fontsize=8))
            ax.axhline(bm_a, color="grey", linestyle="--")
            ax.axvline(bm_v, color="grey", linestyle="--")
            ax.set_xlabel(f"Volatilidad Anualizada {lbl}")
            ax.set_ylabel(f"Retorno Anualizado {lbl}")
            ax.set_title(f"Risk-Reward {lbl}", weight="bold")
            ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=1))
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=1))
            adjust_text(texts, ax=ax, arrowprops=dict(arrowstyle="-", color="grey", lw=0.5))
        plt.tight_layout()
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", dpi=200)
        plt.close(fig)
        page += 1

        # ── PÁG 8: Capture Ratios ──
        _prog("Página 8: Capture Ratios…")
        fig = plt.figure(figsize=(11.69, 11.69))
        gs2 = GridSpec(2, 1, figure=fig, height_ratios=[1, 2], hspace=0.4)
        ax1 = fig.add_subplot(gs2[0])
        ax1.axis("off")
        cap_td = cap_df.reset_index().rename(columns={"index": "Fondos"})
        cap_fmt = cap_td.copy()
        for col in ["Up Capture", "Down Capture"]:
            cap_fmt[col] = (cap_fmt[col] * 100).round(1).map(lambda x: f"{x:.1f}%")
        p2 = ax1.table(cellText=cap_fmt.values.tolist(), colLabels=cap_fmt.columns.tolist(),
                       cellLoc="center", loc="center",
                       colColours=[header_bg] * len(cap_fmt.columns))
        p2.auto_set_font_size(False)
        p2.set_fontsize(8)
        p2.scale(1.2, 1)
        w2 = [0.3] + [0.7 / (len(cap_fmt.columns) - 1)] * (len(cap_fmt.columns) - 1)
        for (i, j), cell in p2.get_celld().items():
            if i == 0:
                cell.set_facecolor(header_bg)
                cell.get_text().set_color(header_txt)
            if j == 0:
                cell.get_text().set_ha("left")
            cell.set_width(w2[j])
        ax1.set_title("Capture Ratios vs Benchmark", pad=12, weight="bold", loc="center")

        ax2 = fig.add_subplot(gs2[1])
        texts = []
        for i, name in enumerate(fund_names):
            x, y = cap_df.loc[name, ["Up Capture", "Down Capture"]]
            ax2.scatter(x, y, color=palette[i % len(palette)], s=80)
            texts.append(ax2.text(x, y, name, fontsize=8))
        ax2.axhline(1.0, color=header_bg, linestyle="--")
        ax2.axvline(1.0, color=header_bg, linestyle="--")
        ax2.set_xlabel("Up-Capture")
        ax2.set_ylabel("Down-Capture")
        adjust_text(texts, ax=ax2)
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", dpi=200)
        plt.close(fig)
        page += 1

        # ── PÁG 9: Correlación ──
        _prog("Página 9: Correlación…")
        mask = np.triu(np.ones_like(corr_m, dtype=bool))
        fig, ax = plt.subplots(figsize=(8.27, 8))
        sns.heatmap(corr_m, mask=mask, annot=True, fmt=".2f", cmap="coolwarm",
                    vmin=-1, vmax=1, annot_kws={"size": 8},
                    cbar_kws={"shrink": 0.6, "label": "Correlación"},
                    linewidths=0.5, linecolor="white", ax=ax)
        ax.set_title("Matriz de Correlación de Retornos Diarios", weight="bold", pad=12)
        ax.set_xticklabels(display_names, rotation=45, ha="right")
        ax.set_yticklabels(display_names, rotation=0)
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", dpi=200)
        plt.close(fig)
        page += 1

        # ── PÁG 10: Métricas Extendidas ──
        _prog("Página 10: Métricas Extendidas…")
        tbl8 = metrics_ext_df.copy()
        def fmt_pct(x):
            try:
                return f"{float(x)*100:.2f}%"
            except Exception:
                return "-"
        def fmt_ratio(x):
            try:
                return f"{float(x):.2f}"
            except Exception:
                return "-"
        for col in ["Retorno Ann", "RF Rate", "Volatilidad Ann", "Downside Dev", "Alpha"]:
            tbl8[col] = tbl8[col].apply(fmt_pct)
        for col in ["Sharpe", "Info Ratio", "Sortino", "Beta"]:
            tbl8[col] = tbl8[col].apply(fmt_ratio)

        tbl8_T = tbl8.T
        if len(tbl8_T.columns) > 0:
            fc0 = tbl8_T.columns[0]
            tbl8_T = tbl8_T.rename(columns={fc0: f"{fc0}\n(Benchmark)"})
            tbl8_T.loc["Info Ratio", tbl8_T.columns[0]] = "-"
            tbl8_T.loc["Beta", tbl8_T.columns[0]] = "-"
            tbl8_T.loc["Alpha", tbl8_T.columns[0]] = "-"
        tbl8_T = tbl8_T.reset_index().rename(columns={"index": "Métricas"})
        td8 = tbl8_T.values.tolist()
        ch8 = tbl8_T.columns.tolist()

        nf3 = len(ch8) - 1
        fw = 11.69 if nf3 <= 5 else (14 if nf3 <= 8 else 16)
        bfs3 = 8 if nf3 <= 5 else (7 if nf3 <= 8 else 6)

        fig, ax = plt.subplots(figsize=(fw, len(tbl8_T) * 0.35 + 2))
        ax.axis("off")
        p8 = ax.table(cellText=td8, colLabels=ch8, cellLoc="center", loc="center",
                      colColours=[header_bg] * len(ch8))
        p8.auto_set_font_size(False)
        p8.set_fontsize(bfs3)
        p8.scale(1, 1.8)

        met_w = 0.15
        rem = 1.0 - met_w
        units = [max(1.0, min(2.0, len(h) / 20)) for h in ch8[1:]]
        tot_u = sum(units)
        cw8 = [met_w] + [u / tot_u * rem for u in units]

        for (i, j), cell in p8.get_celld().items():
            if j < 0:
                continue
            cell.set_width(cw8[j])
            if i == 0:
                cell.set_facecolor(header_bg)
                cell.get_text().set_color(header_txt)
                cell.get_text().set_weight("bold")
                cell.get_text().set_fontsize(bfs3 - 1)
                cell.set_height(0.1)
            elif j == 1:
                cell.set_facecolor("#E8F4FD")
                cell.set_height(0.08)
            else:
                cell.set_facecolor("#FFFFFF" if i % 2 == 0 else "#F8F9FA")
                cell.set_height(0.08)
            cell.get_text().set_ha("left" if j == 0 else "center")
            if j == 0:
                cell.get_text().set_weight("semibold")
            cell.set_edgecolor("#E0E0E0")
            cell.set_linewidth(0.5)
            cell.get_text().set_va("center")

        ax.set_title("Métricas Extendidas (5Y)", pad=20, weight="bold", loc="center")
        footer(fig)
        pdf.savefig(fig, bbox_inches="tight", dpi=200)
        plt.close(fig)

    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────
# Worker thread
# ─────────────────────────────────────────────
def run_job(job_id, excel_bytes):
    def progress(msg):
        with JOBS_LOCK:
            JOBS[job_id]["progress"] = msg

    try:
        with JOBS_LOCK:
            JOBS[job_id]["status"] = "running"
            JOBS[job_id]["progress"] = "Iniciando…"

        pdf_bytes = generate_report(excel_bytes, progress_cb=progress)

        with JOBS_LOCK:
            JOBS[job_id]["status"] = "done"
            JOBS[job_id]["pdf_bytes"] = pdf_bytes
            JOBS[job_id]["progress"] = "Listo"
    except Exception as e:
        tb = traceback.format_exc()
        with JOBS_LOCK:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["error"] = str(e)
            JOBS[job_id]["traceback"] = tb
        print(f"[ERROR] Job {job_id}: {e}\n{tb}")


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename.endswith((".xlsx", ".xls")):
        return jsonify({"error": "Solo se aceptan archivos Excel (.xlsx / .xls)"}), 400

    excel_bytes = f.read()
    job_id = str(uuid.uuid4())
    with JOBS_LOCK:
        JOBS[job_id] = {"status": "queued", "pdf_bytes": None, "error": None, "progress": "En cola…"}

    t = threading.Thread(target=run_job, args=(job_id, excel_bytes), daemon=True)
    t.start()

    return jsonify({"job_id": job_id})


@app.route("/status/<job_id>")
def status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404
    return jsonify({
        "status": job["status"],
        "progress": job.get("progress", ""),
        "error": job.get("error"),
    })


@app.route("/download/<job_id>")
def download(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "No disponible"}), 404
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(
        io.BytesIO(job["pdf_bytes"]),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"fund_report_{ts}.pdf",
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
