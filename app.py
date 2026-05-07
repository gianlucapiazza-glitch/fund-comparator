# ═══════════════════════════════════════════════════════════
# REEMPLAZAR desde "fund_codes = df.columns.tolist()" hasta
# antes de "_prog("Calculando métricas…")"
# ═══════════════════════════════════════════════════════════

import re

def _norm(s):
    """Normaliza strings para matching: lowercase, sin paréntesis tipo (L1)/(R2),
    sin fechas, sin puntuación, espacios colapsados."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).lower()
    s = re.sub(r'\([lry]\d+\)', ' ', s)              # (L1), (R2), (Y1)
    s = re.sub(r'\bon \d{1,2}/\d{1,2}/\d{2,4}\b', ' ', s)  # "on 5/6/26"
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _is_benchmark_or_index(s):
    """Detecta si una columna es un benchmark/índice (no un fondo con ISIN)."""
    if not s:
        return False
    return bool(re.search(r'\b(index|benchmark|msci|s&p|spx|bloomberg agg)\b',
                          str(s), re.IGNORECASE))

fund_codes = df.columns.tolist()

# ── Construir maps con MÚLTIPLES claves por fila de meta ──
# Cada fila puede ser referenciada por: ticker corto, nombre completo Fondo,
# nombre completo Nombre, código completo, o cualquiera de esas normalizadas.
name_map = {}   # ticker_corto → Nombre (se mantiene por compat con resto del código)
isin_map = {}   # ticker_corto → ISIN  (idem)

# Map auxiliar para resolver fund_code → ticker_corto
resolve_map = {}  # clave_normalizada → ticker_corto

for _, row in meta.iterrows():
    codigo_full = str(row.get("Codigo", "")).strip()
    if not codigo_full:
        continue
    ticker = codigo_full.split()[0][:7]  # primer token, max 7 chars
    nombre = row.get("Nombre", "")
    fondo = row.get("Fondo", "")
    isin = row.get("ISIN") if "ISIN" in meta.columns else None

    name_map[ticker] = nombre
    if isin is not None and not (isinstance(isin, float) and pd.isna(isin)):
        isin_map[ticker] = isin

    # Registrar todas las claves posibles para este ticker
    for key_raw in [codigo_full, ticker, nombre, fondo]:
        if not key_raw or (isinstance(key_raw, float) and pd.isna(key_raw)):
            continue
        key_norm = _norm(key_raw)
        if key_norm and key_norm not in resolve_map:
            resolve_map[key_norm] = ticker
        # también el raw uppercase para tickers
        if isinstance(key_raw, str):
            resolve_map[key_raw.upper().strip()] = ticker

def resolve_ticker(fund_code):
    """
    Resuelve un fund_code (columna del df de NAVs) al ticker corto del meta.
    Estrategias en orden:
      1) Match exacto (raw uppercase)
      2) Match exacto normalizado
      3) Si fund_code tiene ' - ', probar con la parte después
      4) Contención: alguna key normalizada está contenida en fund_code, o viceversa
    Devuelve ticker o None.
    """
    if not fund_code:
        return None

    # 1) Exact raw
    raw_up = str(fund_code).upper().strip()
    if raw_up in resolve_map:
        return resolve_map[raw_up]

    # 2) Exact normalizado
    norm = _norm(fund_code)
    if norm in resolve_map:
        return resolve_map[norm]

    # 3) Parte después de ' - ' (típico: "BlackRock Strategic Funds - EM Equity ...")
    parts = str(fund_code).split(' - ', 1)
    if len(parts) == 2:
        norm_specific = _norm(parts[1])
        if norm_specific in resolve_map:
            return resolve_map[norm_specific]
    else:
        norm_specific = norm

    # 4) Contención bidireccional con tokens (evitamos matches espurios cortos)
    best_ticker, best_score = None, 0
    for key_norm, ticker in resolve_map.items():
        if len(key_norm) < 8:  # skip claves muy cortas (tickers solos), ya probados arriba
            continue
        if key_norm in norm or norm_specific in key_norm or key_norm in norm_specific:
            score = len(key_norm)  # priorizamos el match más largo (más específico)
            if score > best_score:
                best_score, best_ticker = score, ticker

    return best_ticker

print(f"DEBUG MAPS: name_map size={len(name_map)}, isin_map size={len(isin_map)}, "
      f"resolve_map size={len(resolve_map)}", flush=True)
print(f"DEBUG FUND_CODES: {fund_codes}", flush=True)

# ── Resolver cada columna de NAVs a su ticker ──
fc_to_ticker = {}
for fc in fund_codes:
    if _is_benchmark_or_index(fc):
        fc_to_ticker[fc] = None  # benchmark, no buscar ISIN
        continue
    t = resolve_ticker(fc)
    fc_to_ticker[fc] = t
    if t is None:
        print(f"[WARN] No pude resolver ticker para columna: {fc!r}", flush=True)
    else:
        print(f"[OK] {fc!r} → ticker={t}", flush=True)

# ── display_names: usar nombre del meta si lo resolvimos, sino la columna original ──
display_names = []
for fc in fund_codes:
    t = fc_to_ticker.get(fc)
    if t and t in name_map:
        display_names.append(name_map[t])
    else:
        display_names.append(fc)

bm_col = fund_codes[0]
funds = fund_codes[1:]
fund_names = display_names[1:]

_prog("Obteniendo datos de Financial Times…")
ratings_data = []
for fc in funds:
    t = fc_to_ticker.get(fc)
    fn = name_map.get(t, fc) if t else fc
    isin = isin_map.get(t) if t else None
    print(f"DEBUG MAP: fund_code={fc!r} | ticker={t} | isin={isin}", flush=True)
    if isin:
        ms, cat, lr, fs, ld = get_ft_data(isin)
        time.sleep(0.5)
    else:
        if _is_benchmark_or_index(fc):
            print(f"[INFO] Skip FT lookup para benchmark/índice: {fn}", flush=True)
        else:
            print(f"[WARN] ISIN no encontrado para {fn}", flush=True)
        ms = cat = lr = fs = ld = None
    ratings_data.append({"Fondo": fn, "ISIN": isin, "MS Stars": ms,
                          "Categoría": cat, "Lipper Rating": lr,
                          "Fund Size": fs, "Share Class Inception": ld})
ratings_df = pd.DataFrame(ratings_data)
