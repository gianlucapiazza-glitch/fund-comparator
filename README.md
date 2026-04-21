# Fund Comparator — Informe estilo Morningstar

App web que genera un PDF de análisis institucional de fondos de inversión a partir de un Excel con NAVs e ISINs.

## Páginas del informe

| Pág | Contenido |
|-----|-----------|
| 1 | Fondos: Ratings Morningstar/Lipper, Fund Size, Inception |
| 2 | Total Return — serie histórica |
| 3 | Retornos Anuales — barras por año |
| 4 | Retornos por período: MTD, QTD, YTD, 1Y, 3Y, 5Y |
| 5A | Resiliencia: días de recuperación tras MDD por año |
| 5B | Detalle completo de drawdowns y recuperación |
| 6 | Drawdown Series — serie temporal |
| 7 | Risk-Reward scatter 3Y y 5Y |
| 8 | Capture Ratios vs benchmark |
| 9 | Matriz de Correlación (heatmap) |
| 10 | Métricas extendidas: Sharpe, Sortino, Alpha, Beta, IR… |

---

## Formato del Excel

El archivo debe tener **2 hojas**:

### Hoja 1 — NAV Series
| Fecha | BENCHMARK | FONDO_01 | FONDO_02 | … |
|-------|-----------|----------|----------|---|
| 2020-01-01 | 100.00 | 100.00 | 100.00 | |

- Primera columna: fechas (cualquier formato que pandas reconozca)
- Primera columna de datos: **benchmark** (siempre primero)
- El código de fondo debe coincidir con los primeros 7 caracteres del `Codigo` en la Hoja 2

### Hoja 2 — Metadatos
| Codigo | Nombre | ISIN |
|--------|--------|------|
| FONDO_0 | Nombre Completo del Fondo | LU1234567890 |

- `Codigo`: los primeros 7 caracteres se usan como clave
- `ISIN`: se usa para obtener datos de Financial Times (opcional; si no se tiene, se omite)

---

## Instalación local

```bash
# 1. Clonar / descomprimir
cd fund-comparator

# 2. Crear entorno virtual
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Ejecutar
python app.py
```

Abrir http://localhost:5000

---

## Deploy en Railway

### Opción A — GitHub (recomendada)

1. Subir este directorio a un repositorio GitHub
2. Ir a [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
3. Seleccionar el repositorio
4. Railway detectará automáticamente el `Procfile` y desplegará

### Opción B — Railway CLI

```bash
npm install -g @railway/cli
railway login
railway init
railway up
```

### Variables de entorno (opcionales)

No se requieren variables obligatorias. Railway inyecta `$PORT` automáticamente.

---

## Notas técnicas

- La generación usa **threads** para no bloquear el servidor
- El PDF se genera en memoria (no se guarda en disco)
- El timeout de gunicorn es 600s para permitir PDFs con muchos fondos
- `MPLBACKEND=Agg` es obligatorio en servidor (sin GUI)
- Los datos de FT se obtienen via web scraping; si FT bloquea, los campos de ratings quedan como `-`
- La tasa libre de riesgo usa DGS5 de FRED; si falla, usa 4% anual como fallback
