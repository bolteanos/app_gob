# simple_streamlit_app.py (columnas reordenadas)
"""
App para filtrar y descargar series de tiempo de https://datos.gob.ar

Novedad de esta versión
-----------------------
* Las columnas se muestran en el orden preferido por el usuario:
  1. consultas_90_dias
  2. dataset_descripcion
  3. dataset_fuente
  4. dataset_tema
  5. serie_indice_final
  6. serie_indice_inicio
  7. serie_unidades
  8. serie_valor_ultimo
  9. Resto de columnas en el orden original.
"""

from __future__ import annotations

import io
import os
import re
import sys
import subprocess
import urllib.parse
from datetime import date
from typing import Any

import pandas as pd

# ─────────────────────────────────────────────
# 1. Dependencias
# ─────────────────────────────────────────────
try:
    import streamlit as st  # type: ignore
except ModuleNotFoundError:
    sys.exit("❌ Falta streamlit. Instálala con pip install streamlit.")

try:
    import requests  # type: ignore
except ModuleNotFoundError:
    requests = None  # type: ignore

try:
    from slugify import slugify  # type: ignore
except ModuleNotFoundError:
    def slugify(txt: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", txt.lower()).strip("-")

_cache = st.cache_data if hasattr(st, "cache_data") else st.cache

# ─────────────────────────────────────────────
# 2. Configuración global
# ─────────────────────────────────────────────
CATALOG_URL = "https://apis.datos.gob.ar/series/api/dump/series-tiempo-metadatos.csv"
API_BASE = "https://apis.datos.gob.ar/series/api/series?format=csv&"

# Carpeta “descargas” junto al script
APP_DIR = os.path.dirname(__file__)
DL_DIR = os.path.join(APP_DIR, "descargas")
os.makedirs(DL_DIR, exist_ok=True)

# Carpeta para scripts asociados
SCRIPTS_DIR = os.path.join(APP_DIR, "scripts_series")
os.makedirs(SCRIPTS_DIR, exist_ok=True)

_EMBEDDED_CSV = (
    "serie_id,serie_titulo,dataset_tema,dataset_descripcion,serie_descripcion,consultas_90_dias\n"
    "TEST123,Serie de ejemplo,Actividad,Dataset de prueba,Desc corta,999\n"
)

st.set_page_config(page_title="Descarga series – datos.gob.ar", layout="wide")

# ─────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────
st.markdown(
    """
    <style>
    ::-webkit-scrollbar { width: 12px; height: 12px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background-color: rgba(0, 0, 0, 0.3); border-radius: 6px; border: 3px solid transparent; }
    div.stTextInput > div > input:focus,
    div.stSelectbox > div > div > div > select:focus {
        outline: 2px solid #ADD8E6 !important;
        background-color: rgba(173, 216, 230, 0.2) !important;
    }
    .ag-cell-range-selected { border: 2px solid #ADD8E6 !important; }
    .ag-range-selection .ag-selection-highlight { background-color: rgba(173, 216, 230, 0.3) !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Descargar series de tiempo de datos.gob.ar")

# ─────────────────────────────────────────────
# 3. Helpers
# ─────────────────────────────────────────────
def build_api_url(serie_id: str, start: str | None = None, end: str | None = None) -> str:
    q: dict[str, Any] = {"ids": serie_id}
    if start:
        q["start_date"] = start
    if end:
        q["end_date"] = end
    return API_BASE + urllib.parse.urlencode(q)

@_cache(show_spinner="Descargando catálogo …")
def load_catalog() -> pd.DataFrame:
    if requests is None:
        st.warning("No se encontró *requests*. Se usará el CSV embebido.")
        csv_bytes = _EMBEDDED_CSV.encode()
    else:
        try:
            r = requests.get(CATALOG_URL, timeout=60)
            r.raise_for_status()
            csv_bytes = r.content
        except Exception as e:
            st.warning(f"Fallo descarga catálogo → {e}. Se usará versión mínima.")
            csv_bytes = _EMBEDDED_CSV.encode()
    df = pd.read_csv(io.BytesIO(csv_bytes), encoding="utf-8", on_bad_lines="skip", low_memory=False)
    if "consultas_90_dias" not in df.columns:
        df["consultas_90_dias"] = 0
    df["consultas_90_dias"] = pd.to_numeric(df["consultas_90_dias"], errors="coerce").fillna(0)
    return df


def download_series(url: str) -> bytes:
    if requests is None:
        raise RuntimeError("Necesitas la librería *requests* para descargar las series.")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

# ─────────────────────────────────────────────
# 4. Filtros y orden
# ─────────────────────────────────────────────
meta = load_catalog()

st.sidebar.header("Filtros globales")
all_topics = sorted(meta["dataset_tema"].dropna().unique())
sel_topic = st.sidebar.selectbox("Tema", ["Todos"] + all_topics)
filtered = meta if sel_topic == "Todos" else meta[meta["dataset_tema"] == sel_topic]

query = st.sidebar.text_input("🔍 Buscar en título")
if query:
    filtered = filtered[filtered["serie_titulo"].str.contains(query, case=False, na=False)]
cols_for_sort = list(filtered.columns)
order_col = st.sidebar.selectbox(
    "Ordenar por",
    cols_for_sort,
    index=cols_for_sort.index("consultas_90_dias") if "consultas_90_dias" in cols_for_sort else 0,
)
asc = st.sidebar.checkbox("Ascendente", value=False)
filtered = filtered.sort_values(order_col, ascending=asc, na_position="last")

# ─────────────────────────────────────────────
# 5. Reordenar columnas para la vista
# ─────────────────────────────────────────────
preferred_order = [
    "consultas_90_dias",
    "dataset_descripcion",
    "dataset_fuente",
    "dataset_tema",
    "serie_indice_final",
    "serie_indice_inicio",
    "serie_unidades",
    "serie_valor_ultimo",
]
existing_preferred = [c for c in preferred_order if c in filtered.columns]
remaining_cols = [c for c in filtered.columns if c not in existing_preferred]
filtered = filtered[existing_preferred + remaining_cols]

# ─────────────────────────────────────────────
# 6. Mostrar tabla completa
# ─────────────────────────────────────────────
st.subheader(f"Catálogo filtrado ({len(filtered)} series)")
st.dataframe(filtered, use_container_width=True, height=600)

# ─────────────────────────────────────────────
# 7. Descarga de series
# ─────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.header("Descarga de series")

fila_labels = ["— Elige una —"] + [str(idx) for idx in filtered.index]
sel_fila = st.sidebar.selectbox("Número de fila", fila_labels)
st.sidebar.caption("Typea o selecciona la fila")

start_date = st.sidebar.text_input("Fecha inicio", "", autocomplete="off")
end_date = st.sidebar.text_input("Fecha fin", "", autocomplete="off")
st.sidebar.caption("Formato de fecha: YYYY-MM-DD")

if sel_fila != "— Elige una —":
    idx = int(sel_fila)
    serie_row = filtered.loc[idx]
    sel_id = serie_row["serie_id"]
    start = start_date.strip() or None
    end = end_date.strip() or None
    api_url = build_api_url(sel_id, start=start, end=end)
    base_slug = slugify(serie_row.serie_titulo)
    if start or end:
        parts: list[str] = []
        if start:
            parts.append(start)
        if end:
            parts.append(end)
        suffix = "_".join(parts)
        fname = f"{base_slug}_{suffix}.csv"
    else:
        fname = f"{base_slug}.csv"

    if st.sidebar.button("💾 Guardar en servidor"):
        try:
            content = download_series(api_url)
            file_path = os.path.join(DL_DIR, fname)
            with open(file_path, "wb") as f:
                f.write(content)
            st.sidebar.success(f"✅ Guardado en servidor: descargas/{fname}")

            script_name = f"{base_slug}.py"
            script_path = os.path.join(SCRIPTS_DIR, script_name)
            if not os.path.exists(script_path):
                with open(script_path, "w", encoding="utf-8") as sf:
                    sf.write(f"# {script_name}\n")
                    sf.write(f"# Script asociado a la serie '{serie_row.serie_titulo}'\n")
                    sf.write(f"# Creado el {date.today().isoformat()}\n\n")
                    sf.write('"""\n')
                    sf.write("¿Por qué necesito una ruta robusta en este contexto?\n")
                    sf.write("- Permite calcular rutas relativas al script, sin depender del directorio de ejecución.\n")
                    sf.write("- Usa os.path.dirname(__file__) para construir rutas siempre válidas.\n")
                    sf.write("- Evita errores al mover la app o cambiar de máquina.\n")
                    sf.write('"""\n\n')
                    sf.write("# Ejemplos de rutas robustas:\n")
                    sf.write("# base = os.path.splitext(os.path.basename(__file__))[0]\n")
                    sf.write("# output_dir = os.path.join(os.path.dirname(__file__), 'series_transformadas')\n")
                    sf.write("# os.makedirs(output_dir, exist_ok=True)\n")
                    sf.write("\n")
                    sf.write('"""\n')
                    sf.write("¿Qué son los requisitos?\n")
                    sf.write("- Son los requisitos necesarios para ejecutar este script.\n")
                    sf.write("- Ejemplos: serie IPC actualizada, un archivo o recurso determinado.\n")
                    sf.write("- SON OPCIONALES. Solo se mostrarán en la aplicación antes de que inicies la descarga.\n")
                    sf.write('"""\n\n')
                    sf.write("import os\n")
                    sf.write("import json\n\n")
                    sf.write("# Crear carpeta requisitos_script y JSON de requisitos vacío\n")
                    sf.write("base_dir = os.path.dirname(__file__)\n")
                    sf.write("requisitos_dir = os.path.join(base_dir, 'requisitos_script')\n")
                    sf.write("os.makedirs(requisitos_dir, exist_ok=True)\n\n")
                    sf.write("script_name = os.path.splitext(os.path.basename(__file__))[0]\n")
                    sf.write("json_path = os.path.join(requisitos_dir, f\"{script_name}_requisitos.json\")\n\n")
                    sf.write("requisitos_data = {\n")
                    sf.write(f"    'serie_titulo': \"{serie_row.serie_titulo}\",\n")
                    sf.write("    'requisitos': []\n")
                    sf.write("}\n\n")
                    sf.write("with open(json_path, 'w', encoding='utf-8') as rf:\n")
                    sf.write("    json.dump(requisitos_data, rf, ensure_ascii=False, indent=4)\n")
                    sf.write("# ----------------------------------------------------------------------------------------------\n")
                    sf.write("# TODO: Agrega tus manipulaciones aquí\n")
                    sf.write("# ----------------------------------------------------------------------------------------------")
                st.sidebar.success(f"✅ Script creado: scripts_series/{script_name}")

            result = subprocess.run(
                [sys.executable, script_path, file_path],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                st.sidebar.success("✅ Script ejecutado correctamente")
            else:
                st.sidebar.error(f"⚠️ Error al ejecutar script:\n{result.stderr}")
        except Exception as e:
            st.sidebar.error(f"Error al guardar o ejecutar en servidor: {e}")

    try:
        st.sidebar.download_button(
            label=f"⬇️ Descargar en local: {fname}",
            data=download_series(api_url),
            file_name=fname,
            mime="text/csv",
        )
    except Exception as e:
        st.sidebar.error(f"Error en descarga local: {e}")
else:
    st.sidebar.info("Selecciona una fila para activar las opciones de descarga.")

# ─────────────────────────────────────────────
# 8. CLI tests
# ─────────────────────────────────────────────
if __name__ == "__main__":
    assert build_api_url("ABC").endswith("ids=ABC")
    print("✔️ Tests OK.")
