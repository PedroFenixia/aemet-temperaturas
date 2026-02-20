#!/usr/bin/env python3
"""
AEMET Temperaturas - Recolector diario de temperaturas de TODOS los municipios
================================================================================
Recoge las temperaturas mín/máx previstas para hoy de los ~8.100 municipios
españoles usando la API OpenData de AEMET. Exporta a JSON acumulado (7 días).

Modos:
    python3 aemet_temperaturas.py                    # Todos los municipios (modo completo)
    python3 aemet_temperaturas.py --solo-capitales   # Solo 53 capitales (rápido)
    python3 aemet_temperaturas.py --provincia Madrid # Solo una provincia
    python3 aemet_temperaturas.py --api-key TU_KEY   # Usa una API key específica

Configuración:
    1. Obtén tu API key gratuita en: https://opendata.aemet.es/centrodedescargas/altaUsuario
    2. Guárdala en el archivo 'aemet_api_key.txt' junto a este script
       o pásala con --api-key
       o defínela como variable de entorno AEMET_API_KEY
"""

import json
import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

import requests

# --- Configuración ---
SCRIPT_DIR = Path(__file__).parent.resolve()
JSON_FILE = SCRIPT_DIR / "data.json"
MUNICIPIOS_CACHE = SCRIPT_DIR / "municipios_cache.json"
API_KEY_FILE = SCRIPT_DIR / "aemet_api_key.txt"
LOG_FILE = SCRIPT_DIR / "aemet_temperaturas.log"
API_BASE = "https://opendata.aemet.es/opendata/api"

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Mapa de código de provincia a nombre de provincia
PROVINCIAS = {
    "01": "Álava", "02": "Albacete", "03": "Alicante", "04": "Almería",
    "05": "Ávila", "06": "Badajoz", "07": "Baleares", "08": "Barcelona",
    "09": "Burgos", "10": "Cáceres", "11": "Cádiz", "12": "Castellón",
    "13": "Ciudad Real", "14": "Córdoba", "15": "A Coruña", "16": "Cuenca",
    "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
    "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León",
    "25": "Lleida", "26": "La Rioja", "27": "Lugo", "28": "Madrid",
    "29": "Málaga", "30": "Murcia", "31": "Navarra", "32": "Ourense",
    "33": "Asturias", "34": "Palencia", "35": "Las Palmas", "36": "Pontevedra",
    "37": "Salamanca", "38": "S.C. Tenerife", "39": "Cantabria", "40": "Segovia",
    "41": "Sevilla", "42": "Soria", "43": "Tarragona", "44": "Teruel",
    "45": "Toledo", "46": "Valencia", "47": "Valladolid", "48": "Bizkaia",
    "49": "Zamora", "50": "Zaragoza", "51": "Ceuta", "52": "Melilla",
}

# Capitales de provincia (códigos INE)
CAPITALES_CODIGOS = {
    "15030", "02003", "03014", "04013", "01059", "33044", "05019", "06015",
    "07040", "08019", "48020", "09059", "10037", "11012", "39075", "12040",
    "51001", "13034", "14021", "16078", "20069", "17079", "18087", "19130",
    "21041", "22125", "23050", "24089", "25120", "27028", "28079", "29067",
    "52001", "30030", "31201", "32054", "34120", "35016", "36038", "26089",
    "37274", "38038", "40194", "41091", "42173", "43148", "44216", "45168",
    "46250", "47186", "49275", "50297",
}


def get_api_key(cli_key=None):
    """Obtiene la API key de AEMET desde env var, archivo o argumento CLI."""
    if cli_key:
        return cli_key.strip()
    env_key = os.environ.get("AEMET_API_KEY", "").strip()
    if env_key:
        return env_key
    if API_KEY_FILE.exists():
        key = API_KEY_FILE.read_text().strip()
        if key:
            return key
    log.error(
        "No se encontró API key de AEMET.\n"
        "1. Regístrate gratis en: https://opendata.aemet.es/centrodedescargas/altaUsuario\n"
        "2. Guarda tu key en: %s\n"
        "   o usa: python3 aemet_temperaturas.py --api-key TU_KEY\n"
        "   o define la variable de entorno AEMET_API_KEY",
        API_KEY_FILE
    )
    sys.exit(1)


def aemet_request(endpoint, api_key, params=None, max_retries=3):
    """Hace una petición a la API de AEMET (doble salto: metadata -> datos)."""
    headers = {"api_key": api_key}
    url = f"{API_BASE}{endpoint}"

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning("Rate limit (429), esperando %ds...", wait)
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                log.debug("Error API: %s -> %d", endpoint, resp.status_code)
                return None

            meta = resp.json()
            if meta.get("estado") != 200:
                return None

            datos_url = meta.get("datos")
            if not datos_url:
                return None

            resp2 = requests.get(datos_url, timeout=30)
            if resp2.status_code != 200:
                return None

            return resp2.json()

        except requests.RequestException:
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            return None
        except json.JSONDecodeError:
            return None

    return None


def obtener_municipios(api_key):
    """
    Obtiene la lista completa de municipios de AEMET, agrupados por provincia.
    Usa caché local para no repetir la petición cada día.
    """
    # Usar caché si existe y es de hoy
    if MUNICIPIOS_CACHE.exists():
        try:
            cache = json.loads(MUNICIPIOS_CACHE.read_text(encoding="utf-8"))
            if cache.get("fecha") == datetime.now().strftime("%Y-%m-%d"):
                log.info("Usando caché de municipios (%d municipios)", cache.get("total", 0))
                return cache["provincias"]
        except (json.JSONDecodeError, KeyError):
            pass

    log.info("Descargando lista de municipios de AEMET...")
    datos = aemet_request("/maestro/municipios", api_key)
    if not datos:
        log.error("No se pudo obtener lista de municipios")
        # Intentar usar caché antigua
        if MUNICIPIOS_CACHE.exists():
            cache = json.loads(MUNICIPIOS_CACHE.read_text(encoding="utf-8"))
            return cache.get("provincias", {})
        return {}

    # Agrupar por provincia
    provincias = defaultdict(list)
    for mun in datos:
        mun_id = mun.get("id", "")  # formato "id28079"
        if not mun_id.startswith("id"):
            continue
        cod_completo = mun_id[2:]  # "28079"
        cod_prov = cod_completo[:2]  # "28"
        nombre_prov = PROVINCIAS.get(cod_prov, f"Provincia {cod_prov}")

        provincias[nombre_prov].append({
            "codigo": cod_completo,
            "nombre": mun.get("nombre", ""),
            "habitantes": int(mun.get("num_hab", "0") or "0"),
            "altitud": int(mun.get("altitud", "0") or "0"),
            "es_capital": cod_completo in CAPITALES_CODIGOS,
        })

    # Ordenar municipios dentro de cada provincia por habitantes (desc)
    for prov in provincias:
        provincias[prov].sort(key=lambda m: m["habitantes"], reverse=True)

    # Guardar caché
    total = sum(len(v) for v in provincias.values())
    cache = {
        "fecha": datetime.now().strftime("%Y-%m-%d"),
        "total": total,
        "provincias": dict(provincias),
    }
    MUNICIPIOS_CACHE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    log.info("Municipios descargados: %d en %d provincias", total, len(provincias))
    return dict(provincias)


def _extraer_temp_dia(datos, fecha_objetivo):
    """Extrae t_min y t_max de la respuesta de predicción para una fecha."""
    t_min = None
    t_max = None

    try:
        prediccion = datos[0].get("prediccion", {})
        dias = prediccion.get("dia", [])

        for dia in dias:
            fecha_dia = dia.get("fecha", "")[:10]
            if fecha_dia == fecha_objetivo:
                temps = dia.get("temperatura", {})
                if isinstance(temps, dict):
                    t_max = temps.get("maxima")
                    t_min = temps.get("minima")
                elif isinstance(temps, list):
                    for t in temps:
                        if t.get("descripcion") == "Máxima" or t.get("periodo") == "00-24":
                            t_max = t.get("valor", t.get("value"))
                        if t.get("descripcion") == "Mínima":
                            t_min = t.get("valor", t.get("value"))
                break

        # Si no encontramos la fecha, tomar el primer día
        if t_min is None and t_max is None and dias:
            dia = dias[0]
            temps = dia.get("temperatura", {})
            if isinstance(temps, dict):
                t_max = temps.get("maxima")
                t_min = temps.get("minima")

    except (IndexError, KeyError, TypeError):
        pass

    # Convertir a int
    try:
        t_min = int(t_min) if t_min is not None else None
    except (ValueError, TypeError):
        pass
    try:
        t_max = int(t_max) if t_max is not None else None
    except (ValueError, TypeError):
        pass

    return t_min, t_max


def obtener_temperaturas_completo(api_key, filtro_provincia=None):
    """
    Obtiene temperaturas de TODOS los municipios de España.
    Devuelve dict: {provincia: [{municipio, codigo, habitantes, t_min, t_max, fecha, es_capital}]}
    """
    hoy = datetime.now().strftime("%Y-%m-%d")
    provincias_mun = obtener_municipios(api_key)

    if not provincias_mun:
        log.error("No hay municipios disponibles")
        return {}

    if filtro_provincia:
        provincias_mun = {
            k: v for k, v in provincias_mun.items()
            if filtro_provincia.lower() in k.lower()
        }
        if not provincias_mun:
            log.error("No se encontró la provincia: %s", filtro_provincia)
            return {}

    total_mun = sum(len(v) for v in provincias_mun.values())
    log.info("Recogiendo temperaturas para %s (%d municipios en %d provincias)...",
             hoy, total_mun, len(provincias_mun))

    resultados = {}
    procesados = 0
    errores = 0
    inicio = time.time()
    # Rate limiter: AEMET allows ~50 req/min, each municipio = 2 requests
    # So max ~25 municipios/min. We target 22/min to stay safe.
    BATCH_SIZE = 22
    BATCH_PAUSE = 62  # seconds between batches
    batch_count = 0
    batch_start = time.time()

    for nombre_prov in sorted(provincias_mun.keys()):
        municipios = provincias_mun[nombre_prov]
        datos_prov = []

        log.info("  Provincia: %s (%d municipios)", nombre_prov, len(municipios))

        for mun in municipios:
            cod = mun["codigo"]
            nombre = mun["nombre"]

            # Rate limit: pause after every BATCH_SIZE municipalities
            batch_count += 1
            if batch_count >= BATCH_SIZE:
                elapsed = time.time() - batch_start
                if elapsed < BATCH_PAUSE:
                    wait = BATCH_PAUSE - elapsed
                    log.info("    Pausa rate limit: %.0fs...", wait)
                    time.sleep(wait)
                batch_count = 0
                batch_start = time.time()

            endpoint = f"/prediccion/especifica/municipio/diaria/{cod}"
            datos = aemet_request(endpoint, api_key)

            if not datos:
                errores += 1
                datos_prov.append({
                    "municipio": nombre,
                    "codigo": cod,
                    "provincia": nombre_prov,
                    "habitantes": mun["habitantes"],
                    "es_capital": mun["es_capital"],
                    "t_min": None,
                    "t_max": None,
                    "fecha": hoy,
                })
                time.sleep(0.1)
                continue

            t_min, t_max = _extraer_temp_dia(datos, hoy)

            datos_prov.append({
                "municipio": nombre,
                "codigo": cod,
                "provincia": nombre_prov,
                "habitantes": mun["habitantes"],
                "es_capital": mun["es_capital"],
                "t_min": t_min,
                "t_max": t_max,
                "fecha": hoy,
            })

            procesados += 1

            # Progreso cada 100 municipios
            if procesados % 100 == 0:
                elapsed_total = time.time() - inicio
                rate = procesados / (elapsed_total / 60) if elapsed_total > 0 else 0
                remaining = (total_mun - procesados) / rate if rate > 0 else 0
                log.info("    Progreso: %d/%d (%.0f/min, ~%.0f min restantes)",
                         procesados, total_mun, rate, remaining)

            time.sleep(0.1)  # Pequeña pausa entre requests

        resultados[nombre_prov] = datos_prov

    elapsed_total = time.time() - inicio
    log.info("Completado: %d municipios procesados, %d errores, %.1f minutos",
             procesados, errores, elapsed_total / 60)

    return resultados


def obtener_temperaturas_capitales(api_key):
    """Obtiene temperaturas solo de capitales de provincia (modo rápido)."""
    hoy = datetime.now().strftime("%Y-%m-%d")
    provincias_mun = obtener_municipios(api_key)
    resultados = {}

    total = 0
    for nombre_prov, municipios in sorted(provincias_mun.items()):
        capitales = [m for m in municipios if m["es_capital"]]
        if not capitales:
            capitales = municipios[:1]

        datos_prov = []
        for mun in capitales:
            cod = mun["codigo"]
            endpoint = f"/prediccion/especifica/municipio/diaria/{cod}"
            datos = aemet_request(endpoint, api_key)

            t_min, t_max = (None, None)
            if datos:
                t_min, t_max = _extraer_temp_dia(datos, hoy)

            datos_prov.append({
                "municipio": mun["nombre"],
                "codigo": cod,
                "provincia": nombre_prov,
                "habitantes": mun["habitantes"],
                "es_capital": True,
                "t_min": t_min,
                "t_max": t_max,
                "fecha": hoy,
            })
            total += 1

            if total % 10 == 0:
                log.info("  Progreso: %d capitales...", total)
            time.sleep(0.3)

        resultados[nombre_prov] = datos_prov

    log.info("Capitales completadas: %d", total)
    return resultados


def guardar_json(datos_por_provincia, json_path=JSON_FILE, dias_retener=7):
    """Exporta datos a JSON con acumulación de los últimos N días."""
    json_path.parent.mkdir(parents=True, exist_ok=True)

    hoy = datetime.now().strftime("%Y-%m-%d")
    fecha_limite = (datetime.now() - timedelta(days=dias_retener)).strftime("%Y-%m-%d")

    # 1. Cargar datos existentes
    registros_existentes = []
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data_anterior = json.load(f)
            registros_existentes = data_anterior.get("registros", [])
        except (json.JSONDecodeError, KeyError):
            log.warning("JSON existente corrupto, empezando de cero")

    # 2. Eliminar registros de hoy (idempotencia si se re-ejecuta)
    registros_existentes = [r for r in registros_existentes if r.get("fecha") != hoy]

    # 3. Podar registros más antiguos de N días
    registros_existentes = [r for r in registros_existentes if r.get("fecha", "") >= fecha_limite]

    # 4. Construir registros de hoy
    registros_hoy = []
    for nombre_prov, datos in datos_por_provincia.items():
        for d in datos:
            t_min = d["t_min"]
            t_max = d["t_max"]
            media = round((t_min + t_max) / 2, 1) if t_min is not None and t_max is not None else None
            registros_hoy.append({
                "nombre": d["municipio"],
                "codigo": d["codigo"],
                "provincia": nombre_prov,
                "hab": d["habitantes"],
                "min": t_min,
                "max": t_max,
                "media": media,
                "fecha": d["fecha"],
                "capital": d["es_capital"],
            })

    # 5. Fusionar
    todos_registros = registros_existentes + registros_hoy

    # 6. Metadatos
    dias_disponibles = sorted(set(r["fecha"] for r in todos_registros))
    codigos_unicos = set(r["codigo"] for r in todos_registros)

    output = {
        "actualizado": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "dias_disponibles": dias_disponibles,
        "total_municipios": len(codigos_unicos),
        "registros": todos_registros,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    log.info("JSON exportado: %s (%d registros, %d días)", json_path, len(todos_registros), len(dias_disponibles))


def main():
    parser = argparse.ArgumentParser(
        description="Recolector diario de temperaturas AEMET para todos los municipios de España"
    )
    parser.add_argument("--api-key", help="API key de AEMET OpenData")
    parser.add_argument(
        "--solo-capitales", action="store_true",
        help="Solo recoger capitales de provincia (rápido, ~2 min)"
    )
    parser.add_argument(
        "--provincia", type=str, default=None,
        help="Solo recoger una provincia específica (ej: Madrid)"
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("AEMET Temperaturas - Inicio recolección")
    log.info("=" * 60)

    api_key = get_api_key(args.api_key)

    if args.solo_capitales:
        log.info("Modo: Solo capitales de provincia")
        datos_por_provincia = obtener_temperaturas_capitales(api_key)
    else:
        modo_label = f"provincia {args.provincia}" if args.provincia else "todos los municipios"
        log.info("Modo: %s", modo_label)
        datos_por_provincia = obtener_temperaturas_completo(api_key, filtro_provincia=args.provincia)

    if not datos_por_provincia:
        log.error("No se obtuvieron datos. Abortando.")
        sys.exit(1)

    # Resumen
    total = sum(len(v) for v in datos_por_provincia.values())
    total_ok = sum(1 for v in datos_por_provincia.values() for d in v if d["t_min"] is not None)
    log.info("Resumen: %d municipios, %d con datos válidos", total, total_ok)

    # Exportar JSON acumulado (carga existente + hoy + poda >7 días)
    guardar_json(datos_por_provincia)

    log.info("Recolección completada exitosamente.")


if __name__ == "__main__":
    main()
