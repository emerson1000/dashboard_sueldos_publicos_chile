#!/usr/bin/env python3
"""
Script de extracción de datos desde DIPRES.

Busca enlaces a archivos públicos con extensiones .xlsx, .xls, .csv y .pdf en las páginas semilla
definidas, descarga los archivos y los guarda en el directorio data/raw/dipres/<YYYY-MM>.
Mantiene un manifiesto JSON para evitar descargas duplicadas comparando hashes SHA256.

Este script utiliza BeautifulSoup para encontrar enlaces en páginas HTML y requests para
descargar archivos. Ajusta la lista SEED_PAGES según las necesidades de tu proyecto.
"""

import json
import hashlib
import time
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / 'data' / 'raw'
MANIFEST_PATH = DATA_RAW / 'dipres_manifest.json'

# Cabecera simple para evitar bloqueos por parte de algunos sitios
HEADERS = {"User-Agent": "TransparenciaSueldosBot/1.0 (+https://example.com)"}

# Páginas iniciales de DIPRES donde buscar archivos. Puedes añadir o quitar según convenga.
SEED_PAGES = [
    "https://www.dipres.gob.cl/598/w3-article-190248.html",
    "https://www.dipres.gob.cl/transparencia/doc/remuneraciones/per_remuneraciones.html",
]

# Extensiones de archivo de interés
EXTS = ('.xlsx', '.xls', '.csv', '.pdf')


def sha256(data: bytes) -> str:
    """Calcula el hash SHA256 de un objeto de bytes."""
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def ensure_dir(p: Path):
    """Crea el directorio si no existe."""
    p.mkdir(parents=True, exist_ok=True)


def load_manifest() -> dict:
    """Carga el manifiesto de descargas anteriores."""
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text(encoding='utf-8'))
    return {}


def save_manifest(manifest: dict):
    """Guarda el manifiesto en disco."""
    ensure_dir(MANIFEST_PATH.parent)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding='utf-8')


def discover_links(url: str) -> list:
    """Devuelve una lista de URLs absolutas encontradas en una página que terminan en las extensiones de interés."""
    links = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"Error al acceder {url}: {e}")
        return []
    soup = BeautifulSoup(resp.text, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        full = urljoin(url, href)
        parsed = urlparse(full)
        # Solo descargar archivos dentro del dominio dipres.gob.cl
        if 'dipres.gob.cl' not in parsed.netloc:
            continue
        if any(full.lower().endswith(ext) for ext in EXTS):
            links.append(full)
    return list(dict.fromkeys(links))


def safe_filename(url: str) -> str:
    """Genera un nombre de archivo seguro a partir de una URL."""
    name = Path(urlparse(url).path).name
    if not name:
        # Sustituye caracteres no alfanuméricos para formar un nombre válido
        name = re.sub(r'\W+', '_', url)
    return name


def main():
    manifest = load_manifest()
    discovered = []
    # Descubre enlaces en todas las páginas semilla
    for page in SEED_PAGES:
        discovered.extend(discover_links(page))
    discovered = list(dict.fromkeys(discovered))
    print(f"Enlaces encontrados: {len(discovered)}")
    for url in discovered:
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            content = r.content
            h = sha256(content)
            prev = manifest.get(url)
            if prev and prev.get('sha256') == h:
                print(f"Saltando (sin cambios): {url}")
                continue
            # Directorio basado en año-mes
            y_m = time.strftime("%Y-%m")
            dest_dir = DATA_RAW / 'dipres' / y_m
            ensure_dir(dest_dir)
            filename = safe_filename(url)
            dest = dest_dir / filename
            with open(dest, 'wb') as f:
                f.write(content)
            manifest[url] = {"sha256": h, "path": str(dest)}
            print(f"Descargado {dest}")
        except Exception as e:
            print(f"Error al descargar {url}: {e}")
    save_manifest(manifest)


if __name__ == '__main__':
    main()