#!/usr/bin/env python3
"""
Script de prueba para verificar que los headers funcionan correctamente.
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup

def test_headers():
    """Prueba los headers en diferentes sitios."""
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    
    # URLs de prueba
    test_urls = [
        "https://www.sii.cl/transparencia/2025/plantilla_escala_ene.html",
        "https://www.hacienda.cl/transparencia/",
        "https://www.mineduc.cl/transparencia/",
        "https://www.minsal.cl/transparencia/"
    ]
    
    print("🧪 Probando headers en diferentes sitios...")
    
    for url in test_urls:
        try:
            print(f"\n📡 Probando: {url}")
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                print(f"✅ Éxito: {response.status_code}")
                print(f"📄 Tamaño: {len(response.content)} bytes")
                
                # Verificar si hay contenido HTML
                if 'html' in response.headers.get('content-type', '').lower():
                    soup = BeautifulSoup(response.content, 'html.parser')
                    title = soup.find('title')
                    if title:
                        print(f"📋 Título: {title.get_text().strip()}")
                
            else:
                print(f"❌ Error: {response.status_code}")
                
        except Exception as e:
            print(f"💥 Excepción: {e}")
    
    print("\n🎯 Prueba completada")

if __name__ == '__main__':
    test_headers()
