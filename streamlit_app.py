#!/usr/bin/env python3
"""
Streamlit app principal para Streamlit Cloud.
Redirige al dashboard principal con datos reales.
"""

import streamlit as st
import sys
from pathlib import Path

# Agregar el directorio actual al path
sys.path.append(str(Path(__file__).parent))

# Importar y ejecutar el dashboard principal
if __name__ == "__main__":
    # Redirigir al dashboard principal
    exec(open('dashboard_real_data.py').read())