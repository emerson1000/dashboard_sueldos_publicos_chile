#!/usr/bin/env python3
"""
Configuración para suprimir warnings molestos en el dashboard.
"""

import warnings
import os

def suppress_all_warnings():
    """Suprime todos los warnings molestos."""
    
    # Suprimir warnings de Plotly
    warnings.filterwarnings('ignore', category=UserWarning, module='plotly')
    warnings.filterwarnings('ignore', message='.*keyword arguments have been deprecated.*')
    warnings.filterwarnings('ignore', message='.*config instead to specify Plotly configuration options.*')
    warnings.filterwarnings('ignore', message='.*deprecated.*')
    
    # Suprimir warnings de pandas
    warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')
    warnings.filterwarnings('ignore', message='.*DataFrame.*')
    
    # Suprimir warnings de Streamlit
    warnings.filterwarnings('ignore', category=UserWarning, module='streamlit')
    warnings.filterwarnings('ignore', message='.*use_container_width.*')
    warnings.filterwarnings('ignore', message='.*deprecation.*')
    
    # Suprimir warnings de urllib3
    warnings.filterwarnings('ignore', category=UserWarning, module='urllib3')
    
    # Configurar variables de entorno para suprimir warnings
    os.environ['PYTHONWARNINGS'] = 'ignore'
    
    print("✅ Warnings suprimidos correctamente")

if __name__ == "__main__":
    suppress_all_warnings()


