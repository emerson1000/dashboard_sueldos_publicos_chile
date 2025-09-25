#!/usr/bin/env python3
"""
Valida y limpia datos extraídos de transparencia activa.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import re
from typing import List, Dict, Tuple
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

class DataValidator:
    """Validador y limpiador de datos extraídos."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.validation_rules = self._load_validation_rules()
    
    def _load_validation_rules(self) -> Dict:
        """Carga reglas de validación."""
        return {
            'sueldo_min': 100000,  # Mínimo 100,000 pesos
            'sueldo_max': 50000000,  # Máximo 50,000,000 pesos
            'nombre_min_length': 3,
            'nombre_max_length': 100,
            'cargo_min_length': 3,
            'cargo_max_length': 200,
            'estamento_validos': [
                'DIRECTIVO', 'PROFESIONAL', 'TÉCNICO', 'ADMINISTRATIVO', 'AUXILIAR',
                'FISCALIZADOR', 'EJECUTIVO', 'SUPERVISOR', 'COORDINADOR', 'ANALISTA',
                'Directivo', 'Profesional', 'Técnico', 'Administrativo', 'Auxiliar',
                'Fiscalizador', 'Ejecutivo', 'Supervisor', 'Coordinador', 'Analista'
            ],
            'organismos_validos': [
                'MINISTERIO', 'SERVICIO', 'MUNICIPALIDAD', 'UNIVERSIDAD', 'CORPORACIÓN',
                'FUNDACIÓN', 'INSTITUTO', 'AGENCIA', 'COMISIÓN', 'CONSEJO'
            ]
        }
    
    def validate_sueldo(self, sueldo: float) -> Tuple[bool, str]:
        """Valida valor de sueldo."""
        if pd.isna(sueldo):
            return False, "Sueldo nulo"
        
        if not isinstance(sueldo, (int, float)):
            return False, "Sueldo no numérico"
        
        if sueldo < self.validation_rules['sueldo_min']:
            return False, f"Sueldo muy bajo: ${sueldo:,.0f}"
        
        if sueldo > self.validation_rules['sueldo_max']:
            return False, f"Sueldo muy alto: ${sueldo:,.0f}"
        
        return True, "Válido"
    
    def validate_nombre(self, nombre: str) -> Tuple[bool, str]:
        """Valida nombre de funcionario."""
        if pd.isna(nombre) or not nombre:
            return False, "Nombre vacío"
        
        nombre = str(nombre).strip()
        
        if len(nombre) < self.validation_rules['nombre_min_length']:
            return False, "Nombre muy corto"
        
        if len(nombre) > self.validation_rules['nombre_max_length']:
            return False, "Nombre muy largo"
        
        # Verificar que contenga solo letras, espacios y caracteres especiales válidos
        if not re.match(r'^[a-zA-ZáéíóúÁÉÍÓÚñÑ\s\.\-]+$', nombre):
            return False, "Nombre contiene caracteres inválidos"
        
        return True, "Válido"
    
    def validate_cargo(self, cargo: str) -> Tuple[bool, str]:
        """Valida cargo de funcionario."""
        if pd.isna(cargo) or not cargo:
            return False, "Cargo vacío"
        
        cargo = str(cargo).strip()
        
        if len(cargo) < self.validation_rules['cargo_min_length']:
            return False, "Cargo muy corto"
        
        if len(cargo) > self.validation_rules['cargo_max_length']:
            return False, "Cargo muy largo"
        
        return True, "Válido"
    
    def validate_estamento(self, estamento: str) -> Tuple[bool, str]:
        """Valida estamento."""
        if pd.isna(estamento) or not estamento:
            return False, "Estamento vacío"
        
        estamento = str(estamento).strip().upper()
        
        if estamento not in self.validation_rules['estamento_validos']:
            return False, f"Estamento no válido: {estamento}"
        
        return True, "Válido"
    
    def validate_organismo(self, organismo: str) -> Tuple[bool, str]:
        """Valida organismo."""
        if pd.isna(organismo) or not organismo:
            return False, "Organismo vacío"
        
        organismo = str(organismo).strip().upper()
        
        # Verificar que contenga palabras clave válidas
        if not any(keyword in organismo for keyword in self.validation_rules['organismos_validos']):
            return False, f"Organismo no válido: {organismo}"
        
        return True, "Válido"
    
    def clean_sueldo(self, sueldo: str) -> float:
        """Limpia valor de sueldo."""
        if pd.isna(sueldo):
            return np.nan
        
        # Convertir a string y limpiar
        sueldo_str = str(sueldo).strip()
        
        # Remover caracteres no numéricos excepto puntos y comas
        sueldo_str = re.sub(r'[^\d.,]', '', sueldo_str)
        
        if not sueldo_str:
            return np.nan
        
        try:
            # Manejar formato chileno (1.234.567,89)
            if '.' in sueldo_str and ',' in sueldo_str:
                sueldo_str = sueldo_str.replace('.', '').replace(',', '.')
            elif '.' in sueldo_str:
                # Verificar si es separador de miles
                parts = sueldo_str.split('.')
                if len(parts) > 2 or (len(parts) == 2 and len(parts[1]) <= 2):
                    sueldo_str = sueldo_str.replace('.', '')
            
            return float(sueldo_str)
        except:
            return np.nan
    
    def clean_nombre(self, nombre: str) -> str:
        """Limpia nombre de funcionario."""
        if pd.isna(nombre) or not nombre:
            return ""
        
        nombre = str(nombre).strip()
        
        # Capitalizar palabras
        nombre = ' '.join(word.capitalize() for word in nombre.split())
        
        # Limpiar espacios múltiples
        nombre = re.sub(r'\s+', ' ', nombre)
        
        return nombre
    
    def clean_cargo(self, cargo: str) -> str:
        """Limpia cargo de funcionario."""
        if pd.isna(cargo) or not cargo:
            return ""
        
        cargo = str(cargo).strip()
        
        # Capitalizar palabras
        cargo = ' '.join(word.capitalize() for word in cargo.split())
        
        # Limpiar espacios múltiples
        cargo = re.sub(r'\s+', ' ', cargo)
        
        return cargo
    
    def clean_estamento(self, estamento: str) -> str:
        """Limpia estamento."""
        if pd.isna(estamento) or not estamento:
            return ""
        
        estamento = str(estamento).strip().upper()
        
        # Mapear variaciones comunes
        estamento_mapping = {
            'DIRECTIVO': 'DIRECTIVO',
            'PROFESIONAL': 'PROFESIONAL',
            'TECNICO': 'TÉCNICO',
            'TÉCNICO': 'TÉCNICO',
            'ADMINISTRATIVO': 'ADMINISTRATIVO',
            'AUXILIAR': 'AUXILIAR',
            'FISCALIZADOR': 'FISCALIZADOR',
            'EJECUTIVO': 'EJECUTIVO',
            'SUPERVISOR': 'SUPERVISOR',
            'COORDINADOR': 'COORDINADOR',
            'ANALISTA': 'ANALISTA'
        }
        
        return estamento_mapping.get(estamento, estamento)
    
    def clean_organismo(self, organismo: str) -> str:
        """Limpia nombre de organismo."""
        if pd.isna(organismo) or not organismo:
            return ""
        
        organismo = str(organismo).strip()
        
        # Capitalizar palabras
        organismo = ' '.join(word.capitalize() for word in organismo.split())
        
        # Limpiar espacios múltiples
        organismo = re.sub(r'\s+', ' ', organismo)
        
        return organismo
    
    def validate_and_clean_dataframe(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Valida y limpia DataFrame completo."""
        logger.info(f"Validando y limpiando {len(df)} registros")
        
        # Copiar DataFrame
        df_clean = df.copy()
        
        # Estadísticas de validación
        validation_stats = {
            'total_records': len(df),
            'valid_records': 0,
            'invalid_records': 0,
            'validation_errors': {}
        }
        
        # Limpiar datos
        df_clean['sueldo_bruto'] = df_clean['sueldo_bruto'].apply(self.clean_sueldo)
        df_clean['nombre'] = df_clean['nombre'].apply(self.clean_nombre)
        df_clean['cargo'] = df_clean['cargo'].apply(self.clean_cargo)
        df_clean['estamento'] = df_clean['estamento'].apply(self.clean_estamento)
        df_clean['organismo'] = df_clean['organismo'].apply(self.clean_organismo)
        
        # Validar cada registro
        valid_indices = []
        invalid_indices = []
        
        for idx, row in df_clean.iterrows():
            is_valid = True
            errors = []
            
            # Validar sueldo
            sueldo_valid, sueldo_error = self.validate_sueldo(row['sueldo_bruto'])
            if not sueldo_valid:
                is_valid = False
                errors.append(f"Sueldo: {sueldo_error}")
            
            # Validar nombre
            nombre_valid, nombre_error = self.validate_nombre(row['nombre'])
            if not nombre_valid:
                is_valid = False
                errors.append(f"Nombre: {nombre_error}")
            
            # Validar cargo
            cargo_valid, cargo_error = self.validate_cargo(row['cargo'])
            if not cargo_valid:
                is_valid = False
                errors.append(f"Cargo: {cargo_error}")
            
            # Validar estamento
            estamento_valid, estamento_error = self.validate_estamento(row['estamento'])
            if not estamento_valid:
                is_valid = False
                errors.append(f"Estamento: {estamento_error}")
            
            # Validar organismo
            organismo_valid, organismo_error = self.validate_organismo(row['organismo'])
            if not organismo_valid:
                is_valid = False
                errors.append(f"Organismo: {organismo_error}")
            
            if is_valid:
                valid_indices.append(idx)
            else:
                invalid_indices.append(idx)
                validation_stats['validation_errors'][idx] = errors
        
        # Separar datos válidos e inválidos
        df_valid = df_clean.loc[valid_indices].copy()
        df_invalid = df_clean.loc[invalid_indices].copy()
        
        # Actualizar estadísticas
        validation_stats['valid_records'] = len(df_valid)
        validation_stats['invalid_records'] = len(df_invalid)
        
        logger.info(f"Registros válidos: {len(df_valid)}")
        logger.info(f"Registros inválidos: {len(df_invalid)}")
        
        return df_valid, validation_stats
    
    def save_cleaned_data(self, df_valid: pd.DataFrame, validation_stats: Dict, output_dir: Path):
        """Guarda datos limpios y estadísticas."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Guardar datos válidos
        valid_file = output_dir / 'datos_validos.csv'
        df_valid.to_csv(valid_file, index=False, encoding='utf-8')
        
        # Guardar estadísticas
        stats_file = output_dir / 'estadisticas_validacion.json'
        with open(stats_file, 'w', encoding='utf-8') as f:
            import json
            json.dump(validation_stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Datos válidos guardados en {valid_file}")
        logger.info(f"Estadísticas guardadas en {stats_file}")
        
        return valid_file, stats_file

def main():
    """Función principal."""
    base_dir = Path(__file__).resolve().parent.parent
    db_path = base_dir / 'data' / 'processed' / 'extraction_progress.db'
    output_dir = base_dir / 'data' / 'processed'
    
    # Crear validador
    validator = DataValidator(db_path)
    
    # Conectar a base de datos
    conn = sqlite3.connect(db_path)
    
    # Cargar datos extraídos
    df = pd.read_sql_query('SELECT * FROM extracted_data', conn)
    conn.close()
    
    if df.empty:
        logger.warning("No hay datos para validar")
        return
    
    logger.info(f"Cargados {len(df)} registros para validación")
    
    # Validar y limpiar
    df_valid, validation_stats = validator.validate_and_clean_dataframe(df)
    
    # Guardar resultados
    validator.save_cleaned_data(df_valid, validation_stats, output_dir)
    
    # Mostrar resumen
    print("\n" + "="*60)
    print("📊 RESUMEN DE VALIDACIÓN")
    print("="*60)
    print(f"Total registros: {validation_stats['total_records']:,}")
    print(f"Registros válidos: {validation_stats['valid_records']:,}")
    print(f"Registros inválidos: {validation_stats['invalid_records']:,}")
    print(f"Tasa de validez: {validation_stats['valid_records']/validation_stats['total_records']*100:.1f}%")
    
    if df_valid['sueldo_bruto'].notna().any():
        print(f"Sueldo promedio: ${df_valid['sueldo_bruto'].mean():,.0f}")
        print(f"Sueldo mediana: ${df_valid['sueldo_bruto'].median():,.0f}")
        print(f"Rango sueldos: ${df_valid['sueldo_bruto'].min():,.0f} - ${df_valid['sueldo_bruto'].max():,.0f}")
    
    print(f"Organismos únicos: {df_valid['organismo'].nunique()}")
    print(f"Estamentos únicos: {df_valid['estamento'].nunique()}")
    print("="*60)

if __name__ == '__main__':
    main()

