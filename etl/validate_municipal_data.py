#!/usr/bin/env python3
"""
Validador de datos municipales para detectar y corregir inconsistencias geográficas.
Identifica registros que están asignados a municipalidades incorrectas.
"""

import pandas as pd
import re
from pathlib import Path
import logging
from typing import Dict, List, Tuple
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MunicipalDataValidator:
    """Validador de datos municipales para detectar inconsistencias geográficas."""
    
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.base_dir / 'data' / 'processed'
        
        # Mapeo de instituciones educativas y sus municipalidades correctas
        self.institution_mapping = {
            # San Felipe
            'LICEO SAN FELIPE': 'Municipalidad de San Felipe',
            'ESCUELA SAN FELIPE': 'Municipalidad de San Felipe',
            'COLEGIO SAN FELIPE': 'Municipalidad de San Felipe',
            
            # Angol
            'LICEO ANGOL': 'Municipalidad de Angol',
            'ESCUELA ANGOL': 'Municipalidad de Angol',
            'COLEGIO ANGOL': 'Municipalidad de Angol',
            
            # Concepción
            'LICEO CONCEPCION': 'Municipalidad de Concepción',
            'ESCUELA CONCEPCION': 'Municipalidad de Concepción',
            'COLEGIO CONCEPCION': 'Municipalidad de Concepción',
            
            # Santiago
            'LICEO SANTIAGO': 'Municipalidad de Santiago',
            'ESCUELA SANTIAGO': 'Municipalidad de Santiago',
            'COLEGIO SANTIAGO': 'Municipalidad de Santiago',
        }
        
        # Patrones para identificar nombres de ciudades en instituciones
        self.city_patterns = {
            'san felipe': 'Municipalidad de San Felipe',
            'angol': 'Municipalidad de Angol',
            'concepcion': 'Municipalidad de Concepción',
            'santiago': 'Municipalidad de Santiago',
            'valparaiso': 'Municipalidad de Valparaíso',
            'vina del mar': 'Municipalidad de Viña del Mar',
            'temuco': 'Municipalidad de Temuco',
            'antofagasta': 'Municipalidad de Antofagasta',
            'la serena': 'Municipalidad de La Serena',
            'rancagua': 'Municipalidad de Rancagua',
            'talca': 'Municipalidad de Talca',
            'chillan': 'Municipalidad de Chillán',
            'osorno': 'Municipalidad de Osorno',
            'puerto montt': 'Municipalidad de Puerto Montt',
            'arica': 'Municipalidad de Arica',
            'iquique': 'Municipalidad de Iquique',
            'calama': 'Municipalidad de Calama',
            'copiapo': 'Municipalidad de Copiapó',
            'coquimbo': 'Municipalidad de Coquimbo',
            'valdivia': 'Municipalidad de Valdivia',
        }
        
    def extract_city_from_institution(self, institution_name: str) -> str:
        """Extrae el nombre de la ciudad de una institución educativa."""
        if not institution_name:
            return None
            
        institution_lower = institution_name.lower()
        
        # Buscar patrones de ciudades
        for city_pattern, municipality in self.city_patterns.items():
            if city_pattern in institution_lower:
                return municipality
                
        # Buscar mapeos directos
        for institution_pattern, municipality in self.institution_mapping.items():
            if institution_pattern.lower() in institution_lower:
                return municipality
                
        return None
    
    def validate_record(self, record: pd.Series) -> Dict:
        """Valida un registro individual."""
        result = {
            'is_valid': True,
            'issues': [],
            'suggested_municipality': None,
            'confidence': 0
        }
        
        organismo = record.get('organismo', '')
        cargo = record.get('cargo', '')
        
        # Solo validar registros de municipalidades
        if 'municipalidad' not in organismo.lower():
            return result
            
        # Extraer ciudad sugerida del cargo/institución
        suggested_municipality = self.extract_city_from_institution(cargo)
        
        if suggested_municipality:
            result['suggested_municipality'] = suggested_municipality
            result['confidence'] = 0.8
            
            # Verificar si coincide con la municipalidad actual
            if suggested_municipality.lower() != organismo.lower():
                result['is_valid'] = False
                result['issues'].append({
                    'type': 'geographic_mismatch',
                    'message': f"Institución '{cargo}' sugiere '{suggested_municipality}' pero está asignada a '{organismo}'",
                    'current_municipality': organismo,
                    'suggested_municipality': suggested_municipality
                })
                
        return result
    
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida un DataFrame completo."""
        logger.info(f"Validando {len(df)} registros...")
        
        validation_results = []
        issues_found = 0
        
        for idx, record in df.iterrows():
            validation = self.validate_record(record)
            validation_results.append(validation)
            
            if not validation['is_valid']:
                issues_found += 1
                
        logger.info(f"Validación completada. Issues encontrados: {issues_found}")
        
        # Agregar columnas de validación al DataFrame
        df['validation_is_valid'] = [r['is_valid'] for r in validation_results]
        df['validation_issues'] = [json.dumps(r['issues']) for r in validation_results]
        df['suggested_municipality'] = [r.get('suggested_municipality') for r in validation_results]
        df['validation_confidence'] = [r.get('confidence', 0) for r in validation_results]
        
        return df
    
    def get_validation_report(self, df: pd.DataFrame) -> Dict:
        """Genera reporte de validación."""
        total_records = len(df)
        municipal_records = len(df[df['organismo'].str.contains('municipalidad', case=False, na=False)])
        invalid_records = len(df[df['validation_is_valid'] == False])
        
        # Agrupar issues por tipo
        issue_summary = {}
        for _, record in df[df['validation_is_valid'] == False].iterrows():
            issues = json.loads(record['validation_issues'])
            for issue in issues:
                issue_type = issue['type']
                if issue_type not in issue_summary:
                    issue_summary[issue_type] = []
                issue_summary[issue_type].append({
                    'organismo': record['organismo'],
                    'cargo': record.get('cargo', ''),
                    'suggested_municipality': issue.get('suggested_municipality'),
                    'message': issue['message']
                })
        
        # Top inconsistencias
        top_issues = []
        for issue_type, issues in issue_summary.items():
            for issue in issues[:10]:  # Top 10 por tipo
                top_issues.append(issue)
        
        report = {
            'total_records': total_records,
            'municipal_records': municipal_records,
            'invalid_records': invalid_records,
            'validation_rate': (municipal_records - invalid_records) / municipal_records * 100 if municipal_records > 0 else 0,
            'issue_summary': {k: len(v) for k, v in issue_summary.items()},
            'top_issues': top_issues[:20],  # Top 20 issues generales
            'detailed_issues': issue_summary
        }
        
        return report
    
    def fix_geographic_inconsistencies(self, df: pd.DataFrame, apply_fixes: bool = False) -> pd.DataFrame:
        """Corrige inconsistencias geográficas detectadas."""
        if not apply_fixes:
            logger.info("Modo simulación - no se aplicarán cambios")
            
        fixes_applied = 0
        df_fixed = df.copy()
        
        for idx, record in df_fixed.iterrows():
            if not record['validation_is_valid'] and record['suggested_municipality']:
                confidence = record.get('validation_confidence', 0)
                
                # Solo aplicar fixes con alta confianza
                if confidence >= 0.8:
                    if apply_fixes:
                        df_fixed.at[idx, 'organismo'] = record['suggested_municipality']
                        df_fixed.at[idx, 'validation_is_valid'] = True
                        df_fixed.at[idx, 'validation_issues'] = '[]'
                        fixes_applied += 1
                    else:
                        logger.info(f"FIX SUGERIDO: {record['organismo']} -> {record['suggested_municipality']} (cargo: {record.get('cargo', 'N/A')})")
                        
        logger.info(f"{'Aplicados' if apply_fixes else 'Sugeridos'}: {fixes_applied} fixes")
        return df_fixed
    
    def run_validation(self, input_file: str, output_file: str = None, apply_fixes: bool = False):
        """Ejecuta validación completa."""
        logger.info(f"Iniciando validación de {input_file}")
        
        # Cargar datos
        input_path = self.data_dir / input_file
        if not input_path.exists():
            logger.error(f"Archivo no encontrado: {input_path}")
            return
            
        df = pd.read_csv(input_path)
        logger.info(f"Cargados {len(df)} registros")
        
        # Validar
        df_validated = self.validate_dataframe(df)
        
        # Generar reporte
        report = self.get_validation_report(df_validated)
        
        # Mostrar reporte
        self.print_validation_report(report)
        
        # Aplicar fixes si se solicita
        if apply_fixes or report['invalid_records'] > 0:
            df_fixed = self.fix_geographic_inconsistencies(df_validated, apply_fixes)
            
            # Guardar resultado
            if output_file:
                output_path = self.data_dir / output_file
                df_fixed.to_csv(output_path, index=False)
                logger.info(f"Datos {'corregidos' if apply_fixes else 'validados'} guardados en: {output_path}")
            
            return df_fixed
        
        return df_validated
    
    def print_validation_report(self, report: Dict):
        """Imprime reporte de validación."""
        print("\n" + "="*80)
        print("REPORTE DE VALIDACION DE DATOS MUNICIPALES")
        print("="*80)
        print(f"Total registros: {report['total_records']:,}")
        print(f"Registros municipales: {report['municipal_records']:,}")
        print(f"Registros con inconsistencias: {report['invalid_records']:,}")
        print(f"Tasa de validacion: {report['validation_rate']:.1f}%")
        
        if report['issue_summary']:
            print("\nTIPOS DE INCONSISTENCIAS:")
            for issue_type, count in report['issue_summary'].items():
                print(f"  - {issue_type}: {count} casos")
        
        if report['top_issues']:
            print("\nTOP INCONSISTENCIAS DETECTADAS:")
            for i, issue in enumerate(report['top_issues'], 1):
                print(f"{i:2d}. {issue['organismo']}")
                print(f"    Cargo: {issue['cargo']}")
                print(f"    Sugerido: {issue.get('suggested_municipality', 'N/A')}")
                print(f"    Problema: {issue['message']}")
                print()
        
        print("="*80)

def main():
    """Función principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validador de datos municipales')
    parser.add_argument('--input-file', type=str, required=True,
                       help='Archivo CSV de entrada')
    parser.add_argument('--output-file', type=str,
                       help='Archivo CSV de salida')
    parser.add_argument('--apply-fixes', action='store_true',
                       help='Aplicar correcciones automáticas')
    
    args = parser.parse_args()
    
    validator = MunicipalDataValidator()
    validator.run_validation(args.input_file, args.output_file, args.apply_fixes)

if __name__ == '__main__':
    main()
