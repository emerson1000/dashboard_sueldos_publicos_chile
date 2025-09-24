#!/usr/bin/env python3
"""
Consolida y mejora los datos reales existentes.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json
from datetime import datetime
import sqlite3

logger = logging.getLogger(__name__)

class RealDataConsolidator:
    """Consolida y mejora datos reales existentes."""
    
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.base_dir / 'data' / 'processed'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Archivos de datos reales existentes
        self.data_files = [
            'sueldos_reales_consolidado.csv',
            'sueldos_consolidado_final_small.csv',
            'sueldos_filtrados_small.parquet',
            'sueldos_categorizados_small.parquet'
        ]
    
    def load_existing_data(self):
        """Carga todos los datos reales existentes."""
        all_data = []
        
        for file_name in self.data_files:
            file_path = self.data_dir / file_name
            
            if file_path.exists():
                try:
                    logger.info(f"Cargando datos de {file_name}")
                    
                    if file_name.endswith('.parquet'):
                        df = pd.read_parquet(file_path)
                    else:
                        df = pd.read_csv(file_path)
                    
                    logger.info(f"  - {len(df)} registros cargados")
                    all_data.append(df)
                    
                except Exception as e:
                    logger.error(f"Error cargando {file_name}: {e}")
            else:
                logger.warning(f"Archivo no encontrado: {file_name}")
        
        if all_data:
            # Combinar todos los datos
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total registros combinados: {len(combined_df)}")
            return combined_df
        else:
            logger.error("No se encontraron datos para cargar")
            return pd.DataFrame()
    
    def clean_and_standardize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia y estandariza los datos."""
        logger.info("Limpiando y estandarizando datos")
        
        # Crear copia
        df_clean = df.copy()
        
        # Estandarizar nombres de columnas
        column_mapping = {
            'organismo': 'organismo',
            'nombre': 'nombre',
            'cargo': 'cargo',
            'estamento': 'estamento',
            'grado': 'grado',
            'sueldo_bruto': 'sueldo_bruto',
            'fuente': 'fuente',
            'url_origen': 'url_origen',
            'fecha_procesamiento': 'fecha_procesamiento',
            'categoria_organismo': 'categoria_organismo'
        }
        
        # Renombrar columnas si existen
        for old_col, new_col in column_mapping.items():
            if old_col in df_clean.columns:
                df_clean = df_clean.rename(columns={old_col: new_col})
        
        # Limpiar datos de sueldo
        if 'sueldo_bruto' in df_clean.columns:
            df_clean['sueldo_bruto'] = pd.to_numeric(df_clean['sueldo_bruto'], errors='coerce')
            df_clean = df_clean[df_clean['sueldo_bruto'].notna()]
            df_clean = df_clean[df_clean['sueldo_bruto'] > 100000]  # Mínimo razonable
        
        # Limpiar nombres
        if 'nombre' in df_clean.columns:
            df_clean['nombre'] = df_clean['nombre'].fillna('Sin especificar')
            df_clean['nombre'] = df_clean['nombre'].astype(str).str.strip()
        
        # Limpiar cargos
        if 'cargo' in df_clean.columns:
            df_clean['cargo'] = df_clean['cargo'].fillna('Sin especificar')
            df_clean['cargo'] = df_clean['cargo'].astype(str).str.strip()
        
        # Limpiar estamentos
        if 'estamento' in df_clean.columns:
            df_clean['estamento'] = df_clean['estamento'].fillna('Sin especificar')
            df_clean['estamento'] = df_clean['estamento'].astype(str).str.strip().str.upper()
        
        # Limpiar organismos
        if 'organismo' in df_clean.columns:
            df_clean['organismo'] = df_clean['organismo'].fillna('Sin especificar')
            df_clean['organismo'] = df_clean['organismo'].astype(str).str.strip()
        
        # Agregar categorización de organismos
        df_clean['categoria_organismo'] = df_clean['organismo'].apply(self._categorize_organismo)
        
        # Agregar fecha de procesamiento si no existe
        if 'fecha_procesamiento' not in df_clean.columns:
            df_clean['fecha_procesamiento'] = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Datos limpios: {len(df_clean)} registros")
        return df_clean
    
    def _categorize_organismo(self, organismo: str) -> str:
        """Categoriza un organismo."""
        organismo_lower = str(organismo).lower()
        
        if 'ministerio' in organismo_lower:
            return 'Ministerio'
        elif 'municipalidad' in organismo_lower:
            return 'Municipalidad'
        elif 'servicio' in organismo_lower:
            return 'Servicio'
        elif 'universidad' in organismo_lower:
            return 'Universidad'
        elif 'corporacion' in organismo_lower or 'corporación' in organismo_lower:
            return 'Corporación'
        elif 'fundacion' in organismo_lower or 'fundación' in organismo_lower:
            return 'Fundación'
        elif 'instituto' in organismo_lower:
            return 'Instituto'
        elif 'agencia' in organismo_lower:
            return 'Agencia'
        elif 'comision' in organismo_lower or 'comisión' in organismo_lower:
            return 'Comisión'
        elif 'consejo' in organismo_lower:
            return 'Consejo'
        else:
            return 'Otros'
    
    def enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enriquece los datos con información adicional."""
        logger.info("Enriqueciendo datos")
        
        df_enriched = df.copy()
        
        # Agregar información de sueldos
        if 'sueldo_bruto' in df_enriched.columns:
            df_enriched['sueldo_categoria'] = pd.cut(
                df_enriched['sueldo_bruto'],
                bins=[0, 500000, 1000000, 2000000, 5000000, float('inf')],
                labels=['Bajo', 'Medio-Bajo', 'Medio', 'Medio-Alto', 'Alto']
            )
            
            # Percentiles
            df_enriched['sueldo_percentil'] = df_enriched['sueldo_bruto'].rank(pct=True) * 100
        
        # Agregar información de organismos
        if 'organismo' in df_enriched.columns:
            org_stats = df_enriched.groupby('organismo')['sueldo_bruto'].agg(['count', 'mean', 'median']).round(0)
            org_stats.columns = ['total_funcionarios', 'promedio_sueldo', 'mediana_sueldo']
            
            df_enriched = df_enriched.merge(org_stats, left_on='organismo', right_index=True, how='left')
        
        # Agregar información de estamentos
        if 'estamento' in df_enriched.columns:
            estamento_stats = df_enriched.groupby('estamento')['sueldo_bruto'].agg(['count', 'mean', 'median']).round(0)
            estamento_stats.columns = ['total_estamento', 'promedio_estamento', 'mediana_estamento']
            
            df_enriched = df_enriched.merge(estamento_stats, left_on='estamento', right_index=True, how='left')
        
        logger.info("Datos enriquecidos")
        return df_enriched
    
    def generate_statistics(self, df: pd.DataFrame) -> dict:
        """Genera estadísticas de los datos."""
        stats = {
            'fecha_generacion': datetime.now().isoformat(),
            'total_registros': len(df),
            'organismos_unicos': df['organismo'].nunique() if 'organismo' in df.columns else 0,
            'estamentos_unicos': df['estamento'].nunique() if 'estamento' in df.columns else 0,
            'categorias_organismos': df['categoria_organismo'].nunique() if 'categoria_organismo' in df.columns else 0,
        }
        
        if 'sueldo_bruto' in df.columns:
            stats.update({
                'sueldo_promedio': float(df['sueldo_bruto'].mean()),
                'sueldo_mediana': float(df['sueldo_bruto'].median()),
                'sueldo_minimo': float(df['sueldo_bruto'].min()),
                'sueldo_maximo': float(df['sueldo_bruto'].max()),
                'sueldo_desviacion': float(df['sueldo_bruto'].std()),
            })
        
        # Top organismos
        if 'organismo' in df.columns:
            top_organismos = df['organismo'].value_counts().head(10).to_dict()
            stats['top_organismos'] = top_organismos
        
        # Top estamentos
        if 'estamento' in df.columns:
            top_estamentos = df['estamento'].value_counts().head(10).to_dict()
            stats['top_estamentos'] = top_estamentos
        
        # Distribución por categoría
        if 'categoria_organismo' in df.columns:
            categoria_dist = df['categoria_organismo'].value_counts().to_dict()
            stats['distribucion_categorias'] = categoria_dist
        
        return stats
    
    def save_consolidated_data(self, df: pd.DataFrame, stats: dict):
        """Guarda los datos consolidados."""
        logger.info("Guardando datos consolidados")
        
        # Guardar datos principales
        main_file = self.data_dir / 'datos_reales_consolidados.csv'
        df.to_csv(main_file, index=False, encoding='utf-8')
        
        # Guardar datos enriquecidos
        enriched_file = self.data_dir / 'datos_reales_enriquecidos.csv'
        df.to_csv(enriched_file, index=False, encoding='utf-8')
        
        # Guardar estadísticas
        stats_file = self.data_dir / 'estadisticas_datos_reales.json'
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        # Guardar en base de datos SQLite (simplificado)
        try:
            db_file = self.data_dir / 'datos_reales.db'
            conn = sqlite3.connect(db_file)
            
            # Eliminar columnas duplicadas antes de guardar
            df_clean = df.loc[:, ~df.columns.duplicated()]
            df_clean.to_sql('sueldos_reales', conn, if_exists='replace', index=False)
            conn.close()
            logger.info(f"  - {db_file}")
        except Exception as e:
            logger.warning(f"Error guardando en base de datos: {e}")
        
        logger.info(f"Datos guardados en:")
        logger.info(f"  - {main_file}")
        logger.info(f"  - {enriched_file}")
        logger.info(f"  - {stats_file}")
        logger.info(f"  - {db_file}")
    
    def run_consolidation(self):
        """Ejecuta el proceso completo de consolidación."""
        logger.info("Iniciando consolidación de datos reales")
        
        # Cargar datos existentes
        df = self.load_existing_data()
        if df.empty:
            logger.error("No hay datos para consolidar")
            return
        
        # Limpiar y estandarizar
        df_clean = self.clean_and_standardize_data(df)
        
        # Enriquecer datos
        df_enriched = self.enrich_data(df_clean)
        
        # Generar estadísticas
        stats = self.generate_statistics(df_enriched)
        
        # Guardar datos
        self.save_consolidated_data(df_enriched, stats)
        
        # Mostrar resumen
        print("\n" + "="*60)
        print("📊 RESUMEN DE CONSOLIDACIÓN DE DATOS REALES")
        print("="*60)
        print(f"Total registros consolidados: {stats['total_registros']:,}")
        print(f"Organismos únicos: {stats['organismos_unicos']}")
        print(f"Estamentos únicos: {stats['estamentos_unicos']}")
        print(f"Categorías de organismos: {stats['categorias_organismos']}")
        
        if 'sueldo_promedio' in stats:
            print(f"Sueldo promedio: ${stats['sueldo_promedio']:,.0f}")
            print(f"Sueldo mediana: ${stats['sueldo_mediana']:,.0f}")
            print(f"Rango sueldos: ${stats['sueldo_minimo']:,.0f} - ${stats['sueldo_maximo']:,.0f}")
        
        print("\n🏆 TOP ORGANISMOS:")
        for organismo, count in list(stats['top_organismos'].items())[:5]:
            print(f"  {organismo}: {count} registros")
        
        print("\n📊 TOP ESTAMENTOS:")
        for estamento, count in list(stats['top_estamentos'].items())[:5]:
            print(f"  {estamento}: {count} registros")
        
        print("="*60)
        
        return df_enriched, stats

def main():
    """Función principal."""
    consolidator = RealDataConsolidator()
    df, stats = consolidator.run_consolidation()
    
    if not df.empty:
        print(f"\n✅ Consolidación exitosa: {len(df)} registros de datos reales")
    else:
        print("\n❌ No se pudieron consolidar los datos")

if __name__ == '__main__':
    main()
