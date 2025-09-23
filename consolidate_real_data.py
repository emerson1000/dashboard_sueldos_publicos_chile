#!/usr/bin/env python3
"""
Consolida todos los datos reales extraídos.
"""

import pandas as pd
from pathlib import Path

def main():
    """Consolida todos los datos reales."""
    print("🔄 Consolidando datos reales...")
    
    datos_reales = []
    
    # Datos del SII (escalas)
    sii_file = Path('data/raw/sii/2025-09/escala.csv')
    if sii_file.exists():
        print("📊 Cargando datos del SII...")
        df_sii = pd.read_csv(sii_file)
        for _, row in df_sii.iterrows():
            sueldo_str = str(row['Remuneracion Bruta Mensualizada'])
            sueldo_limpio = sueldo_str.replace('.', '').replace(',', '.')
            try:
                sueldo_num = float(sueldo_limpio)
                if sueldo_num > 100000:
                    datos_reales.append({
                        'organismo': 'SII - Escala Oficial',
                        'nombre': None,
                        'cargo': f"{row['Estamento']} Grado {row['Grado']}",
                        'grado': row['Grado'],
                        'estamento': row['Estamento'],
                        'sueldo_bruto': sueldo_num,
                        'fuente': 'sii_escala',
                        'archivo_origen': 'escala.csv'
                    })
            except:
                continue
        print(f"  ✅ SII: {len([d for d in datos_reales if d['fuente'] == 'sii_escala'])} registros")
    
    # Datos del Ministerio del Trabajo
    trabajo_file = Path('data/raw/funcionarios_reales/2025-09/funcionarios_reales.csv')
    if trabajo_file.exists():
        print("📊 Cargando datos del Ministerio del Trabajo...")
        df_trabajo = pd.read_csv(trabajo_file)
        for _, row in df_trabajo.iterrows():
            datos_reales.append({
                'organismo': 'Ministerio del Trabajo',
                'nombre': None,
                'cargo': row.get('cargo', 'Funcionario'),
                'grado': None,
                'estamento': 'Funcionario',
                'sueldo_bruto': row['sueldo_bruto'],
                'fuente': 'ministerio_trabajo',
                'archivo_origen': 'funcionarios_reales.csv'
            })
        print(f"  ✅ Trabajo: {len([d for d in datos_reales if d['fuente'] == 'ministerio_trabajo'])} registros")
    
    # Datos específicos del SII
    sii_especifico_file = Path('data/raw/datos_reales_especificos/2025-09/funcionarios_reales_especificos.csv')
    if sii_especifico_file.exists():
        print("📊 Cargando datos específicos del SII...")
        df_sii_esp = pd.read_csv(sii_especifico_file)
        for _, row in df_sii_esp.iterrows():
            datos_reales.append({
                'organismo': 'SII - Datos Específicos',
                'nombre': None,
                'cargo': 'Funcionario SII',
                'grado': None,
                'estamento': row.get('estamento', 'Funcionario'),
                'sueldo_bruto': row['sueldo_bruto'],
                'fuente': 'sii_especifico',
                'archivo_origen': 'funcionarios_reales_especificos.csv'
            })
        print(f"  ✅ SII Específico: {len([d for d in datos_reales if d['fuente'] == 'sii_especifico'])} registros")
    
    # Crear DataFrame consolidado
    if datos_reales:
        df_consolidado = pd.DataFrame(datos_reales)
        
        # Guardar datos consolidados
        output_file = Path('data/processed/sueldos_reales_consolidado.csv')
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_consolidado.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"\n🎯 RESUMEN DE DATOS REALES:")
        print(f"📊 Total registros: {len(df_consolidado)}")
        print(f"🏛️ Organismos: {df_consolidado['organismo'].nunique()}")
        print(f"💰 Promedio sueldo: ${df_consolidado['sueldo_bruto'].mean():,.0f}")
        print(f"💰 Mediana sueldo: ${df_consolidado['sueldo_bruto'].median():,.0f}")
        print(f"💰 Rango: ${df_consolidado['sueldo_bruto'].min():,.0f} - ${df_consolidado['sueldo_bruto'].max():,.0f}")
        
        print(f"\n📋 Distribución por fuente:")
        for fuente, count in df_consolidado['fuente'].value_counts().items():
            print(f"  {fuente}: {count} registros")
        
        print(f"\n🏛️ Distribución por organismo:")
        for org, count in df_consolidado['organismo'].value_counts().items():
            print(f"  {org}: {count} registros")
        
        print(f"\n✅ Datos guardados en: {output_file}")
        
    else:
        print("❌ No se encontraron datos reales")

if __name__ == '__main__':
    main()
