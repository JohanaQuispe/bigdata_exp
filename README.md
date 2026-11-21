# Unity Perú SAC — BI, Big Data y experimentación

Proyecto integrador de Base de Datos Avanzadas y Big Data · NRC 26055 · 2026-2 · Docente: Víctor Isaac Chávez Bardales.

## Entregables

- [Informe final editable](docs/2026-2_ISC-Informe_Proyecto_Final_BASAV.docx): capítulos I–VIII, anexos A–I y manifiesto E-01–E-16.
- [Guía de ejecución y capturas por integrante](docs/GUIA_CAPTURAS_Y_EXPERIMENTOS_POR_INTEGRANTE.md) ([Word](docs/GUIA_CAPTURAS_Y_EXPERIMENTOS_POR_INTEGRANTE.docx)).
- [Replicación paso a paso](REPLICACION.md) y [guion de defensa](docs/Guion_Defensa.md).
- [Dashboard Power BI](powerbi/Dashboard_Almacen_BI.pbix), ETL y scripts SQL; [notebooks Databricks](bigdata/notebooks/) y [pipeline PySpark local](bigdata/pipeline_medallion_spark.py).
- [Resultados experimentales](experimentos/salidas/) y [10 gráficos comparativos](experimentos/evidencias/).

## Estado y límites de la evidencia

- La ejecución Spark y los experimentos fueron realizados **localmente**, en Python/SQLite/pandas y Spark 3.5.3 `local[2]`. Los notebooks 01–03 se probaron localmente con Parquet. Los experimentos BI-01/03/05 no miden directamente SQL Server ni DAX; sus escenarios son aproximaciones. Los datos escalados son sintéticos.
- Los notebooks están preparados para adaptación y ejecución en Databricks con Delta, pero **no se dispone de una ejecución real en Databricks**. Crear dashboard, activar Genie si corresponde y obtener capturas reales son tareas pendientes. `fig_bd03` y `fig_bd04` son maquetas creadas en Python, no capturas ni respuestas de Genie.
- El PBIX y las capturas Power BI existentes se conservan, al igual que los datos de sensores. Se eliminó únicamente la figura independiente de anomalías ambientales.
- Las asignaciones individuales son una propuesta: cada integrante debe ejecutar su prueba, capturar evidencia propia, confirmar sus aportes y firmar la declaración de IA. También faltan revisión docente, aprobación, porcentajes, etiqueta final y permisos/procedencia verificados.

## Organización

| Ruta | Contenido |
|---|---|
| `scripts/`, `etl/`, `logs/` | Creación del origen/data mart, ETL y log de ejecución anterior |
| `datos/`, `powerbi/respaldo/` | Fuentes complementarias y exportaciones anónimas |
| `powerbi/`, `evidencias/` | PBIX, DAX, guía y gráficos BI existentes |
| `bigdata/` | Pipeline local, notebooks, salidas Gold y visualizaciones |
| `experimentos/` | Suites BI y Big Data, resultados y gráficos |
| `analisis/` | Verificación de KPI y generadores de figuras/informe |
| `docs/` | Informe, guía individual y defensa |

## Experimentos asignados

| Integrante | BI | Big Data |
|---|---|---|
| Quispe Ruiz, Johana Carolina | EXP-BI-01 | EXP-BD-01 |
| Sotelo Espinoza, Leoneld Mathias | EXP-BI-02 | EXP-BD-02 |
| Montalvo Abrego, Camila Nadine | EXP-BI-03 | EXP-BD-03 |
| Arroyo Julca, Mishell Alexandra Belen | EXP-BI-04 | EXP-BD-04 |
| Cordova Paz, Diego Jason | EXP-BI-05 | EXP-BD-05 |

Cada prueba compara por lo menos tres escenarios. La configuración y los resultados medidos se encuentran en los JSON; las latencias dependen del hardware, caché y carga y no son predicciones de producción.

## Ejecución rápida local

Desde la raíz, crear un entorno Python 3.11 con Java compatible (el requisito `jdk4py` ofrece un JDK local):

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r bigdata/requirements.txt
python bigdata/pipeline_medallion_spark.py
python experimentos/bi/ejecutar_experimentos_bi.py
python experimentos/bigdata/ejecutar_experimentos_bigdata.py
python analisis/generar_figuras_finales.py
python analisis/generar_informe_final.py
```

Las suites sobrescriben los resultados existentes y eliminan sus directorios temporales fijos; leer precauciones y pasos de Databricks en [REPLICACION.md](REPLICACION.md). Para SQL Server/Power BI se necesitan además las dependencias de `etl/requirements.txt` e infraestructura externa.
