#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generar_notebooks_databricks.py — Construye los notebooks oficiales .ipynb listos para
importar en Databricks (Workspace -> Import) o ejecutar localmente con Jupyter/PySpark.

Notebooks generados en `bigdata/notebooks/`:
  - 01_Ingesta_y_EDA_Bronze.ipynb
  - 02_Limpieza_y_Transformacion_Silver.ipynb
  - 03_Capa_Gold_KPIs_Dashboard_y_Genie.ipynb
  - 04_Experimentos_BigData_Equipo_5_Integrantes.ipynb
"""

import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
NB_DIR = RAIZ / "bigdata" / "notebooks"
NB_DIR.mkdir(parents=True, exist_ok=True)
MED = json.loads((RAIZ / "bigdata" / "salidas" / "medallion_resultados.json").read_text(encoding="utf-8"))
EXP_BD = json.loads((RAIZ / "experimentos" / "salidas" / "resultados_experimentos_bigdata.json").read_text(encoding="utf-8"))


def md_cell(text: str):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in text.strip().splitlines()],
    }


def code_cell(code: str, output_text: str = None):
    # Las salidas se producen al ejecutar el notebook; no se insertan salidas simuladas.
    return {"cell_type": "code", "execution_count": None, "metadata": {},
            "outputs": [], "source": [line + "\n" for line in code.strip().splitlines()]}



def guardar_nb(filename: str, cells: list):
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3 (Databricks / PySpark)", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    path = NB_DIR / filename
    path.write_text(json.dumps(nb, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Notebook creado: {path}")


def nb_01():
    cells = [
        md_cell("""
# Notebook 01 — Ingesta Multiformato (Capa Bronze) y Análisis Exploratorio (EDA) en Databricks
**Proyecto:** Implementación de Soluciones BI, Big Data y Experimentación — Unity Perú SAC
**Curso:** Base de Datos Avanzadas y Big Data (NRC 26055) · Periodo 2026-2
**Arquitectura:** Medallion Lakehouse (Capa Bronze) sobre Apache Spark / Delta Lake

### Propósito del Notebook
1. Ingestar las **cuatro fuentes heterogéneas** del almacén de componentes tecnológicos en **DataFrames de Apache Spark**:
   - Movimientos y catálogo operacional provenientes de `ALMACEN_ANONIMO` (5,348 hechos, 24 componentes, 3,685 lotes).
   - Planificación mensual de demanda (`Plan_Demanda_2026`, 63 registros).
   - Serie horaria de sensores IoT ambientales (`Sensores_Almacen_2026.txt`, 25,824 lecturas).
   - Auditoría de conteos cíclicos (`Conteos_Ciclicos_2026.txt`, 72 registros).
2. Persistir los datasets crudos con metadatos de linaje (`_fuente`, `_ts_ingesta`) en la **Capa Bronze** (Delta / Parquet).
3. Ejecutar el **Análisis Exploratorio de Datos (EDA)** distribuido (`printSchema`, `describe`, conteo de valores nulos y distribución por tipo de movimiento).
"""),
        code_cell("""
# Configuración compatible tanto con Databricks Runtime como con ejecución PySpark local
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("UnityPeru_01_Bronze_EDA").getOrCreate()

# Ruta base: en Databricks ajustar a '/Volumes/workspace/default/unity_datos' o 'dbfs:/FileStore/unity_peru'
BASE_DATOS = "dbfs:/FileStore/unity_peru/datos"
BASE_RESPALDO = "dbfs:/FileStore/unity_peru/respaldo"
FORMATO_TABLA = "delta"  # Cambiar a 'parquet' si se ejecuta fuera de Databricks

print(f"Sesión iniciada correctamente — Apache Spark {spark.version}")
""", "Sesión iniciada correctamente — Apache Spark 3.5.3"),
        md_cell("## 1. Carga de Fuentes en DataFrames de Apache Spark y Escritura en Capa Bronze"),
        code_cell("""
# 1.1 Lectura de fuentes operacionales y complementarias en DataFrames de Spark
df_mov_raw = (
    spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
    .csv(f"{BASE_RESPALDO}/Fact_Movimiento.csv")
    .withColumn("_fuente", F.lit("OLTP_ALMACEN_ANONIMO"))
    .withColumn("_ts_ingesta", F.current_timestamp())
)

df_prod_raw = (
    spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
    .csv(f"{BASE_RESPALDO}/Dim_Producto.csv")
    .withColumn("_fuente", F.lit("OLTP_ALMACEN_ANONIMO"))
    .withColumn("_ts_ingesta", F.current_timestamp())
)

df_tipo_raw = (
    spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
    .csv(f"{BASE_RESPALDO}/Dim_TipoMovimiento.csv")
)

df_proy_raw = (
    spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
    .csv(f"{BASE_RESPALDO}/Dim_Proyecto.csv")
)

df_plan_raw = (
    spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
    .csv(f"{BASE_RESPALDO}/stg_Plan_Mensual.csv")
    .withColumn("_fuente", F.lit("Plan_Demanda_2026.xlsx"))
    .withColumn("_ts_ingesta", F.current_timestamp())
)

df_sensores_raw = (
    spark.read.option("header", "true").option("inferSchema", "true")
    .csv(f"{BASE_DATOS}/Sensores_Almacen_2026.txt")
    .withColumn("_fuente", F.lit("Sensores_Almacen_2026.txt"))
    .withColumn("_ts_ingesta", F.current_timestamp())
)

df_conteos_raw = (
    spark.read.option("header", "true").option("inferSchema", "true")
    .csv(f"{BASE_DATOS}/Conteos_Ciclicos_2026.txt")
    .withColumn("_fuente", F.lit("Conteos_Ciclicos_2026.txt"))
    .withColumn("_ts_ingesta", F.current_timestamp())
)

# 1.2 Persistencia como tablas administradas de la Capa Bronze en Databricks
spark.sql("CREATE SCHEMA IF NOT EXISTS unity_bronze")
df_mov_raw.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_bronze.movimientos_raw")
df_prod_raw.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_bronze.productos_raw")
df_tipo_raw.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_bronze.tipos_movimiento_raw")
df_proy_raw.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_bronze.proyectos_raw")
df_plan_raw.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_bronze.plan_demanda_raw")
df_sensores_raw.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_bronze.sensores_iot_raw")
df_conteos_raw.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_bronze.conteos_ciclicos_raw")

print("Conteos Capa Bronze:")
for nombre, df_b in [
    ("unity_bronze.movimientos_raw", df_mov_raw),
    ("unity_bronze.productos_raw", df_prod_raw),
    ("unity_bronze.plan_demanda_raw", df_plan_raw),
    ("unity_bronze.sensores_iot_raw", df_sensores_raw),
    ("unity_bronze.conteos_ciclicos_raw", df_conteos_raw),
]:
    print(f"  {nombre:<36}: {df_b.count():>7,} registros")
""", """Conteos Capa Bronze:
  unity_bronze.movimientos_raw        :   5,348 registros
  unity_bronze.productos_raw          :      24 registros
  unity_bronze.plan_demanda_raw       :      63 registros
  unity_bronze.sensores_iot_raw       :  25,824 registros
  unity_bronze.conteos_ciclicos_raw   :      72 registros"""),
        md_cell("## 2. Análisis Exploratorio de Datos (EDA) con PySpark (`describe`, perfil de nulos y distribuciones)"),
        code_cell("""
# 2.1 Estadísticos descriptivos de las variables numéricas de movimientos
print("=== Estadísticos descriptivos: Movimientos de Almacén ===")
df_mov_raw.select("Cantidad", "Costo_Total", "Dias_Permanencia").describe().show()

print("=== Estadísticos descriptivos: Sensores Ambientales IoT ===")
df_sensores_raw.select("temperatura_c", "humedad_pct").describe().show()
""", """=== Estadísticos descriptivos: Movimientos de Almacén ===
+-------+------------------+------------------+------------------+
|summary|          Cantidad|       Costo_Total|  Dias_Permanencia|
+-------+------------------+------------------+------------------+
|  count|              5348|              5348|              1663|
|   mean|            5.7036|          15843.91|            3.8687|
| stddev|            3.8331|          24671.76|            2.5258|
|    min|                 1|             180.0|               0.0|
|    max|                16|          222000.0|              20.0|
+-------+------------------+------------------+------------------+

=== Estadísticos descriptivos: Sensores Ambientales IoT ===
+-------+------------------+------------------+
|summary|     temperatura_c|       humedad_pct|
+-------+------------------+------------------+
|  count|             25824|             25824|
|   mean|           21.0485|           50.0523|
| stddev|            1.3296|            4.3492|
|    min|              17.5|              31.8|
|    max|              30.6|              83.1|
+-------+------------------+------------------+"""),
        code_cell("""
# 2.2 Auditoría de valores nulos por columna (interpretación semántica de nulidad)
cols_audit = ["Id_Movimiento_NK", "Id_Fecha", "Id_Producto", "Id_TipoMovimiento",
              "Id_Proyecto", "Id_Proveedor", "Cantidad", "Costo_Total", "Dias_Permanencia"]

df_nulos = df_mov_raw.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in cols_audit
])
df_nulos.show()

# 2.3 Distribución de movimientos y unidades por tipo operacional
(
    df_mov_raw.join(df_tipo_raw, on="Id_TipoMovimiento", how="left")
    .groupBy("Codigo", "Es_Entrada", "Es_Salida_Real")
    .agg(
        F.count("*").alias("movimientos"),
        F.sum("Cantidad").alias("unidades_totales"),
        F.round(F.sum("Costo_Total"), 2).alias("costo_total_soles"),
    )
    .show()
)
""", """+----------------+--------+-----------+-----------------+-----------+------------+--------+-----------+----------------+
|Id_Movimiento_NK|Id_Fecha|Id_Producto|Id_TipoMovimiento|Id_Proyecto|Id_Proveedor|Cantidad|Costo_Total|Dias_Permanencia|
+----------------+--------+-----------+-----------------+-----------+------------+--------+-----------+----------------+
|               0|       0|          0|                0|       3685|        1663|       0|          0|            3685|
+----------------+--------+-----------+-----------------+-----------+------------+--------+-----------+----------------+

+-------+----------+--------------+-----------+----------------+-----------------+
| Codigo|Es_Entrada|Es_Salida_Real|movimientos|unidades_totales|costo_total_soles|
+-------+----------+--------------+-----------+----------------+-----------------+
|INGRESO|         1|             0|       3685|           15425|      42772640.00|
| SALIDA|         0|             1|       1663|           15078|      41960580.00|
+-------+----------+--------------+-----------+----------------+-----------------+"""),
    ]
    guardar_nb("01_Ingesta_y_EDA_Bronze.ipynb", cells)


def nb_02():
    cells = [
        md_cell("""
# Notebook 02 — Limpieza, Calidad y Transformación Distribuida (Capa Silver)
**Proyecto:** Implementación de Soluciones BI, Big Data y Experimentación — Unity Perú SAC
**Arquitectura:** Medallion Lakehouse (Bronze $\\rightarrow$ Silver)

### Operaciones de DataFrames PySpark demostradas
- `select()`, `withColumnRenamed()`, `withColumn()`: normalización a `snake_case` y derivación de atributos temporales (`anio`, `mes`, `hora`) y tramos de velocidad FIFO.
- `na.drop()`, `na.fill()`: descarte de registros con atributos obligatorios nulos (RQ-01) e imputación semántica de llaves opcionales (`SIN_PROYECTO`).
- `filter()`, `dropDuplicates()`: cumplimiento estricto de cantidades positivas (RQ-02) y unicidad de eventos (RQ-04).
- `join()` (inner y left): validación referencial contra catálogo (RQ-03) y desnormalización controlada hacia tablas Silver particionadas por `mes`.
"""),
        code_cell("""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("UnityPeru_02_Silver").getOrCreate()
spark.sql("CREATE SCHEMA IF NOT EXISTS unity_silver")
FORMATO_TABLA = "delta"

df_mov_raw = spark.table("unity_bronze.movimientos_raw")
df_prod_raw = spark.table("unity_bronze.productos_raw")
df_tipo_raw = spark.table("unity_bronze.tipos_movimiento_raw")
df_proy_raw = spark.table("unity_bronze.proyectos_raw")
df_plan_raw = spark.table("unity_bronze.plan_demanda_raw")
df_sen_raw  = spark.table("unity_bronze.sensores_iot_raw")
df_con_raw  = spark.table("unity_bronze.conteos_ciclicos_raw")
""", ""),
        md_cell("## 1. Limpieza, Renombrado y Enriquecimiento de Catálogo y Movimientos (`silver_movimientos`)"),
        code_cell("""
# 1.1 Estandarización del catálogo tecnológico
df_prod_silver = (
    df_prod_raw.select(
        "Id_Producto", "Cod_Componente", "Descripcion", "Categoria",
        "Marca", "Modelo", "Criticidad", "Stock_Minimo", "Precio_Unitario"
    )
    .withColumnRenamed("Cod_Componente", "codigo_componente")
    .withColumnRenamed("Categoria", "categoria")
    .withColumnRenamed("Criticidad", "criticidad")
    .withColumnRenamed("Stock_Minimo", "stock_minimo")
    .withColumnRenamed("Precio_Unitario", "precio_unitario")
    .na.drop(subset=["Id_Producto", "codigo_componente"])
    .dropDuplicates(["codigo_componente"])
    .withColumn("categoria", F.trim(F.col("categoria")))
    .withColumn("criticidad", F.upper(F.trim(F.col("criticidad"))))
)

# 1.2 Aplicación de reglas RQ-01..RQ-04 y enriquecimiento en Capa Silver
df_mov_silver = (
    df_mov_raw
    .na.drop(subset=["Id_Movimiento_NK", "Id_Fecha", "Id_Producto", "Id_TipoMovimiento", "Cantidad"])
    .filter(F.col("Cantidad") > 0)
    .dropDuplicates(["Id_Movimiento_NK"])
    .withColumnRenamed("Id_Movimiento_NK", "id_movimiento")
    .withColumnRenamed("Cantidad", "cantidad")
    .withColumnRenamed("Costo_Total", "costo_total")
    .withColumnRenamed("Dias_Permanencia", "dias_permanencia")
    .withColumn("fecha_movimiento", F.to_date(F.col("Fecha").cast("string")))
    .withColumn("anio", F.year("fecha_movimiento"))
    .withColumn("mes", F.month("fecha_movimiento"))
    .na.fill({"Id_Proyecto": -1, "Id_Proveedor": -1})
    .join(
        df_prod_silver.select("Id_Producto", "codigo_componente", "categoria", "criticidad", "precio_unitario", "stock_minimo"),
        on="Id_Producto", how="inner"
    )
    .join(
        df_tipo_raw.select(
            "Id_TipoMovimiento",
            F.col("Codigo").alias("tipo_movimiento"),
            F.col("Es_Entrada").alias("es_entrada"),
            F.col("Es_Salida_Real").alias("es_salida_real")
        ),
        on="Id_TipoMovimiento", how="inner"
    )
    .join(
        df_proy_raw.select(
            "Id_Proyecto",
            F.col("Codigo_Proyecto").alias("codigo_proyecto"),
            F.col("Nombre_Proyecto").alias("nombre_proyecto")
        ),
        on="Id_Proyecto", how="left"
    )
    .na.fill({"codigo_proyecto": "SIN_PROYECTO", "nombre_proyecto": "Operación general de almacén"})
    .withColumn(
        "tramo_permanencia",
        F.when(F.col("es_entrada") == 1, F.lit("INGRESO_LOTE"))
        .when(F.col("dias_permanencia") <= 3, F.lit("RAPIDO_0_3D"))
        .when(F.col("dias_permanencia") <= 7, F.lit("NORMAL_4_7D"))
        .when(F.col("dias_permanencia") <= 10, F.lit("MODERADO_8_10D"))
        .otherwise(F.lit("RETENIDO_MAS_10D"))
    )
)

df_prod_silver.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_silver.productos")
(
    df_mov_silver.write.format(FORMATO_TABLA)
    .mode("overwrite")
    .partitionBy("mes")
    .saveAsTable("unity_silver.movimientos")
)

df_mov_silver.groupBy("tramo_permanencia").agg(
    F.count("*").alias("movimientos"), F.sum("cantidad").alias("unidades")
).orderBy(F.desc("movimientos")).show()
""", """+-----------------+-----------+--------+
|tramo_permanencia|movimientos|unidades|
+-----------------+-----------+--------+
|     INGRESO_LOTE|       3685|   15425|
|      NORMAL_4_7D|        844|    7726|
|      RAPIDO_0_3D|        665|    6064|
|   MODERADO_8_10D|        136|    1132|
| RETENIDO_MAS_10D|         18|     156|
+-----------------+-----------+--------+"""),
        md_cell("## 2. Preparación de Telemetría IoT, Conteos Cíclicos y Plan Mensual en Capa Silver"),
        code_cell("""
df_sensores_silver = (
    df_sen_raw
    .na.drop(subset=["timestamp", "sensor_id", "codigo_ubicacion", "temperatura_c", "humedad_pct"])
    .dropDuplicates(["timestamp", "sensor_id"])
    .withColumn("ts_lectura", F.to_timestamp("timestamp"))
    .withColumn("fecha", F.to_date("ts_lectura"))
    .withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("hora", F.hour("ts_lectura"))
    .withColumn(
        "fuera_rango",
        F.when(
            (F.col("temperatura_c") < 18.0) | (F.col("temperatura_c") > 27.0) |
            (F.col("humedad_pct") < 40.0) | (F.col("humedad_pct") > 60.0), 1
        ).otherwise(0)
    )
)

df_conteos_silver = (
    df_con_raw
    .na.drop(subset=["fecha", "cod_componente", "stock_sistema", "stock_fisico"])
    .withColumnRenamed("cod_componente", "codigo_componente")
    .withColumn("fecha_conteo", F.to_date("fecha"))
    .withColumn("mes", F.month("fecha_conteo"))
    .withColumn("diferencia_calc", F.col("stock_fisico") - F.col("stock_sistema"))
    .withColumn("denominador", F.greatest(F.col("stock_sistema"), F.col("stock_fisico"), F.lit(1)))
    .withColumn("exactitud_linea", F.round(1.0 - (F.abs(F.col("diferencia_calc")) / F.col("denominador")), 4))
)

df_plan_silver = (
    df_plan_raw
    .na.drop(subset=["Anio", "Mes", "Categoria", "Entradas_Plan", "Salidas_Plan"])
    .withColumnRenamed("Anio", "anio")
    .withColumnRenamed("Mes", "mes")
    .withColumnRenamed("Categoria", "categoria")
    .withColumnRenamed("Entradas_Plan", "entradas_plan")
    .withColumnRenamed("Salidas_Plan", "salidas_plan")
    .filter((F.col("entradas_plan") >= 0) & (F.col("salidas_plan") >= 0))
)

df_sensores_silver.write.format(FORMATO_TABLA).mode("overwrite").partitionBy("mes").saveAsTable("unity_silver.sensores_iot")
df_conteos_silver.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_silver.conteos_ciclicos")
df_plan_silver.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_silver.plan_demanda")

print(f"Silver completado -> Movimientos: {df_mov_silver.count():,} | Sensores: {df_sensores_silver.count():,} | Conteos: {df_conteos_silver.count()}")
""", "Silver completado -> Movimientos: 5,348 | Sensores: 25,824 | Conteos: 72"),
    ]
    guardar_nb("02_Limpieza_y_Transformacion_Silver.ipynb", cells)


def nb_03():
    cells = [
        md_cell("""
# Notebook 03 — Capa Gold, Indicadores, Visualizaciones en Databricks y Consultas con Genie IA
**Proyecto:** Implementación de Soluciones BI, Big Data y Experimentación — Unity Perú SAC
**Arquitectura:** Medallion Lakehouse (Silver $\\rightarrow$ Gold + Dashboard Lakeview + Databricks Genie)

### Tablas Analíticas Persistidas en Capa Gold (`unity_gold`)
1. `unity_gold.kpi_mensual`: Entradas, salidas, rotación, TPP, inversión retenida, stock acumulado (Window) y cumplimiento del plan (VM).
2. `unity_gold.kpi_categoria`: Exposición financiera, rotación, TPP y cumplimiento por categoría tecnológica.
3. `unity_gold.stock_componentes`: Disponibilidad neta vs. stock mínimo y exactitud de conteos cíclicos por componente.
4. `unity_gold.consumo_proyectos`: Unidades y presupuesto despachado por proyecto de misión crítica.
5. `unity_gold.telemetria_almacen`: Condiciones térmicas y de humedad agregadas por ubicación y mes.
"""),
        code_cell("""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("UnityPeru_03_Gold_Analytics").getOrCreate()
spark.sql("CREATE SCHEMA IF NOT EXISTS unity_gold")
FORMATO_TABLA = "delta"

df_mov = spark.table("unity_silver.movimientos")
df_plan = spark.table("unity_silver.plan_demanda")
df_con = spark.table("unity_silver.conteos_ciclicos")
df_sen = spark.table("unity_silver.sensores_iot")
""", ""),
        code_cell("""
# 1. Tabla Gold KPI Mensual con Funciones de Ventana (Window)
plan_mes = df_plan.groupBy("anio", "mes").agg(
    F.sum("entradas_plan").alias("entradas_plan"),
    F.sum("salidas_plan").alias("salidas_plan")
)

mov_mes = df_mov.groupBy("anio", "mes").agg(
    F.sum(F.when(F.col("es_entrada") == 1, F.col("cantidad")).otherwise(0)).alias("unidades_entradas"),
    F.sum(F.when(F.col("es_salida_real") == 1, F.col("cantidad")).otherwise(0)).alias("unidades_salidas"),
    F.round(F.sum(F.when(F.col("es_entrada") == 1, F.col("costo_total")).otherwise(0)), 2).alias("costo_entradas"),
    F.round(F.sum(F.when(F.col("es_salida_real") == 1, F.col("costo_total")).otherwise(0)), 2).alias("costo_salidas"),
    F.round(F.avg("dias_permanencia"), 2).alias("tpp_dias")
)

win_acum = Window.orderBy("anio", "mes").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_gold_mensual = (
    mov_mes.join(plan_mes, on=["anio", "mes"], how="left")
    .withColumn("rotacion_flujo", F.round(F.col("unidades_salidas") / F.col("unidades_entradas"), 4))
    .withColumn("balance_neto_mes", F.col("unidades_entradas") - F.col("unidades_salidas"))
    .withColumn("stock_acumulado", F.sum("balance_neto_mes").over(win_acum))
    .withColumn("vm_entradas_pct", F.round((F.col("unidades_entradas") - F.col("entradas_plan")) / F.col("entradas_plan") * 100, 2))
    .withColumn("vm_salidas_pct", F.round((F.col("unidades_salidas") - F.col("salidas_plan")) / F.col("salidas_plan") * 100, 2))
    .orderBy("anio", "mes")
)

df_gold_mensual.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_gold.kpi_mensual")
display(df_gold_mensual) if "display" in globals() else df_gold_mensual.show()
""", """+----+---+-----------------+----------------+--------------+-------------+--------+-------------+------------+--------------+----------------+---------------+---------------+--------------+
|anio|mes|unidades_entradas|unidades_salidas|costo_entradas|costo_salidas|tpp_dias|entradas_plan|salidas_plan|rotacion_flujo|balance_neto_mes|stock_acumulado|vm_entradas_pct|vm_salidas_pct|
+----+---+-----------------+----------------+--------------+-------------+--------+-------------+------------+--------------+----------------+---------------+---------------+--------------+
|2026|  1|             1585|            1499|    4291720.00|   4058600.00|    0.61|       1571.0|      1481.0|        0.9457|              86|             86|           0.89|          1.22|
|2026|  2|             1618|            1524|    4543540.00|   4311850.00|    2.29|       1629.0|      1541.0|        0.9419|              94|            180|          -0.68|         -1.10|
|2026|  3|             1772|            1737|    4881160.00|   4796420.00|    3.48|       1759.0|      1804.0|        0.9802|              35|            215|           0.74|         -3.71|
|2026|  9|             1780|            1771|    4908450.00|   4889310.00|    5.57|       1717.0|      1750.0|        0.9949|               9|            347|           3.67|          1.20|
+----+---+-----------------+----------------+--------------+-------------+--------+-------------+------------+--------------+----------------+---------------+---------------+--------------+"""),
        code_cell("""
# 2. Tablas Gold por Categoría, Componente y Proyecto
df_gold_categoria = (
    df_mov.groupBy("categoria")
    .agg(
        F.countDistinct("codigo_componente").alias("componentes"),
        F.sum(F.when(F.col("es_entrada") == 1, F.col("cantidad")).otherwise(0)).alias("unidades_entradas"),
        F.sum(F.when(F.col("es_salida_real") == 1, F.col("cantidad")).otherwise(0)).alias("unidades_salidas"),
        F.round(F.sum(F.when(F.col("es_entrada") == 1, F.col("costo_total")).otherwise(0)), 2).alias("costo_entradas"),
        F.round(F.sum(F.when(F.col("es_salida_real") == 1, F.col("costo_total")).otherwise(0)), 2).alias("costo_salidas"),
        F.round(F.avg("dias_permanencia"), 2).alias("tpp_dias")
    )
    .withColumn("rotacion_flujo", F.round(F.col("unidades_salidas") / F.col("unidades_entradas"), 4))
    .withColumn("inversion_retenida", F.round(F.col("costo_entradas") - F.col("costo_salidas"), 2))
    .orderBy(F.desc("costo_entradas"))
)

df_gold_proyectos = (
    df_mov.filter(F.col("es_salida_real") == 1)
    .groupBy("codigo_proyecto", "nombre_proyecto")
    .agg(
        F.count("*").alias("num_despachos"),
        F.sum("cantidad").alias("unidades_consumidas"),
        F.round(F.sum("costo_total"), 2).alias("costo_consumido"),
        F.round(F.avg("dias_permanencia"), 2).alias("tpp_dias")
    )
    .orderBy(F.desc("unidades_consumidas"))
)

df_gold_categoria.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_gold.kpi_categoria")
df_gold_proyectos.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_gold.consumo_proyectos")

df_gold_categoria.show(truncate=False)
df_gold_proyectos.show(truncate=False)
""", """+---------------+-----------+-----------------+----------------+--------------+-------------+--------+--------------+------------------+
|categoria      |componentes|unidades_entradas|unidades_salidas|costo_entradas|costo_salidas|tpp_dias|rotacion_flujo|inversion_retenida|
+---------------+-----------+-----------------+----------------+--------------+-------------+--------+--------------+------------------+
|Energía        |3          |1973             |1941            |19271000.00   |18963500.00  |3.45    |0.9838        |307500.00         |
|Procesamiento  |3          |1925             |1893            |8358100.00    |8243700.00   |2.92    |0.9834        |114400.00         |
|Almacenamiento |4          |2479             |2421            |5244950.00    |5122150.00   |4.05    |0.9766        |122800.00         |
|Infraestructura|4          |2428             |2365            |3916150.00    |3796300.00   |4.47    |0.9741        |119850.00         |
|Redes          |4          |2212             |2154            |2443030.00    |2382450.00   |4.00    |0.9738        |60580.00          |
|Memoria        |3          |2187             |2131            |2408320.00    |2347130.00   |4.44    |0.9744        |61190.00          |
|Refrigeración  |3          |2221             |2173            |1131090.00    |1105350.00   |3.61    |0.9784        |25740.00          |
+---------------+-----------+-----------------+----------------+--------------+-------------+--------+--------------+------------------+"""),
        code_cell("""
# 2.3 Disponibilidad por componente y telemetría agregada
exactitud = df_con.groupBy("codigo_componente").agg(F.count("*").alias("num_conteos"), F.round(F.avg("exactitud_linea") * 100, 2).alias("exactitud_pct"))
componentes = (df_mov.groupBy("codigo_componente", "categoria", "criticidad", "stock_minimo", "precio_unitario")
    .agg(F.sum(F.when(F.col("es_entrada") == 1, F.col("cantidad")).otherwise(0)).alias("entradas"),
         F.sum(F.when(F.col("es_salida_real") == 1, F.col("cantidad")).otherwise(0)).alias("salidas"),
         F.round(F.avg("dias_permanencia"), 2).alias("tpp_dias"))
    .withColumn("stock_neto", F.col("entradas") - F.col("salidas"))
    .withColumn("cumple_stock_minimo", F.when(F.col("stock_neto") >= F.col("stock_minimo"), 1).otherwise(0))
    .join(exactitud, "codigo_componente", "left"))
telemetria = (df_sen.groupBy("codigo_ubicacion", "mes")
    .agg(F.count("*").alias("lecturas_totales"), F.round(F.avg("temperatura_c"), 2).alias("temp_promedio_c"),
         F.round(F.avg("humedad_pct"), 2).alias("hum_promedio_pct")))
componentes.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_gold.stock_componentes")
telemetria.write.format(FORMATO_TABLA).mode("overwrite").saveAsTable("unity_gold.telemetria_almacen")
print("componentes:", componentes.count(), "telemetría:", telemetria.count())
"""),
        md_cell("""
## 3. Conciliación Cruzada BI vs. Big Data y Consultas Analíticas para Databricks Genie (IA)
Propuesta pendiente de despliegue: conectar `unity_gold.*` a un **Espacio Genie de Databricks** y verificar sus respuestas contra SQL y el respaldo BI:
- **Prompt Genie 1:** *"¿Cuál es la categoría tecnológica con mayor inversión retenida y qué porcentaje del costo total de entradas representa?"*
- **Prompt Genie 2:** *"Muestra la evolución mensual del TPP en días frente a las unidades de entrada y salida de enero a setiembre de 2026."*
- **Prompt Genie 3:** *"¿Qué proyecto ha consumido más unidades y cuál es su tiempo promedio de despacho?"*
"""),
        code_cell("""
# Verificación SQL exacta de los KPI oficiales en Databricks
spark.sql('''
    SELECT
        COUNT(*) AS movimientos_verificados,
        SUM(CASE WHEN es_entrada = 1 THEN cantidad ELSE 0 END) AS entradas_u,
        SUM(CASE WHEN es_salida_real = 1 THEN cantidad ELSE 0 END) AS salidas_u,
        ROUND(SUM(CASE WHEN es_entrada = 1 THEN costo_total ELSE 0 END) -
              SUM(CASE WHEN es_salida_real = 1 THEN costo_total ELSE 0 END), 2) AS inversion_retenida_soles,
        ROUND(AVG(dias_permanencia), 6) AS tpp_exacto_dias,
        ROUND(SUM(CASE WHEN es_salida_real = 1 THEN cantidad ELSE 0 END) * 1.0 /
              SUM(CASE WHEN es_entrada = 1 THEN cantidad ELSE 0 END), 4) AS rotacion_global
    FROM unity_silver.movimientos
''').show()
""", """+-----------------------+----------+---------+------------------------+---------------+---------------+
|movimientos_verificados|entradas_u|salidas_u|inversion_retenida_soles|tpp_exacto_dias|rotacion_global|
+-----------------------+----------+---------+------------------------+---------------+---------------+
|                   5348|     15425|    15078|               812060.00|       3.868701|         0.9775|
+-----------------------+----------+---------+------------------------+---------------+---------------+"""),
    ]
    guardar_nb("03_Capa_Gold_KPIs_Dashboard_y_Genie.ipynb", cells)


def nb_04():
    cells = [
        md_cell("""
# Notebook 04 — Experimentos de Big Data por Integrante (EXP-BD-01 a EXP-BD-05)
**Proyecto:** Implementación de Soluciones BI, Big Data y Experimentación — Unity Perú SAC

Cada integrante ejecuta y documenta su propio experimento sobre la solución Big Data en Databricks / PySpark:
1. **EXP-BD-01 — Quispe Ruiz, Johana Carolina:** Formatos de almacenamiento Lakehouse (`CSV` vs. `JSON` vs. `Parquet Snappy`).
2. **EXP-BD-02 — Sotelo Espinoza, Leoneld Mathias:** Transformación en Capa Silver (`Python UDF` vs. `Spark SQL` vs. `Expresiones Nativas Catalyst`).
3. **EXP-BD-03 — Montalvo Abrego, Camila Nadine:** `Partition Pruning` por `mes` y persistencia `MEMORY_AND_DISK` para Dashboards / Genie.
4. **EXP-BD-04 — Arroyo Julca, Mishell Alexandra Belen:** Escalamiento volumétrico progresivo `x1`, `x10`, `x50` y `x100` (hasta **3,117,200 registros**).
5. **EXP-BD-05 — Cordova Paz, Diego Jason:** Ajuste de `spark.sql.shuffle.partitions` (`200`, `64`, `4`) y `BroadcastHashJoin` vs. `SortMergeJoin`.
"""),
    ]
    cells.append(md_cell("""## Preparación
Clone el repositorio como carpeta Git de Databricks. Configure `REPO` con su ruta absoluta bajo `/Workspace/...`. Las funciones ejecutan Spark, no muestran datos prefabricados. Corra solo la sección asignada; los resultados cambian según el entorno. Cada función usa `/tmp/unity_spark_experimentos` y puede sobrescribir ese directorio: no lo comparta entre ejecuciones simultáneas. Estas funciones usan archivos locales del driver: se requiere un nodo único con acceso al filesystem local; para multinodo/serverless adapte las rutas y la medición de tamaños a almacenamiento compartido.
"""))
    cells.append(code_cell("""
import importlib.util
from pathlib import Path
REPO = Path('/Workspace/Users/usuario/bigdata_exp')  # CAMBIAR: ruta de la carpeta Git clonada
assert (REPO / 'experimentos/bigdata/ejecutar_experimentos_bigdata.py').exists(), 'Ajuste REPO'
spec = importlib.util.spec_from_file_location('exp_bd', REPO / 'experimentos/bigdata/ejecutar_experimentos_bigdata.py')
exp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exp)
df_silver, df_prod, df_tipo, df_sen = exp.cargar_base_silver(spark)
"""))
    funciones = ['exp_bd_01_johana(spark, df_silver)', 'exp_bd_02_leoneld(spark, df_silver)',
                 'exp_bd_03_camila(spark, df_silver)', 'exp_bd_04_mishell(spark, df_silver, df_sen)',
                 'exp_bd_05_diego(spark, df_prod, df_tipo)']
    for experimento, llamada in zip(EXP_BD['experimentos'], funciones):
        cells.append(md_cell(f"## {experimento['id']} — {experimento['integrante']}\n{experimento['titulo']}"))
        cells.append(code_cell(f"resultado = exp.{llamada}\nfor escenario in resultado['escenarios']:\n    print(escenario)"))
    guardar_nb("04_Experimentos_BigData_Equipo_5_Integrantes.ipynb", cells)


if __name__ == "__main__":
    nb_01()
    nb_02()
    nb_03()
    nb_04()
