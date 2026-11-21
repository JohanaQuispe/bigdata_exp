#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline_medallion_spark.py — Solución Big Data de Unity Perú SAC
Arquitectura Medallion (Bronze -> Silver -> Gold) sobre Apache Spark / Databricks.

Ejecuta sobre PySpark 3.5.x en local o en Databricks:
  1. Ingesta de las 4 fuentes heterogéneas en DataFrames de Apache Spark (Capa Bronze).
  2. Análisis Exploratorio de Datos (EDA) distribuido: esquemas, describe(), conteo de nulos,
     estadísticos por categoría, tipo de movimiento y sensores.
  3. Limpieza, estandarización y preparación con operaciones nativas de DataFrames:
     select, filter, where, na.drop, na.fill, dropDuplicates, withColumn, withColumnRenamed,
     join, groupBy, agg, Window functions (Capa Silver).
  4. Construcción de tablas analíticas agregadas de negocio (Capa Gold):
     - gold_kpi_mensual
     - gold_kpi_categoria
     - gold_trazabilidad_proyecto
     - gold_monitoreo_sensores
  5. Persistencia columnar en Parquet / Delta y exportación de métricas verificadas a JSON/CSV.
"""

import json
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

# Configurar JAVA_HOME automáticamente si se usa jdk4py en entorno local
try:
    import jdk4py
    if not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = str(jdk4py.JAVA_HOME)
except ImportError:
    pass

import openpyxl
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RAIZ = Path(__file__).resolve().parents[1]
DATOS_DIR = RAIZ / "datos"
RESPALDO_DIR = RAIZ / "powerbi" / "respaldo"
SALIDAS_DIR = RAIZ / "bigdata" / "salidas"
LAKEHOUSE_DIR = Path("/tmp/unity_lakehouse")

SALIDAS_DIR.mkdir(parents=True, exist_ok=True)


def crear_sesion_spark(app_name: str = "UnityPeru_Medallion_BigData") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def convertir_excel_a_csv_temporal(xlsx_path: Path, csv_dest: Path):
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb["Plan_Mensual"]
    with open(csv_dest, "w", encoding="utf-8") as fh:
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                fh.write(",".join(str(c) for c in row[:5]) + "\n")
            elif row[0] is not None and isinstance(row[0], int):
                fh.write(",".join(str(c) for c in row[:5]) + "\n")


def main():
    t_inicio_global = time.perf_counter()
    if LAKEHOUSE_DIR.exists():
        shutil.rmtree(LAKEHOUSE_DIR)
    LAKEHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    spark = crear_sesion_spark()
    print(f"=== SOLUCIÓN BIG DATA — UNITY PERÚ SAC (Apache Spark {spark.version}) ===")

    # =========================================================================
    # 1. CAPA BRONZE: Ingesta cruda multiformato en DataFrames de Spark
    # =========================================================================
    t0 = time.perf_counter()
    plan_csv_tmp = LAKEHOUSE_DIR / "plan_mensual_raw.csv"
    convertir_excel_a_csv_temporal(DATOS_DIR / "Plan_Demanda_2026.xlsx", plan_csv_tmp)

    df_mov_raw = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Fact_Movimiento.csv"))
        .withColumn("_fuente", F.lit("OLTP_ALMACEN_ANONIMO"))
        .withColumn("_ts_ingesta", F.current_timestamp())
    )

    df_prod_raw = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Dim_Producto.csv"))
        .withColumn("_fuente", F.lit("OLTP_ALMACEN_ANONIMO"))
        .withColumn("_ts_ingesta", F.current_timestamp())
    )

    df_tipo_raw = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Dim_TipoMovimiento.csv"))
    )

    df_proy_raw = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Dim_Proyecto.csv"))
    )

    df_alm_raw = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Dim_Almacen.csv"))
    )

    df_lote_raw = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Dim_Lote.csv"))
    )

    df_plan_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(plan_csv_tmp))
        .withColumn("_fuente", F.lit("Plan_Demanda_2026.xlsx"))
        .withColumn("_ts_ingesta", F.current_timestamp())
    )

    df_sensores_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(DATOS_DIR / "Sensores_Almacen_2026.txt"))
        .withColumn("_fuente", F.lit("Sensores_Almacen_2026.txt"))
        .withColumn("_ts_ingesta", F.current_timestamp())
    )

    df_conteos_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(DATOS_DIR / "Conteos_Ciclicos_2026.txt"))
        .withColumn("_fuente", F.lit("Conteos_Ciclicos_2026.txt"))
        .withColumn("_ts_ingesta", F.current_timestamp())
    )

    # Persistir Capa Bronze en formato columnar Parquet (equivalente Delta en Databricks)
    bronze_path = LAKEHOUSE_DIR / "bronze"
    df_mov_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_movimientos"))
    df_prod_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_productos"))
    df_tipo_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_tipos_movimiento"))
    df_proy_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_proyectos"))
    df_alm_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_almacenes"))
    df_lote_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_lotes"))
    df_plan_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_plan_demanda"))
    df_sensores_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_sensores"))
    df_conteos_raw.write.mode("overwrite").parquet(str(bronze_path / "bronze_conteos"))

    t_bronze = round(time.perf_counter() - t0, 3)

    conteos_bronze = {
        "bronze_movimientos": df_mov_raw.count(),
        "bronze_productos": df_prod_raw.count(),
        "bronze_lotes": df_lote_raw.count(),
        "bronze_plan_demanda": df_plan_raw.count(),
        "bronze_sensores": df_sensores_raw.count(),
        "bronze_conteos": df_conteos_raw.count(),
    }
    print(f"[BRONZE] Completado en {t_bronze} s -> {conteos_bronze}")

    # =========================================================================
    # 2. ANÁLISIS EXPLORATORIO DE DATOS (EDA) EN PYSPARK
    # =========================================================================
    t1 = time.perf_counter()
    # Estadísticos descriptivos con describe()
    desc_mov = {
        r["summary"]: {
            "Cantidad": round(float(r["Cantidad"]), 4) if r["Cantidad"] is not None else None,
            "Costo_Total": round(float(r["Costo_Total"]), 2) if r["Costo_Total"] is not None else None,
            "Dias_Permanencia": round(float(r["Dias_Permanencia"]), 4) if r["Dias_Permanencia"] is not None else None,
        }
        for r in df_mov_raw.select("Cantidad", "Costo_Total", "Dias_Permanencia").describe().collect()
    }

    desc_sen = {
        r["summary"]: {
            "temperatura_c": round(float(r["temperatura_c"]), 4) if r["temperatura_c"] is not None else None,
            "humedad_pct": round(float(r["humedad_pct"]), 4) if r["humedad_pct"] is not None else None,
        }
        for r in df_sensores_raw.select("temperatura_c", "humedad_pct").describe().collect()
    }

    # Perfilado de valores nulos por columna en movimientos
    cols_mov_base = [
        "Id_Movimiento_NK", "Id_Fecha", "Id_Producto", "Id_TipoMovimiento",
        "Id_Proyecto", "Id_Proveedor", "Cantidad", "Costo_Total", "Dias_Permanencia",
    ]
    nulos_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in cols_mov_base
    ]
    nulos_mov = df_mov_raw.select(nulos_exprs).collect()[0].asDict()

    # Distribución de movimientos por tipo en crudo
    dist_tipos = [
        r.asDict()
        for r in (
            df_mov_raw.join(df_tipo_raw, on="Id_TipoMovimiento", how="left")
            .groupBy("Codigo", "Es_Entrada", "Es_Salida_Real")
            .agg(
                F.count("*").alias("num_movimientos"),
                F.sum("Cantidad").alias("total_unidades"),
                F.round(F.sum("Costo_Total"), 2).alias("total_costo"),
            )
            .orderBy(F.desc("num_movimientos"))
            .collect()
        )
    ]
    t_eda = round(time.perf_counter() - t1, 3)
    print(f"[EDA] Completado en {t_eda} s -> describe() y perfil de nulos generados.")

    # =========================================================================
    # 3. CAPA SILVER: Limpieza, renombrado, enriquecimiento y calidad con Spark
    # =========================================================================
    t2 = time.perf_counter()

    # 3.1 Limpieza y estandarización del catálogo de productos
    df_prod_silver = (
        df_prod_raw.select(
            "Id_Producto", "Cod_Componente", "Descripcion", "Categoria",
            "Marca", "Modelo", "Criticidad", "Stock_Minimo", "Precio_Unitario",
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

    # 3.2 Limpieza, validación de reglas de calidad y enriquecimiento de movimientos
    df_mov_silver = (
        df_mov_raw
        # Filtrado de reglas de calidad RQ-01 y RQ-02 con filter() y na.drop()
        .na.drop(subset=["Id_Movimiento_NK", "Id_Fecha", "Id_Producto", "Id_TipoMovimiento", "Cantidad"])
        .filter(F.col("Cantidad") > 0)
        .dropDuplicates(["Id_Movimiento_NK"])
        # Renombrado estandarizado snake_case con withColumnRenamed
        .withColumnRenamed("Id_Movimiento_NK", "id_movimiento")
        .withColumnRenamed("Cantidad", "cantidad")
        .withColumnRenamed("Costo_Total", "costo_total")
        .withColumnRenamed("Dias_Permanencia", "dias_permanencia")
        .withColumn("fecha_movimiento", F.to_date(F.col("Fecha").cast("string")))
        .withColumn("anio", F.year("fecha_movimiento"))
        .withColumn("mes", F.month("fecha_movimiento"))
        # Tratamiento de nulos opcionales con na.fill
        .na.fill({"Id_Proyecto": -1, "Id_Proveedor": -1})
        # Enriquecimiento con dimensiones (Producto, TipoMovimiento, Proyecto)
        .join(
            df_prod_silver.select("Id_Producto", "codigo_componente", "categoria", "criticidad", "precio_unitario", "stock_minimo"),
            on="Id_Producto",
            how="inner",  # RQ-03: garantiza existencia de producto
        )
        .join(
            df_tipo_raw.select(
                "Id_TipoMovimiento",
                F.col("Codigo").alias("tipo_movimiento"),
                F.col("Es_Entrada").alias("es_entrada"),
                F.col("Es_Salida_Real").alias("es_salida_real"),
            ),
            on="Id_TipoMovimiento",
            how="inner",
        )
        .join(
            df_proy_raw.select(
                "Id_Proyecto",
                F.col("Codigo_Proyecto").alias("codigo_proyecto"),
                F.col("Nombre_Proyecto").alias("nombre_proyecto"),
            ),
            on="Id_Proyecto",
            how="left",
        )
        .na.fill({"codigo_proyecto": "SIN_PROYECTO", "nombre_proyecto": "Operación general de almacén"})
        # Clasificación analítica de velocidad de despacho
        .withColumn(
            "tramo_permanencia",
            F.when(F.col("es_entrada") == 1, F.lit("INGRESO_LOTE"))
            .when(F.col("dias_permanencia") <= 3, F.lit("RAPIDO_0_3D"))
            .when(F.col("dias_permanencia") <= 7, F.lit("NORMAL_4_7D"))
            .when(F.col("dias_permanencia") <= 10, F.lit("MODERADO_8_10D"))
            .otherwise(F.lit("RETENIDO_MAS_10D")),
        )
    )

    # 3.3 Limpieza y tipado de sensores IoT ambientales (25,824 lecturas horarias)
    df_sensores_silver = (
        df_sensores_raw
        .na.drop(subset=["timestamp", "sensor_id", "codigo_ubicacion", "temperatura_c", "humedad_pct"])
        .dropDuplicates(["timestamp", "sensor_id"])
        .withColumn("ts_lectura", F.to_timestamp("timestamp"))
        .withColumn("fecha", F.to_date("ts_lectura"))
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
        .withColumn("hora", F.hour("ts_lectura"))
        .withColumn(
            "fuera_rango_temp",
            F.when((F.col("temperatura_c") < 18.0) | (F.col("temperatura_c") > 27.0), 1).otherwise(0),
        )
        .withColumn(
            "fuera_rango_hum",
            F.when((F.col("humedad_pct") < 40.0) | (F.col("humedad_pct") > 60.0), 1).otherwise(0),
        )
        .withColumn(
            "es_evento_ambiental",
            F.when((F.col("fuera_rango_temp") == 1) | (F.col("fuera_rango_hum") == 1), 1).otherwise(0),
        )
    )

    # 3.4 Limpieza y validación de conteos cíclicos y plan mensual
    df_conteos_silver = (
        df_conteos_raw
        .na.drop(subset=["fecha", "cod_componente", "stock_sistema", "stock_fisico"])
        .withColumnRenamed("cod_componente", "codigo_componente")
        .withColumn("fecha_conteo", F.to_date("fecha"))
        .withColumn("mes", F.month("fecha_conteo"))
        .withColumn("diferencia_calc", F.col("stock_fisico") - F.col("stock_sistema"))
        .withColumn(
            "denominador",
            F.greatest(F.col("stock_sistema"), F.col("stock_fisico"), F.lit(1)),
        )
        .withColumn(
            "exactitud_linea",
            F.round(1.0 - (F.abs(F.col("diferencia_calc")) / F.col("denominador")), 4),
        )
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

    # Escritura Capa Silver particionada
    silver_path = LAKEHOUSE_DIR / "silver"
    df_prod_silver.write.mode("overwrite").parquet(str(silver_path / "silver_productos"))
    df_mov_silver.write.mode("overwrite").partitionBy("mes").parquet(str(silver_path / "silver_movimientos"))
    df_sensores_silver.write.mode("overwrite").partitionBy("mes").parquet(str(silver_path / "silver_sensores"))
    df_conteos_silver.write.mode("overwrite").parquet(str(silver_path / "silver_conteos"))
    df_plan_silver.write.mode("overwrite").parquet(str(silver_path / "silver_plan_demanda"))

    t_silver = round(time.perf_counter() - t2, 3)
    print(f"[SILVER] Completado en {t_silver} s -> Movimientos válidos: {df_mov_silver.count()}")

    # =========================================================================
    # 4. CAPA GOLD: Tablas de negocio e indicadores listos para consumo y Genie
    # =========================================================================
    t3 = time.perf_counter()

    # 4.1 Gold KPI Mensual (cruce movimientos reales + plan mensual)
    plan_mes = (
        df_plan_silver.groupBy("anio", "mes")
        .agg(
            F.sum("entradas_plan").alias("entradas_plan"),
            F.sum("salidas_plan").alias("salidas_plan"),
        )
    )

    mov_mes = (
        df_mov_silver.groupBy("anio", "mes")
        .agg(
            F.sum(F.when(F.col("es_entrada") == 1, F.col("cantidad")).otherwise(0)).alias("unidades_entradas"),
            F.sum(F.when(F.col("es_salida_real") == 1, F.col("cantidad")).otherwise(0)).alias("unidades_salidas"),
            F.round(F.sum(F.when(F.col("es_entrada") == 1, F.col("costo_total")).otherwise(0)), 2).alias("costo_entradas"),
            F.round(F.sum(F.when(F.col("es_salida_real") == 1, F.col("costo_total")).otherwise(0)), 2).alias("costo_salidas"),
            F.round(F.avg("dias_permanencia"), 2).alias("tpp_dias"),
            F.count(F.when(F.col("dias_permanencia").isNotNull(), 1)).alias("num_salidas_fifo"),
        )
    )

    win_acum = Window.orderBy("anio", "mes").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_gold_mensual = (
        mov_mes.join(plan_mes, on=["anio", "mes"], how="left")
        .withColumn("rotacion_flujo", F.round(F.col("unidades_salidas") / F.col("unidades_entradas"), 4))
        .withColumn("balance_neto_mes", F.col("unidades_entradas") - F.col("unidades_salidas"))
        .withColumn("stock_acumulado", F.sum("balance_neto_mes").over(win_acum))
        .withColumn("inversion_retenida_mes", F.round(F.col("costo_entradas") - F.col("costo_salidas"), 2))
        .withColumn(
            "vm_entradas_pct",
            F.round((F.col("unidades_entradas") - F.col("entradas_plan")) / F.col("entradas_plan") * 100, 2),
        )
        .withColumn(
            "vm_salidas_pct",
            F.round((F.col("unidades_salidas") - F.col("salidas_plan")) / F.col("salidas_plan") * 100, 2),
        )
        .orderBy("anio", "mes")
    )

    # 4.2 Gold KPI por Categoría Tecnológica
    plan_cat = (
        df_plan_silver.groupBy("categoria")
        .agg(
            F.sum("entradas_plan").alias("entradas_plan"),
            F.sum("salidas_plan").alias("salidas_plan"),
        )
    )

    df_gold_categoria = (
        df_mov_silver.groupBy("categoria")
        .agg(
            F.countDistinct("codigo_componente").alias("num_componentes"),
            F.sum(F.when(F.col("es_entrada") == 1, F.col("cantidad")).otherwise(0)).alias("unidades_entradas"),
            F.sum(F.when(F.col("es_salida_real") == 1, F.col("cantidad")).otherwise(0)).alias("unidades_salidas"),
            F.round(F.sum(F.when(F.col("es_entrada") == 1, F.col("costo_total")).otherwise(0)), 2).alias("costo_entradas"),
            F.round(F.sum(F.when(F.col("es_salida_real") == 1, F.col("costo_total")).otherwise(0)), 2).alias("costo_salidas"),
            F.round(F.avg("dias_permanencia"), 2).alias("tpp_dias"),
        )
        .join(plan_cat, on="categoria", how="left")
        .withColumn("rotacion_flujo", F.round(F.col("unidades_salidas") / F.col("unidades_entradas"), 4))
        .withColumn("inversion_retenida", F.round(F.col("costo_entradas") - F.col("costo_salidas"), 2))
        .withColumn(
            "vm_entradas_pct",
            F.round((F.col("unidades_entradas") - F.col("entradas_plan")) / F.col("entradas_plan") * 100, 2),
        )
        .orderBy(F.desc("costo_entradas"))
    )

    # 4.3 Gold Stock y Criticidad por Componente + Conteos Cíclicos
    exac_comp = (
        df_conteos_silver.groupBy("codigo_componente")
        .agg(
            F.count("*").alias("num_conteos"),
            F.round(F.avg("exactitud_linea") * 100, 2).alias("exactitud_pct"),
        )
    )

    df_gold_componentes = (
        df_mov_silver.groupBy("codigo_componente", "categoria", "criticidad", "stock_minimo", "precio_unitario")
        .agg(
            F.sum(F.when(F.col("es_entrada") == 1, F.col("cantidad")).otherwise(0)).alias("entradas"),
            F.sum(F.when(F.col("es_salida_real") == 1, F.col("cantidad")).otherwise(0)).alias("salidas"),
            F.round(F.avg("dias_permanencia"), 2).alias("tpp_dias"),
        )
        .withColumn("stock_neto", F.col("entradas") - F.col("salidas"))
        .withColumn("valor_stock_neto", F.round(F.col("stock_neto") * F.col("precio_unitario"), 2))
        .withColumn("cumple_stock_minimo", F.when(F.col("stock_neto") >= F.col("stock_minimo"), 1).otherwise(0))
        .join(exac_comp, on="codigo_componente", how="left")
        .orderBy("codigo_componente")
    )

    # 4.4 Gold Trazabilidad por Proyecto
    df_gold_proyectos = (
        df_mov_silver.filter(F.col("es_salida_real") == 1)
        .groupBy("codigo_proyecto", "nombre_proyecto")
        .agg(
            F.count("*").alias("num_despachos"),
            F.sum("cantidad").alias("unidades_consumidas"),
            F.round(F.sum("costo_total"), 2).alias("costo_consumido"),
            F.round(F.avg("dias_permanencia"), 2).alias("tpp_despacho_dias"),
            F.countDistinct("codigo_componente").alias("componentes_distintos"),
        )
        .orderBy(F.desc("unidades_consumidas"))
    )

    # 4.5 Gold Telemetría Ambiental IoT por Ubicación y Mes
    df_gold_sensores = (
        df_sensores_silver.groupBy("codigo_ubicacion", "mes")
        .agg(
            F.count("*").alias("lecturas_totales"),
            F.round(F.avg("temperatura_c"), 2).alias("temp_promedio_c"),
            F.round(F.min("temperatura_c"), 2).alias("temp_min_c"),
            F.round(F.max("temperatura_c"), 2).alias("temp_max_c"),
            F.round(F.avg("humedad_pct"), 2).alias("hum_promedio_pct"),
            F.sum("es_evento_ambiental").alias("eventos_fuera_rango"),
        )
        .withColumn(
            "tasa_eventos_pct",
            F.round(F.col("eventos_fuera_rango") / F.col("lecturas_totales") * 100, 2),
        )
        .orderBy("codigo_ubicacion", "mes")
    )

    # Escritura Capa Gold
    gold_path = LAKEHOUSE_DIR / "gold"
    df_gold_mensual.write.mode("overwrite").parquet(str(gold_path / "gold_kpi_mensual"))
    df_gold_categoria.write.mode("overwrite").parquet(str(gold_path / "gold_kpi_categoria"))
    df_gold_componentes.write.mode("overwrite").parquet(str(gold_path / "gold_stock_componentes"))
    df_gold_proyectos.write.mode("overwrite").parquet(str(gold_path / "gold_consumo_proyectos"))
    df_gold_sensores.write.mode("overwrite").parquet(str(gold_path / "gold_telemetria_almacen"))

    # Registrar vistas SQL temporales (equivalente a Tablas administradas en Unity Catalog / Hive Metastore)
    df_gold_mensual.createOrReplaceTempView("gold_kpi_mensual")
    df_gold_categoria.createOrReplaceTempView("gold_kpi_categoria")
    df_gold_componentes.createOrReplaceTempView("gold_stock_componentes")
    df_gold_proyectos.createOrReplaceTempView("gold_consumo_proyectos")
    df_gold_sensores.createOrReplaceTempView("gold_telemetria_almacen")
    df_mov_silver.createOrReplaceTempView("silver_movimientos")
    df_conteos_silver.createOrReplaceTempView("silver_conteos")

    # Verificación global de KPI en Spark SQL (debe conciliar 100% con BI)
    kpi_global_row = spark.sql("""
        SELECT
            COUNT(*) AS total_movimientos,
            SUM(CASE WHEN es_entrada = 1 THEN cantidad ELSE 0 END) AS unidades_entradas,
            SUM(CASE WHEN es_salida_real = 1 THEN cantidad ELSE 0 END) AS unidades_salidas,
            ROUND(SUM(CASE WHEN es_entrada = 1 THEN costo_total ELSE 0 END), 2) AS costo_entradas,
            ROUND(SUM(CASE WHEN es_salida_real = 1 THEN costo_total ELSE 0 END), 2) AS costo_salidas,
            ROUND(AVG(dias_permanencia), 6) AS tpp_exacto,
            ROUND(AVG(dias_permanencia), 2) AS tpp_redondeado,
            ROUND(
                SUM(CASE WHEN es_salida_real = 1 THEN cantidad ELSE 0 END) * 1.0 /
                SUM(CASE WHEN es_entrada = 1 THEN cantidad ELSE 0 END), 4
            ) AS rotacion_global
        FROM silver_movimientos
    """).collect()[0].asDict()

    exactitud_global = spark.sql(
        "SELECT ROUND(AVG(exactitud_linea) * 100, 2) AS exa FROM silver_conteos"
    ).collect()[0]["exa"]

    comp_sobre_min = spark.sql(
        "SELECT SUM(cumple_stock_minimo) AS ok, COUNT(*) AS tot FROM gold_stock_componentes"
    ).collect()[0].asDict()

    tramos_permanencia = [
        r.asDict()
        for r in spark.sql("""
            SELECT tramo_permanencia, COUNT(*) AS movimientos, SUM(cantidad) AS unidades
            FROM silver_movimientos
            GROUP BY tramo_permanencia
            ORDER BY movimientos DESC
        """).collect()
    ]

    t_gold = round(time.perf_counter() - t3, 3)
    t_total = round(time.perf_counter() - t_inicio_global, 3)

    # Exportar tablas Gold a CSV para evidencias en el repositorio
    for nombre, df_g in [
        ("gold_kpi_mensual.csv", df_gold_mensual),
        ("gold_kpi_categoria.csv", df_gold_categoria),
        ("gold_stock_componentes.csv", df_gold_componentes),
        ("gold_consumo_proyectos.csv", df_gold_proyectos),
        ("gold_telemetria_almacen.csv", df_gold_sensores),
    ]:
        pdf = df_g.toPandas()
        pdf.to_csv(SALIDAS_DIR / nombre, index=False, encoding="utf-8-sig")

    resumen = {
        "motor": f"Apache Spark {spark.version} (PySpark / Arquitectura Medallion)",
        "fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tiempos_s": {
            "bronze_ingesta": t_bronze,
            "eda_distribuido": t_eda,
            "silver_limpieza": t_silver,
            "gold_agregacion": t_gold,
            "total_pipeline": t_total,
        },
        "conteos_bronze": conteos_bronze,
        "conteos_silver": {
            "silver_productos": df_prod_silver.count(),
            "silver_movimientos": df_mov_silver.count(),
            "silver_sensores": df_sensores_silver.count(),
            "silver_conteos": df_conteos_silver.count(),
            "silver_plan_demanda": df_plan_silver.count(),
        },
        "conteos_gold": {
            "gold_kpi_mensual": df_gold_mensual.count(),
            "gold_kpi_categoria": df_gold_categoria.count(),
            "gold_stock_componentes": df_gold_componentes.count(),
            "gold_consumo_proyectos": df_gold_proyectos.count(),
            "gold_telemetria_almacen": df_gold_sensores.count(),
        },
        "eda": {
            "describe_movimientos": desc_mov,
            "describe_sensores": desc_sen,
            "nulos_movimientos": nulos_mov,
            "distribucion_tipos": dist_tipos,
            "tramos_permanencia": tramos_permanencia,
        },
        "kpis_gold_verificados": {
            **kpi_global_row,
            "inversion_retenida": round(kpi_global_row["costo_entradas"] - kpi_global_row["costo_salidas"], 2),
            "stock_neto": int(kpi_global_row["unidades_entradas"] - kpi_global_row["unidades_salidas"]),
            "exactitud_conteos_pct": float(exactitud_global),
            "componentes_sobre_minimo": f"{comp_sobre_min['ok']}/{comp_sobre_min['tot']}",
            "conciliacion_bi_bigdata_ok": (
                kpi_global_row["total_movimientos"] == 5348
                and kpi_global_row["unidades_entradas"] == 15425
                and kpi_global_row["unidades_salidas"] == 15078
                and abs(kpi_global_row["tpp_exacto"] - 3.868701) < 1e-5
            ),
        },
        "gold_mensual": [r.asDict() for r in df_gold_mensual.collect()],
        "gold_categoria": [r.asDict() for r in df_gold_categoria.collect()],
        "gold_proyectos": [r.asDict() for r in df_gold_proyectos.collect()],
    }

    out_json = SALIDAS_DIR / "medallion_resultados.json"
    out_json.write_text(json.dumps(resumen, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"[GOLD] Completado en {t_gold} s | Conciliación BI vs Big Data = {resumen['kpis_gold_verificados']['conciliacion_bi_bigdata_ok']}")
    print(f"Resumen guardado en: {out_json}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
