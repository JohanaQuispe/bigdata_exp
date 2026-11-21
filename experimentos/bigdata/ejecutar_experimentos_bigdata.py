#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejecutar_experimentos_bigdata.py — Paquete de 5 Experimentos Big Data en Apache Spark / PySpark
Evaluación de la Arquitectura Medallion y Motor Distribuido de Unity Perú SAC (1 por integrante).

Integrantes y experimentos asignados:
  - EXP-BD-01 (Quispe Ruiz, Johana Carolina): Formatos de persistencia en la Arquitectura Medallion
    (CSV texto plano vs. JSON semiestructurado vs. Parquet columnar Snappy con Predicate Pushdown).
  - EXP-BD-02 (Sotelo Espinoza, Leoneld Mathias): Estrategias de transformación en Capa Silver
    (Python UDF fila a fila vs. Spark SQL declarativo vs. API Nativa DataFrames Catalyst + Columnas).
  - EXP-BD-03 (Montalvo Abrego, Camila Nadine): Persistencia en memoria (cache / MEMORY_AND_DISK)
    y Partition Pruning por `mes` para acelerar consultas concurrentes de Dashboards y Genie en Capa Gold.
  - EXP-BD-04 (Arroyo Julca, Mishell Alexandra Belen): Escalamiento masivo de volumetría en PySpark
    (x1 = 31,172; x10 = 311,720; x50 = 1,558,600; x100 = 3,117,200 registros integrados Movimientos + IoT).
  - EXP-BD-05 (Cordova Paz, Diego Jason): Configuración de particiones de Shuffle (`spark.sql.shuffle.partitions`:
    2, 8, 64 y 200) y Broadcast Hash Join vs. SortMergeJoin en la consolidación de la Capa Gold.
"""

import json
import os
import shutil
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import jdk4py
    if not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = str(jdk4py.JAVA_HOME)
except ImportError:
    pass

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

RAIZ = Path(__file__).resolve().parents[2]
DATOS_DIR = RAIZ / "datos"
RESPALDO_DIR = RAIZ / "powerbi" / "respaldo"
SALIDAS = RAIZ / "experimentos" / "salidas"
TMP_EXP = Path("/tmp/unity_spark_experimentos")
SALIDAS.mkdir(parents=True, exist_ok=True)


def tamano_directorio_kb(ruta: Path) -> float:
    total = sum(f.stat().st_size for f in ruta.rglob("*") if f.is_file() and not f.name.startswith("."))
    return round(total / 1024.0, 2)


def medir_spark(fn, n_rep=5, warmup=1):
    for _ in range(warmup):
        fn()
    tiempos = []
    res = None
    for _ in range(n_rep):
        t0 = time.perf_counter()
        res = fn()
        tiempos.append((time.perf_counter() - t0) * 1000.0)
    return {
        "repeticiones": n_rep,
        "mediana_ms": round(statistics.median(tiempos), 2),
        "promedio_ms": round(statistics.mean(tiempos), 2),
        "std_ms": round(statistics.stdev(tiempos), 2) if len(tiempos) > 1 else 0.0,
        "min_ms": round(min(tiempos), 2),
        "max_ms": round(max(tiempos), 2),
        "resultado_verificado": res,
    }


def cargar_base_silver(spark: SparkSession):
    df_mov = (
        spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Fact_Movimiento.csv"))
    )
    df_prod = (
        spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Dim_Producto.csv"))
    )
    df_tipo = (
        spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Dim_TipoMovimiento.csv"))
    )
    df_sen = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .csv(str(DATOS_DIR / "Sensores_Almacen_2026.txt"))
    )
    df_silver = (
        df_mov.na.drop(subset=["Id_Movimiento_NK", "Id_Producto", "Id_TipoMovimiento", "Cantidad"])
        .filter(F.col("Cantidad") > 0)
        .withColumn("fecha_mov", F.to_date(F.col("Fecha").cast("string")))
        .withColumn("mes", F.month("fecha_mov"))
        .join(df_prod.select("Id_Producto", "Cod_Componente", "Categoria", "Criticidad", "Precio_Unitario"), "Id_Producto")
        .join(df_tipo.select("Id_TipoMovimiento", "Codigo", "Es_Entrada", "Es_Salida_Real"), "Id_TipoMovimiento")
    )
    return df_silver, df_prod, df_tipo, df_sen


# =============================================================================
# EXP-BD-01 — Johana Carolina Quispe Ruiz
# Comparación de formatos de almacenamiento en Lakehouse: CSV vs JSON vs Parquet (Snappy)
# =============================================================================
def exp_bd_01_johana(spark: SparkSession, df_silver):
    print(">>> Ejecutando EXP-BD-01 (Quispe Ruiz, Johana): CSV vs JSON vs Parquet Snappy en Lakehouse...")
    p_dir = TMP_EXP / "exp_bd_01"
    if p_dir.exists():
        shutil.rmtree(p_dir)

    # Escalar dataset x20 (106,960 filas) para medir tamaño en disco, escritura y lectura selectiva
    df_x20 = df_silver.withColumn("_rep", F.explode(F.array([F.lit(i) for i in range(20)]))).cache()
    total_filas = df_x20.count()

    escenarios = []
    for fmt, opts in [
        ("CSV (Texto plano sin compresión)", {"header": "true"}),
        ("JSON (Semiestructurado por líneas)", {}),
        ("Parquet + Snappy (Columnar Medallion)", {"compression": "snappy"}),
    ]:
        key = fmt.split()[0].lower()
        out_path = p_dir / key

        t_w0 = time.perf_counter()
        writer = df_x20.write.mode("overwrite")
        for k, v in opts.items():
            writer = writer.option(k, v)
        getattr(writer, key)(str(out_path))
        t_write_ms = round((time.perf_counter() - t_w0) * 1000.0, 2)

        size_kb = tamano_directorio_kb(out_path)

        def leer_y_agregar():
            reader = spark.read
            if key == "csv":
                reader = reader.option("header", "true").option("inferSchema", "true")
            df_r = getattr(reader, key)(str(out_path))
            row = (
                df_r.filter((F.col("Categoria") == "Energía") & (F.col("Es_Salida_Real") == 1))
                .agg(F.round(F.avg("Dias_Permanencia"), 4).alias("tpp"))
                .collect()[0]
            )
            return float(row["tpp"])

        m_read = medir_spark(leer_y_agregar, n_rep=5, warmup=1)
        escenarios.append({
            "escenario": fmt,
            "filas": total_filas,
            "tamano_disco_kb": size_kb,
            "escritura_ms": t_write_ms,
            "lectura_mediana_ms": m_read["mediana_ms"],
            "lectura_min_ms": m_read["min_ms"],
            "lectura_max_ms": m_read["max_ms"],
            "std_ms": m_read["std_ms"],
            "tpp_energia_verificado": m_read["resultado_verificado"],
        })

    base_size = escenarios[0]["tamano_disco_kb"]
    base_read = escenarios[0]["lectura_mediana_ms"]
    for e in escenarios:
        e["reduccion_espacio_pct"] = round((1.0 - e["tamano_disco_kb"] / base_size) * 100.0, 1)
        e["speedup_lectura_vs_csv"] = round(base_read / e["lectura_mediana_ms"], 2)

    df_x20.unpersist()
    return {
        "id": "EXP-BD-01",
        "integrante": "Quispe Ruiz, Johana Carolina",
        "titulo": "Evaluación de formatos de persistencia en la Arquitectura Medallion: CSV vs. JSON vs. Parquet Columnar (Snappy)",
        "dataset_filas": total_filas,
        "escenarios": escenarios,
    }


# =============================================================================
# EXP-BD-02 — Leoneld Mathias Sotelo Espinoza
# Estrategias de transformación en Capa Silver: Python UDF vs Spark SQL vs API Nativa Catalyst
# =============================================================================
def exp_bd_02_leoneld(spark: SparkSession, df_silver):
    print(">>> Ejecutando EXP-BD-02 (Sotelo Espinoza, Leoneld): Python UDF vs Spark SQL vs API Nativa DataFrames...")
    df_x30 = df_silver.withColumn("_rep", F.explode(F.array([F.lit(i) for i in range(30)]))).cache()
    total_filas = df_x30.count()
    df_x30.createOrReplaceTempView("vista_movs_x30")

    # E1: Python UDF (serialización JVM <-> Python Pickling por fila)
    @F.udf(returnType=StringType())
    def clasificar_udf(es_ent, dias, crit, costo):
        if es_ent == 1:
            return "INGRESO"
        d = float(dias) if dias is not None else 0.0
        c = float(costo) if costo is not None else 0.0
        if d > 7 and crit == "ALTA" and c > 10000:
            return "PRIORIDAD_CRITICA_FIFO"
        elif d > 4:
            return "DESPACHO_ESTANDAR"
        return "FLUJO_RAPIDO"

    def run_udf():
        res = (
            df_x30.withColumn("clasif", clasificar_udf("Es_Entrada", "Dias_Permanencia", "Criticidad", "Costo_Total"))
            .groupBy("clasif").count().collect()
        )
        return sum(r["count"] for r in res)

    # E2: Expresión declarativa en Spark SQL (CASE WHEN optimizado por Catalyst)
    def run_spark_sql():
        res = spark.sql("""
            SELECT
                CASE
                    WHEN Es_Entrada = 1 THEN 'INGRESO'
                    WHEN Dias_Permanencia > 7 AND Criticidad = 'ALTA' AND Costo_Total > 10000 THEN 'PRIORIDAD_CRITICA_FIFO'
                    WHEN Dias_Permanencia > 4 THEN 'DESPACHO_ESTANDAR'
                    ELSE 'FLUJO_RAPIDO'
                END AS clasif,
                COUNT(*) AS cnt
            FROM vista_movs_x30
            GROUP BY 1
        """).collect()
        return sum(r["cnt"] for r in res)

    # E3: API Nativa de Columnas PySpark (F.when + Whole-Stage CodeGen JVM)
    def run_native_expr():
        clasif_col = (
            F.when(F.col("Es_Entrada") == 1, F.lit("INGRESO"))
            .when((F.col("Dias_Permanencia") > 7) & (F.col("Criticidad") == "ALTA") & (F.col("Costo_Total") > 10000), F.lit("PRIORIDAD_CRITICA_FIFO"))
            .when(F.col("Dias_Permanencia") > 4, F.lit("DESPACHO_ESTANDAR"))
            .otherwise(F.lit("FLUJO_RAPIDO"))
        )
        res = df_x30.withColumn("clasif", clasif_col).groupBy("clasif").count().collect()
        return sum(r["count"] for r in res)

    m1 = medir_spark(run_udf, n_rep=5, warmup=1)
    m2 = medir_spark(run_spark_sql, n_rep=5, warmup=1)
    m3 = medir_spark(run_native_expr, n_rep=5, warmup=1)

    df_x30.unpersist()
    return {
        "id": "EXP-BD-02",
        "integrante": "Sotelo Espinoza, Leoneld Mathias",
        "titulo": "Optimización de transformaciones de limpieza y reglas de negocio en Capa Silver: Python UDF vs. Spark SQL vs. Expresiones Nativas Catalyst",
        "dataset_filas": total_filas,
        "escenarios": [
            {
                "escenario": "E1: Python UDF fila a fila (Serialización Py4J / Pickling)",
                **m1,
                "throughput_filas_s": round(total_filas / (m1["mediana_ms"] / 1000.0), 0),
                "speedup_vs_udf": 1.0,
            },
            {
                "escenario": "E2: Consulta declarativa Spark SQL (CASE WHEN + Catalyst)",
                **m2,
                "throughput_filas_s": round(total_filas / (m2["mediana_ms"] / 1000.0), 0),
                "speedup_vs_udf": round(m1["mediana_ms"] / m2["mediana_ms"], 2),
            },
            {
                "escenario": "E3: API Nativa DataFrames (F.when vectorizado en JVM WholeStageCodegen)",
                **m3,
                "throughput_filas_s": round(total_filas / (m3["mediana_ms"] / 1000.0), 0),
                "speedup_vs_udf": round(m1["mediana_ms"] / m3["mediana_ms"], 2),
            },
        ],
    }


# =============================================================================
# EXP-BD-03 — Camila Nadine Montalvo Abrego
# Partition Pruning por Mes + Caché en Memoria para Consultas Iterativas de Dashboards Databricks / Genie
# =============================================================================
def exp_bd_03_camila(spark: SparkSession, df_silver):
    print(">>> Ejecutando EXP-BD-03 (Montalvo Abrego, Camila): Sin Partición vs Partition Pruning vs Cache en Memoria...")
    p_dir = TMP_EXP / "exp_bd_03"
    if p_dir.exists():
        shutil.rmtree(p_dir)

    df_x80 = df_silver.withColumn("_rep", F.explode(F.sequence(F.lit(1), F.lit(80)))).coalesce(4)
    path_flat = p_dir / "csv_sin_particion"
    path_part = p_dir / "parquet_particionado_mes"

    df_x80.write.mode("overwrite").option("header", "true").csv(str(path_flat))
    df_x80.repartition("mes").write.mode("overwrite").partitionBy("mes").parquet(str(path_part))
    total_filas = spark.read.parquet(str(path_part)).count()

    # Simula una sesión interactiva de Dashboard / Genie: 4 consultas analíticas sobre Setiembre (mes=9)
    def ejecutar_4_queries_dashboard(df_src):
        q1 = df_src.filter(F.col("mes") == 9).groupBy("Categoria").agg(F.sum("Costo_Total")).collect()
        q2 = df_src.filter(F.col("mes") == 9).groupBy("Cod_Componente").agg(F.avg("Dias_Permanencia")).collect()
        q3 = df_src.filter((F.col("mes") == 9) & (F.col("Criticidad") == "ALTA")).count()
        q4 = df_src.filter(F.col("mes") == 9).agg(F.sum("Cantidad")).collect()
        return len(q1) + len(q2) + (1 if q3 > 0 else 0) + len(q4)

    # E1: Lectura directa sobre capa cruda sin particionar y sin caché (Full Scan × 4 visuales)
    def run_flat():
        df_f = spark.read.option("header", "true").option("inferSchema", "true").csv(str(path_flat))
        return ejecutar_4_queries_dashboard(df_f)

    # E2: Parquet particionado por `mes` (Partition Pruning lee solo la partición mes=9)
    def run_partitioned():
        df_p = spark.read.parquet(str(path_part))
        return ejecutar_4_queries_dashboard(df_p)

    # E3: Parquet particionado + persistencia MEMORY_AND_DISK pre-calentada para el dashboard interactivo
    df_cached = spark.read.parquet(str(path_part)).filter(F.col("mes") == 9).persist(StorageLevel.MEMORY_AND_DISK)
    df_cached.count()  # Materializar caché

    def run_cached():
        return ejecutar_4_queries_dashboard(df_cached)

    m1 = medir_spark(run_flat, n_rep=5, warmup=1)
    m2 = medir_spark(run_partitioned, n_rep=5, warmup=1)
    m3 = medir_spark(run_cached, n_rep=5, warmup=1)

    df_cached.unpersist()
    return {
        "id": "EXP-BD-03",
        "integrante": "Montalvo Abrego, Camila Nadine",
        "titulo": "Aceleración de consultas interactivas para Dashboards en Databricks y Genie: Parquet Plano vs. Partition Pruning por Mes vs. Caché en Memoria",
        "dataset_filas": total_filas,
        "consultas_por_rafaga": 4,
        "escenarios": [
            {
                "escenario": "E1: Capa cruda CSV sin particionar ni caché (Full Scan × 4 visuales)",
                **m1,
                "speedup_vs_e1": 1.0,
            },
            {
                "escenario": "E2: Parquet particionado por `mes` (Partition Pruning mes=9)",
                **m2,
                "speedup_vs_e1": round(m1["mediana_ms"] / m2["mediana_ms"], 2),
            },
            {
                "escenario": "E3: Partition Pruning + Persistencia MEMORY_AND_DISK (Tabla cacheada)",
                **m3,
                "speedup_vs_e1": round(m1["mediana_ms"] / m3["mediana_ms"], 2),
            },
        ],
    }


# =============================================================================
# EXP-BD-04 — Mishell Alexandra Belen Arroyo Julca
# Escalamiento Masivo de Volumetría en Apache Spark (x1, x10, x50, x100 -> hasta >3.1 millones de registros)
# =============================================================================
def exp_bd_04_mishell(spark: SparkSession, df_silver, df_sen):
    print(">>> Ejecutando EXP-BD-04 (Arroyo Julca, Mishell): Escalamiento Big Data x1, x10, x50 y x100 (>3.1M filas)...")
    # Prepara base combinada de eventos del almacén (5,348 movimientos + 25,824 sensores = 31,172 registros base)
    df_mov_base = df_silver.cache()
    df_sen_base = (
        df_sen.withColumn("ts", F.to_timestamp("timestamp"))
        .withColumn("mes", F.month("ts"))
        .withColumn(
            "fuera_rango",
            F.when(
                (F.col("temperatura_c") < 18.0) | (F.col("temperatura_c") > 27.0) |
                (F.col("humedad_pct") < 40.0) | (F.col("humedad_pct") > 60.0),
                1,
            ).otherwise(0),
        )
        .cache()
    )
    n_mov_0 = df_mov_base.count()
    n_sen_0 = df_sen_base.count()

    escenarios = []
    for escala in (1, 10, 50, 100):
        def pipeline_escala():
            df_m = df_mov_base.withColumn("_r", F.explode(F.sequence(F.lit(1), F.lit(escala))))
            df_s = df_sen_base.withColumn("_r", F.explode(F.sequence(F.lit(1), F.lit(escala))))

            kpi_m = (
                df_m.groupBy("mes", "Categoria")
                .agg(
                    F.sum(F.when(F.col("Es_Entrada") == 1, F.col("Cantidad")).otherwise(0)).alias("ent"),
                    F.sum(F.when(F.col("Es_Salida_Real") == 1, F.col("Cantidad")).otherwise(0)).alias("sal"),
                    F.avg("Dias_Permanencia").alias("tpp"),
                )
            )
            kpi_s = (
                df_s.groupBy("mes")
                .agg(F.count("*").alias("lecturas"), F.avg("temperatura_c").alias("temp_media"))
            )
            res = (
                kpi_m.join(kpi_s, on="mes", how="inner")
                .agg(
                    F.sum("ent").alias("tot_ent"),
                    F.sum("sal").alias("tot_sal"),
                    F.round(F.avg("tpp"), 4).alias("tpp_medio_celdas"),
                )
                .collect()[0]
            )
            return round(float(res["tot_sal"]) / float(res["tot_ent"]), 4)

        med = medir_spark(pipeline_escala, n_rep=4, warmup=1)
        tot_registros = (n_mov_0 + n_sen_0) * escala
        throughput = round(tot_registros / (med["mediana_ms"] / 1000.0), 0)
        escenarios.append({
            "escenario": f"Escala x{escala} ({tot_registros:,} registros totales)",
            "escala": escala,
            "movimientos_procesados": n_mov_0 * escala,
            "lecturas_iot_procesadas": n_sen_0 * escala,
            "registros_totales": tot_registros,
            "mediana_ms": med["mediana_ms"],
            "min_ms": med["min_ms"],
            "max_ms": med["max_ms"],
            "std_ms": med["std_ms"],
            "throughput_reg_s": throughput,
            "rotacion_global_verificada": med["resultado_verificado"],
        })

    df_mov_base.unpersist()
    df_sen_base.unpersist()
    return {
        "id": "EXP-BD-04",
        "integrante": "Arroyo Julca, Mishell Alexandra Belen",
        "titulo": "Evaluación de escalabilidad masiva del pipeline PySpark ante crecimiento volumétrico (x1 = 31 mil hasta x100 = 3.11 millones de registros)",
        "escenarios": escenarios,
    }


# =============================================================================
# EXP-BD-05 — Diego Jason Cordova Paz
# Ajuste de Arquitectura Spark: Shuffle Partitions (2, 8, 64, 200) y Broadcast Join vs SortMergeJoin
# =============================================================================
def exp_bd_05_diego(spark: SparkSession, df_prod, df_tipo):
    print(">>> Ejecutando EXP-BD-05 (Cordova Paz, Diego): Shuffle Partitions y BroadcastHashJoin vs SortMergeJoin...")
    df_mov_raw = (
        spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
        .csv(str(RESPALDO_DIR / "Fact_Movimiento.csv"))
    )
    df_hechos_x40 = df_mov_raw.withColumn("_r", F.explode(F.sequence(F.lit(1), F.lit(40)))).cache()
    df_dim_p = df_prod.cache()
    df_dim_tm = df_tipo.cache()
    total_filas = df_hechos_x40.count()

    configs = [
        ("E1: SortMergeJoin + 200 shuffle partitions (Default Spark sin optimizar)", "false", "200", False),
        ("E2: SortMergeJoin + 64 shuffle partitions (Sobrecarga media de tareas)", "false", "64", False),
        ("E3: SortMergeJoin + 4 shuffle partitions (Alineado a cores del nodo)", "false", "4", False),
        ("E4: BroadcastHashJoin explícito + AQE activo + 4 particiones (Óptimo)", "true", "4", True),
    ]

    escenarios = []
    for nombre, aqe_flag, n_part, usar_broadcast in configs:
        spark.conf.set("spark.sql.adaptive.enabled", aqe_flag)
        spark.conf.set("spark.sql.shuffle.partitions", n_part)
        if not usar_broadcast:
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        else:
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

        def ejecutar_join_gold():
            dp = F.broadcast(df_dim_p) if usar_broadcast else df_dim_p
            dtm = F.broadcast(df_dim_tm) if usar_broadcast else df_dim_tm
            res = (
                df_hechos_x40
                .join(dp, on="Id_Producto", how="inner")
                .join(dtm, on="Id_TipoMovimiento", how="inner")
                .groupBy("Categoria", "Criticidad", "Codigo")
                .agg(
                    F.sum("Cantidad").alias("unidades"),
                    F.round(F.sum("Costo_Total"), 2).alias("costo"),
                    F.round(F.avg("Dias_Permanencia"), 4).alias("tpp"),
                )
                .collect()
            )
            return len(res)

        med = medir_spark(ejecutar_join_gold, n_rep=5, warmup=1)
        escenarios.append({
            "escenario": nombre,
            "shuffle_partitions": int(n_part),
            "aqe_habilitado": aqe_flag == "true",
            "estrategia_join": "BroadcastHashJoin" if usar_broadcast else "SortMergeJoin",
            **med,
            "throughput_filas_s": round(total_filas / (med["mediana_ms"] / 1000.0), 0),
        })

    base_ms = escenarios[0]["mediana_ms"]
    for e in escenarios:
        e["speedup_vs_default"] = round(base_ms / e["mediana_ms"], 2)

    # Restaurar configuración recomendada
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

    df_hechos_x40.unpersist()
    df_dim_p.unpersist()
    df_dim_tm.unpersist()

    return {
        "id": "EXP-BD-05",
        "integrante": "Cordova Paz, Diego Jason",
        "titulo": "Optimización de arquitectura de ejecución en Spark: Particiones de Shuffle y BroadcastHashJoin vs. SortMergeJoin en Capa Gold",
        "dataset_filas": total_filas,
        "escenarios": escenarios,
    }


def main():
    t0 = time.perf_counter()
    spark = (
        SparkSession.builder.appName("UnityPeru_Experimentos_BigData")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df_silver, df_prod, df_tipo, df_sen = cargar_base_silver(spark)

    resultados = {
        "suite": "Experimentos de Big Data (Apache Spark / Databricks Medallion) — Unity Perú SAC",
        "motor": f"Apache Spark {spark.version}",
        "fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "python_version": sys.version.split()[0],
        "experimentos": [
            exp_bd_01_johana(spark, df_silver),
            exp_bd_02_leoneld(spark, df_silver),
            exp_bd_03_camila(spark, df_silver),
            exp_bd_04_mishell(spark, df_silver, df_sen),
            exp_bd_05_diego(spark, df_prod, df_tipo),
        ],
        "tiempo_total_suite_s": round(time.perf_counter() - t0, 3),
    }

    out = SALIDAS / "resultados_experimentos_bigdata.json"
    out.write_text(json.dumps(resultados, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n=== Suite Big Data finalizada en {resultados['tiempo_total_suite_s']} s -> {out} ===")

    if TMP_EXP.exists():
        shutil.rmtree(TMP_EXP)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
