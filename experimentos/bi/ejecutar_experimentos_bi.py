#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejecutar_experimentos_bi.py — Paquete de 5 Experimentos BI (1 por integrante)
Evaluación del Datamart en Estrella, ETL Python y Modelo Analítico de Unity Perú SAC.

Integrantes y experimentos asignados:
  - EXP-BI-01 (Quispe Ruiz, Johana Carolina): Rendimiento de consultas analíticas
    Modelo OLTP Normalizado (3FN, múltiples JOINs) vs. Modelo Estrella (Kimball) vs. Estrella Indexada.
  - EXP-BI-02 (Sotelo Espinoza, Leoneld Mathias): Robustez y tolerancia a defectos de calidad en el ETL
    (0%, 2% [100 defectos] y 10% [535 defectos] inyectados: nulos, cantidades <= 0, FK huérfanas, duplicados).
  - EXP-BI-03 (Montalvo Abrego, Camila Nadine): Evaluación de estrategias de cálculo de medidas analíticas
    (Iteradores fila a fila tipo FILTER/SUMX vs. Contexto CALCULATE sobre Estrella vs. Vistas Pre-agregadas).
  - EXP-BI-04 (Arroyo Julca, Mishell Alexandra Belen): Escalamiento progresivo de datos en la transformación
    FIFO del ETL (x1 = 5,348; x5 = 26,740; x10 = 53,480; x25 = 133,700 movimientos).
  - EXP-BI-05 (Cordova Paz, Diego Jason): Optimización de estrategias de escritura transaccional en BD
    (Inserción unitaria fila por fila vs. Lotes estándar chunk=500 vs. Carga masiva vectorizada en 1 transacción).
"""

import csv
import json
import sqlite3
import statistics
import sys
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[2]
RESPALDO = RAIZ / "powerbi" / "respaldo"
SALIDAS = RAIZ / "experimentos" / "salidas"
SALIDAS.mkdir(parents=True, exist_ok=True)


def medir_repeticiones(fn, n_rep=7, warmup=1):
    for _ in range(warmup):
        fn()
    tiempos = []
    res = None
    for _ in range(n_rep):
        t0 = time.perf_counter()
        res = fn()
        tiempos.append((time.perf_counter() - t0) * 1000.0)  # ms
    return {
        "repeticiones": n_rep,
        "mediana_ms": round(statistics.median(tiempos), 3),
        "promedio_ms": round(statistics.mean(tiempos), 3),
        "std_ms": round(statistics.stdev(tiempos), 3) if len(tiempos) > 1 else 0.0,
        "min_ms": round(min(tiempos), 3),
        "max_ms": round(max(tiempos), 3),
        "resultado_muestra": res,
    }


# =============================================================================
# EXP-BI-01 — Johana Carolina Quispe Ruiz
# OLTP Normalizado (6 tablas JOIN) vs. Modelo Estrella sin índices vs. Estrella Indexada
# =============================================================================
def exp_bi_01_johana():
    print(">>> Ejecutando EXP-BI-01 (Quispe Ruiz, Johana): OLTP Normalizado vs Estrella vs Estrella Indexada...")
    fact = pd.read_csv(RESPALDO / "Fact_Movimiento.csv", sep=";", encoding="utf-8-sig")
    dim_p = pd.read_csv(RESPALDO / "Dim_Producto.csv", sep=";", encoding="utf-8-sig")
    dim_t = pd.read_csv(RESPALDO / "Dim_Tiempo.csv", sep=";", encoding="utf-8-sig")
    dim_tm = pd.read_csv(RESPALDO / "Dim_TipoMovimiento.csv", sep=";", encoding="utf-8-sig")
    dim_pry = pd.read_csv(RESPALDO / "Dim_Proyecto.csv", sep=";", encoding="utf-8-sig")

    # Escalar x5 (26,740 hechos) para observar con nitidez el impacto en planes de ejecución SQL
    fact_x5 = pd.concat([fact.assign(Id_Movimiento_NK=fact["Id_Movimiento_NK"] + i * 100000) for i in range(5)], ignore_index=True)

    # 1) Base OLTP normalizada (3FN): separa categoria, marca, modelo, criticidad, componente, movimiento
    conn_oltp = sqlite3.connect(":memory:")
    cats = pd.DataFrame({"Id_Cat": range(1, len(dim_p["Categoria"].unique()) + 1), "Categoria": sorted(dim_p["Categoria"].unique())})
    marcas = pd.DataFrame({"Id_Marca": range(1, len(dim_p["Marca"].unique()) + 1), "Marca": sorted(dim_p["Marca"].unique())})
    crits = pd.DataFrame({"Id_Crit": range(1, len(dim_p["Criticidad"].unique()) + 1), "Criticidad": sorted(dim_p["Criticidad"].unique())})
    modelos = dim_p[["Modelo", "Categoria", "Marca"]].drop_duplicates().merge(cats, on="Categoria").merge(marcas, on="Marca")
    modelos["Id_Modelo"] = range(1, len(modelos) + 1)
    comp_3fn = dim_p.merge(modelos[["Id_Modelo", "Modelo"]], on="Modelo").merge(crits, on="Criticidad")[
        ["Id_Producto", "Cod_Componente", "Id_Modelo", "Id_Crit", "Precio_Unitario", "Stock_Minimo"]
    ]
    mov_3fn = fact_x5[["Id_Movimiento_NK", "Fecha", "Id_Producto", "Id_TipoMovimiento", "Id_Proyecto", "Cantidad", "Dias_Permanencia"]]

    cats.to_sql("categoria_componente", conn_oltp, index=False)
    marcas.to_sql("marca", conn_oltp, index=False)
    crits.to_sql("nivel_criticidad", conn_oltp, index=False)
    modelos[["Id_Modelo", "Modelo", "Id_Cat", "Id_Marca"]].to_sql("modelo_componente", conn_oltp, index=False)
    comp_3fn.to_sql("componente", conn_oltp, index=False)
    dim_tm.to_sql("tipo_movimiento", conn_oltp, index=False)
    dim_pry.to_sql("proyecto_servicio", conn_oltp, index=False)
    mov_3fn.to_sql("movimiento_componente", conn_oltp, index=False)

    sql_oltp = """
        SELECT
            CAST(STRFTIME('%m', m.Fecha) AS INTEGER) AS Mes,
            cat.Categoria,
            cr.Criticidad,
            SUM(CASE WHEN tm.Es_Entrada = 1 THEN m.Cantidad ELSE 0 END) AS Entradas,
            SUM(CASE WHEN tm.Es_Salida_Real = 1 THEN m.Cantidad ELSE 0 END) AS Salidas,
            ROUND(SUM(CASE WHEN tm.Es_Entrada = 1 THEN m.Cantidad * c.Precio_Unitario ELSE 0 END), 2) AS Costo_Ent,
            ROUND(AVG(m.Dias_Permanencia), 4) AS TPP
        FROM movimiento_componente m
        JOIN componente c ON m.Id_Producto = c.Id_Producto
        JOIN modelo_componente mo ON c.Id_Modelo = mo.Id_Modelo
        JOIN categoria_componente cat ON mo.Id_Cat = cat.Id_Cat
        JOIN marca ma ON mo.Id_Marca = ma.Id_Marca
        JOIN nivel_criticidad cr ON c.Id_Crit = cr.Id_Crit
        JOIN tipo_movimiento tm ON m.Id_TipoMovimiento = tm.Id_TipoMovimiento
        LEFT JOIN proyecto_servicio pr ON m.Id_Proyecto = pr.Id_Proyecto
        GROUP BY Mes, cat.Categoria, cr.Criticidad
        ORDER BY Mes, cat.Categoria;
    """

    # 2) Modelo Estrella sin índices secundarios
    conn_star = sqlite3.connect(":memory:")
    fact_x5.to_sql("Fact_Movimiento", conn_star, index=False)
    dim_p.to_sql("Dim_Producto", conn_star, index=False)
    dim_t.to_sql("Dim_Tiempo", conn_star, index=False)
    dim_tm.to_sql("Dim_TipoMovimiento", conn_star, index=False)

    sql_star = """
        SELECT
            t.Mes,
            p.Categoria,
            p.Criticidad,
            SUM(CASE WHEN tm.Es_Entrada = 1 THEN f.Cantidad ELSE 0 END) AS Entradas,
            SUM(CASE WHEN tm.Es_Salida_Real = 1 THEN f.Cantidad ELSE 0 END) AS Salidas,
            ROUND(SUM(CASE WHEN tm.Es_Entrada = 1 THEN f.Costo_Total ELSE 0 END), 2) AS Costo_Ent,
            ROUND(AVG(f.Dias_Permanencia), 4) AS TPP
        FROM Fact_Movimiento f
        JOIN Dim_Tiempo t ON f.Id_Fecha = t.Id_Fecha
        JOIN Dim_Producto p ON f.Id_Producto = p.Id_Producto
        JOIN Dim_TipoMovimiento tm ON f.Id_TipoMovimiento = tm.Id_TipoMovimiento
        GROUP BY t.Mes, p.Categoria, p.Criticidad
        ORDER BY t.Mes, p.Categoria;
    """

    # 3) Modelo Estrella con índices de cobertura en claves foráneas (como en 03_DM_ALMACEN_ANONIMO.sql)
    conn_star_idx = sqlite3.connect(":memory:")
    fact_x5.to_sql("Fact_Movimiento", conn_star_idx, index=False)
    dim_p.to_sql("Dim_Producto", conn_star_idx, index=False)
    dim_t.to_sql("Dim_Tiempo", conn_star_idx, index=False)
    dim_tm.to_sql("Dim_TipoMovimiento", conn_star_idx, index=False)
    for ddl_i in [
        "CREATE UNIQUE INDEX idx_dt ON Dim_Tiempo(Id_Fecha);",
        "CREATE UNIQUE INDEX idx_dp ON Dim_Producto(Id_Producto);",
        "CREATE UNIQUE INDEX idx_dtm ON Dim_TipoMovimiento(Id_TipoMovimiento);",
        "CREATE INDEX idx_fact_cover ON Fact_Movimiento(Id_Fecha, Id_Producto, Id_TipoMovimiento, Cantidad, Costo_Total, Dias_Permanencia);",
    ]:
        conn_star_idx.execute(ddl_i)

    m_oltp = medir_repeticiones(lambda: len(conn_oltp.execute(sql_oltp).fetchall()), n_rep=9)
    m_star = medir_repeticiones(lambda: len(conn_star.execute(sql_star).fetchall()), n_rep=9)
    m_star_idx = medir_repeticiones(lambda: len(conn_star_idx.execute(sql_star).fetchall()), n_rep=9)

    speedup_star = round(m_oltp["mediana_ms"] / m_star["mediana_ms"], 2)
    speedup_idx = round(m_oltp["mediana_ms"] / m_star_idx["mediana_ms"], 2)

    return {
        "id": "EXP-BI-01",
        "integrante": "Quispe Ruiz, Johana Carolina",
        "titulo": "Evaluación de latencia analítica: Esquema OLTP Normalizado (3FN) vs. Modelo Estrella vs. Estrella con Índices de Cobertura",
        "dataset_filas": len(fact_x5),
        "escenarios": [
            {
                "escenario": "E1: OLTP Normalizado (3FN, 7 JOINs + cálculo al vuelo)",
                **m_oltp,
                "tablas_unidas": 8,
                "speedup_vs_e1": 1.0,
            },
            {
                "escenario": "E2: Modelo Estrella Kimball (3 JOINs desnormalizados, sin índices)",
                **m_star,
                "tablas_unidas": 4,
                "speedup_vs_e1": speedup_star,
            },
            {
                "escenario": "E3: Modelo Estrella Kimball + Índices de Cobertura en FK",
                **m_star_idx,
                "tablas_unidas": 4,
                "speedup_vs_e1": speedup_idx,
            },
        ],
    }


# =============================================================================
# EXP-BI-02 — Leoneld Mathias Sotelo Espinoza
# Auditoría de Reglas de Calidad RQ-01..RQ-04 ante Inyección Controlada de Defectos
# =============================================================================
def exp_bi_02_leoneld():
    print(">>> Ejecutando EXP-BI-02 (Sotelo Espinoza, Leoneld): Inyección de defectos en ETL (0%, 100 y 535 defectos)...")
    fact = pd.read_csv(RESPALDO / "Fact_Movimiento.csv", sep=";", encoding="utf-8-sig")
    dim_p = pd.read_csv(RESPALDO / "Dim_Producto.csv", sep=";", encoding="utf-8-sig")
    productos_validos = set(dim_p["Id_Producto"].astype(int))

    def correr_validador_calidad(df_in: pd.DataFrame, guardar_csv: str = None):
        t0 = time.perf_counter()
        df = df_in.copy()
        df["Cantidad_num"] = pd.to_numeric(df["Cantidad"], errors="coerce")
        m_nulo = df[["Id_Movimiento_NK", "Id_Fecha", "Id_Producto", "Id_TipoMovimiento", "Cantidad_num"]].isna().any(axis=1)
        m_nopos = (~m_nulo) & (df["Cantidad_num"] <= 0)
        m_sinprod = (~m_nulo) & (~df["Id_Producto"].isin(productos_validos))
        m_dup = df.duplicated(subset=["Id_Movimiento_NK"], keep="first")
        bad = m_nulo | m_nopos | m_sinprod | m_dup

        rech = df[bad].copy()
        motivos = []
        for idx in rech.index:
            ms = []
            if m_nulo.loc[idx]:
                ms.append("RQ-01:nulo_obligatorio")
            if m_nopos.loc[idx]:
                ms.append("RQ-02:cantidad_no_positiva")
            if m_sinprod.loc[idx]:
                ms.append("RQ-03:producto_inexistente")
            if m_dup.loc[idx]:
                ms.append("RQ-04:duplicado_llave")
            motivos.append("+".join(ms))
        rech["Motivo_Rechazo"] = motivos
        if guardar_csv:
            rech.drop(columns=["Cantidad_num"]).to_csv(SALIDAS / guardar_csv, index=False, encoding="utf-8-sig")

        validos = df[~bad].copy()
        verdaderos_positivos = int((bad & df["_es_inyectado"]).sum())
        total_inyectados = int(df["_es_inyectado"].sum())
        dt_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        tpp_val = round(float(validos["Dias_Permanencia"].dropna().mean()), 6)
        return {
            "registros_entrada": int(len(df)),
            "registros_validos_cargados": int(len(validos)),
            "registros_rechazados": int(len(rech)),
            "rq01_nulos": int(m_nulo.sum()),
            "rq02_no_positivos": int(m_nopos.sum()),
            "rq03_sin_producto": int(m_sinprod.sum()),
            "rq04_duplicados": int(m_dup.sum()),
            "completitud_lote_pct": round(100.0 * len(validos) / len(df), 2),
            "precision_deteccion_pct": round(100 * verdaderos_positivos / len(rech), 2) if len(rech) else None,
            "recobrado_defectos_pct": round(100 * verdaderos_positivos / total_inyectados, 2) if total_inyectados else None,
            "tpp_post_limpieza": tpp_val,
            "latencia_validacion_ms": dt_ms,
        }

    def inyectar_defectos(df_base: pd.DataFrame, n_por_tipo: int) -> pd.DataFrame:
        df_base = df_base.copy()
        df_base["_es_inyectado"] = False
        if n_por_tipo == 0:
            return df_base
        muestra = df_base.head(n_por_tipo * 4).copy().reset_index(drop=True)
        # 1) Nulos obligatorios
        b1 = muestra.iloc[0:n_por_tipo].copy()
        b1["Id_Movimiento_NK"] = range(900001, 900001 + n_por_tipo)
        b1["Cantidad"] = None
        # 2) Cantidades <= 0
        b2 = muestra.iloc[n_por_tipo:2 * n_por_tipo].copy()
        b2["Id_Movimiento_NK"] = range(910001, 910001 + n_por_tipo)
        b2["Cantidad"] = -5
        # 3) Producto inexistente (Id_Producto = 999)
        b3 = muestra.iloc[2 * n_por_tipo:3 * n_por_tipo].copy()
        b3["Id_Movimiento_NK"] = range(920001, 920001 + n_por_tipo)
        b3["Id_Producto"] = 999
        # 4) Duplicados exactos de llave natural existente
        b4 = df_base.iloc[0:n_por_tipo].copy()
        for parte in (b1, b2, b3, b4):
            parte["_es_inyectado"] = True
        return pd.concat([df_base, b1, b2, b3, b4], ignore_index=True)

    e1 = correr_validador_calidad(inyectar_defectos(fact, 0))
    e2 = correr_validador_calidad(inyectar_defectos(fact, 25), guardar_csv="exp_bi_02_rechazados_100.csv")
    e3 = correr_validador_calidad(inyectar_defectos(fact, 135), guardar_csv="exp_bi_02_rechazados_540.csv")

    return {
        "id": "EXP-BI-02",
        "integrante": "Sotelo Espinoza, Leoneld Mathias",
        "titulo": "Evaluación de robustez y sensibilidad de las reglas de calidad del ETL ante inyección controlada de anomalías de datos",
        "escenarios": [
            {"escenario": "E1: Fuente oficial limpia (0 defectos inyectados)", **e1},
            {"escenario": "E2: Contaminación moderada (+100 defectos: 25 por regla RQ-01..RQ-04)", **e2},
            {"escenario": "E3: Contaminación severa (+540 defectos: 135 por regla, ~10% del lote)", **e3},
        ],
    }


# =============================================================================
# EXP-BI-03 — Camila Nadine Montalvo Abrego
# Evaluación de formulación de medidas analíticas en el Modelo Tabular / BI
# =============================================================================
def exp_bi_03_camila():
    print(">>> Ejecutando EXP-BI-03 (Montalvo Abrego, Camila): Formulación de medidas DAX/analíticas sobre el modelo...")
    fact = pd.read_csv(RESPALDO / "Fact_Movimiento.csv", sep=";", encoding="utf-8-sig")
    dim_p = pd.read_csv(RESPALDO / "Dim_Producto.csv", sep=";", encoding="utf-8-sig")
    dim_tm = pd.read_csv(RESPALDO / "Dim_TipoMovimiento.csv", sep=";", encoding="utf-8-sig")
    dim_t = pd.read_csv(RESPALDO / "Dim_Tiempo.csv", sep=";", encoding="utf-8-sig")
    plan = pd.read_csv(RESPALDO / "stg_Plan_Mensual.csv", sep=";", encoding="utf-8-sig")

    # Escala x10 (53,480 filas) para simular refresco interactivo de las 12 tarjetas y visuales
    fact_big = pd.concat([fact for _ in range(10)], ignore_index=True)

    # Escenario 1: Iteración fila por fila sin banderas precalculadas (equivalente a SUMX(FILTER(Fact, RELATED(...) IN {...})))
    def patron_iterador_fila():
        df = fact_big.merge(dim_tm, on="Id_TipoMovimiento").merge(dim_p, on="Id_Producto").merge(dim_t, on="Id_Fecha")
        ent = sum(r["Cantidad"] for _, r in df.iterrows() if r["Codigo"] in ("INGRESO", "DEVOLUCION", "TRASLADO_ENTRADA", "MANTENIMIENTO_ENTRADA"))
        sal = sum(r["Cantidad"] for _, r in df.iterrows() if r["Codigo"] in ("SALIDA", "ASIGNACION", "BAJA", "OBSOLESCENCIA"))
        return round(sal / ent, 4)

    # Escenario 2: Contexto de filtro tabular vectorizado con banderas booleanas Es_Entrada / Es_Salida_Real (Patrón CALCULATE implementado)
    def patron_calculate_banderas():
        df = fact_big.merge(dim_tm[["Id_TipoMovimiento", "Es_Entrada", "Es_Salida_Real"]], on="Id_TipoMovimiento")
        ent = df.loc[df["Es_Entrada"] == 1, "Cantidad"].sum()
        sal = df.loc[df["Es_Salida_Real"] == 1, "Cantidad"].sum()
        c_ent = df.loc[df["Es_Entrada"] == 1, "Costo_Total"].sum()
        c_sal = df.loc[df["Es_Salida_Real"] == 1, "Costo_Total"].sum()
        tpp = df["Dias_Permanencia"].mean()
        vm_e = ((ent / 10.0) - plan["Entradas_Plan"].sum()) / plan["Entradas_Plan"].sum()
        return round(float(sal / ent), 4)

    # Escenario 3: Tabla resumen pre-agregada por Mes y Categoría (Patrón Aggregations en memoria)
    resumen_cubo = (
        fact_big.merge(dim_tm[["Id_TipoMovimiento", "Es_Entrada", "Es_Salida_Real"]], on="Id_TipoMovimiento")
        .merge(dim_p[["Id_Producto", "Categoria"]], on="Id_Producto")
        .merge(dim_t[["Id_Fecha", "Mes"]], on="Id_Fecha")
        .groupby(["Mes", "Categoria"], as_index=False)
        .agg(
            u_ent=("Cantidad", lambda s: s[fact_big.loc[s.index, "Id_TipoMovimiento"] == 1].sum()),
            Cantidad_total=("Cantidad", "sum"),
            Dias_Permanencia=("Dias_Permanencia", "mean"),
        )
    )
    # Pre-agregación limpia con máscaras vectorizadas
    df_joined = (
        fact_big.merge(dim_tm[["Id_TipoMovimiento", "Es_Entrada", "Es_Salida_Real"]], on="Id_TipoMovimiento")
        .merge(dim_p[["Id_Producto", "Categoria"]], on="Id_Producto")
        .merge(dim_t[["Id_Fecha", "Mes"]], on="Id_Fecha")
    )
    df_joined["u_ent"] = df_joined["Cantidad"] * df_joined["Es_Entrada"]
    df_joined["u_sal"] = df_joined["Cantidad"] * df_joined["Es_Salida_Real"]
    df_joined["c_ent"] = df_joined["Costo_Total"] * df_joined["Es_Entrada"]
    df_joined["c_sal"] = df_joined["Costo_Total"] * df_joined["Es_Salida_Real"]
    cubo_agg = df_joined.groupby(["Mes", "Categoria"], as_index=False).agg(
        u_ent=("u_ent", "sum"), u_sal=("u_sal", "sum"), c_ent=("c_ent", "sum"), c_sal=("c_sal", "sum")
    )

    def patron_cubo_preagregado():
        ent = cubo_agg["u_ent"].sum()
        sal = cubo_agg["u_sal"].sum()
        inv = cubo_agg["c_ent"].sum() - cubo_agg["c_sal"].sum()
        return round(float(sal / ent), 4)

    m_e1 = medir_repeticiones(patron_iterador_fila, n_rep=3, warmup=1)
    m_e2 = medir_repeticiones(patron_calculate_banderas, n_rep=9, warmup=1)
    m_e3 = medir_repeticiones(patron_cubo_preagregado, n_rep=9, warmup=1)

    return {
        "id": "EXP-BI-03",
        "integrante": "Montalvo Abrego, Camila Nadine",
        "titulo": "Optimización de tiempos de respuesta en medidas del modelo analítico: Iteradores de fila vs. Filtro por banderas booleanas vs. Cubo pre-agregado",
        "dataset_filas": len(fact_big),
        "escenarios": [
            {
                "escenario": "E1: Iteración fila a fila sobre texto (Patrón FILTER + SUMX no optimizado)",
                **m_e1,
                "cumple_rnf01_3000ms": m_e1["mediana_ms"] <= 3000.0,
                "speedup_vs_e1": 1.0,
            },
            {
                "escenario": "E2: Contexto CALCULATE con banderas booleanas en Dim_TipoMovimiento (Implementado)",
                **m_e2,
                "cumple_rnf01_3000ms": m_e2["mediana_ms"] <= 3000.0,
                "speedup_vs_e1": round(m_e1["mediana_ms"] / m_e2["mediana_ms"], 1),
            },
            {
                "escenario": "E3: Tabla resumen pre-agregada Mes × Categoría (63 celdas OLAP)",
                **m_e3,
                "cumple_rnf01_3000ms": m_e3["mediana_ms"] <= 3000.0,
                "speedup_vs_e1": round(m_e1["mediana_ms"] / m_e3["mediana_ms"], 1),
            },
        ],
    }


# =============================================================================
# EXP-BI-04 — Mishell Alexandra Belen Arroyo Julca
# Escalamiento progresivo de volumetría en el motor FIFO de permanencia (x1, x5, x10, x25)
# =============================================================================
def exp_bi_04_mishell():
    print(">>> Ejecutando EXP-BI-04 (Arroyo Julca, Mishell): Escalamiento volumétrico FIFO x1, x5, x10 y x25...")
    fact = list(csv.DictReader(open(RESPALDO / "Fact_Movimiento.csv", encoding="utf-8-sig"), delimiter=";"))
    base = [
        {
            "fecha": datetime.fromisoformat(f["Fecha"]),
            "prod": f["Id_Producto"],
            "cantidad": int(float(f["Cantidad"])),
            "es_entrada": f["Id_TipoMovimiento"] == "1",
        }
        for f in fact
    ]
    base.sort(key=lambda x: x["fecha"])

    def ejecutar_fifo_copias(copias):
        suma_dias, n_sal, u_ent, u_sal = 0.0, 0, 0, 0
        for copia in copias:
            lotes = defaultdict(deque)
            for m in copia:
                if m["es_entrada"]:
                    lotes[m["prod"]].append([m["fecha"], m["cantidad"]])
                    u_ent += m["cantidad"]
                else:
                    u_sal += m["cantidad"]
                    qq = m["cantidad"]
                    tot, tom = 0.0, 0
                    while qq > 0 and lotes[m["prod"]]:
                        lote = lotes[m["prod"]][0]
                        toma = min(qq, lote[1])
                        tot += (m["fecha"] - lote[0]).days * toma
                        tom += toma
                        lote[1] -= toma
                        qq -= toma
                        if lote[1] == 0:
                            lotes[m["prod"]].popleft()
                    if tom:
                        suma_dias += tot / tom
                        n_sal += 1
        return {"tpp": round(suma_dias / n_sal, 6), "rotacion": round(u_sal / u_ent, 4), "salidas": n_sal}

    escenarios = []
    for factor in (1, 5, 10, 25):
        copias = [[dict(m) for m in base] for _ in range(factor)]
        med = medir_repeticiones(lambda: ejecutar_fifo_copias(copias), n_rep=7, warmup=1)
        movs = len(base) * factor
        throughput = round(movs / (med["mediana_ms"] / 1000.0), 0)
        escenarios.append({
            "escenario": f"Escala x{factor} ({movs:,} movimientos)",
            "factor": factor,
            "movimientos": movs,
            "mediana_ms": med["mediana_ms"],
            "min_ms": med["min_ms"],
            "max_ms": med["max_ms"],
            "std_ms": med["std_ms"],
            "throughput_mov_s": throughput,
            "tpp_verificado": med["resultado_muestra"]["tpp"],
            "rotacion_verificada": med["resultado_muestra"]["rotacion"],
        })

    return {
        "id": "EXP-BI-04",
        "integrante": "Arroyo Julca, Mishell Alexandra Belen",
        "titulo": "Análisis de escalabilidad progresiva e invarianza de KPIs en el algoritmo de trazabilidad FIFO (x1, x5, x10 y x25)",
        "escenarios": escenarios,
    }


# =============================================================================
# EXP-BI-05 — Diego Jason Cordova Paz
# Optimización de la Fase de Carga Transaccional del Datamart (Cuello de botella del ETL)
# =============================================================================
def exp_bi_05_diego():
    print(">>> Ejecutando EXP-BI-05 (Cordova Paz, Diego): Optimización de carga transaccional al Datamart...")
    fact = pd.read_csv(RESPALDO / "Fact_Movimiento.csv", sep=";", encoding="utf-8-sig")
    registros_tuplas = list(fact.itertuples(index=False, name=None))
    cols_sql = ",".join(fact.columns)
    placeholders = ",".join(["?"] * len(fact.columns))
    ddl = f"CREATE TABLE Fact_Movimiento ({', '.join(c + ' TEXT' for c in fact.columns)});"

    # E1: Inserción fila a fila con autocommit por sentencia en disco (transacción con fsync real)
    db_file = Path("/tmp/test_dm_carga.sqlite")

    def carga_autocommit_unitaria():
        if db_file.exists():
            db_file.unlink()
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute(ddl)
        sql = f"INSERT INTO Fact_Movimiento ({cols_sql}) VALUES ({placeholders})"
        # Medir sobre subconjunto escalado a 5348 filas
        for r in registros_tuplas:
            conn.execute(sql, r)
            conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM Fact_Movimiento").fetchone()[0]
        conn.close()
        return n

    # E2: pandas.to_sql estándar por bloques (chunksize=500) en una transacción en disco
    def carga_pandas_chunks():
        if db_file.exists():
            db_file.unlink()
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA synchronous = NORMAL;")
        with conn:
            fact.to_sql("Fact_Movimiento", conn, if_exists="replace", index=False, chunksize=500)
        n = conn.execute("SELECT COUNT(*) FROM Fact_Movimiento").fetchone()[0]
        conn.close()
        return n

    # E3: Carga masiva parametrizada executemany dentro de una única transacción atómica en disco
    def carga_batch_executemany():
        if db_file.exists():
            db_file.unlink()
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute(ddl)
        sql = f"INSERT INTO Fact_Movimiento ({cols_sql}) VALUES ({placeholders})"
        with conn:
            conn.executemany(sql, registros_tuplas)
        n = conn.execute("SELECT COUNT(*) FROM Fact_Movimiento").fetchone()[0]
        conn.close()
        return n

    m1 = medir_repeticiones(carga_autocommit_unitaria, n_rep=3, warmup=0)
    m2 = medir_repeticiones(carga_pandas_chunks, n_rep=7, warmup=1)
    m3 = medir_repeticiones(carga_batch_executemany, n_rep=7, warmup=1)
    if db_file.exists():
        db_file.unlink()

    return {
        "id": "EXP-BI-05",
        "integrante": "Cordova Paz, Diego Jason",
        "titulo": "Optimización de la fase de carga del ETL al Datamart: Autocommit unitario vs. Bloques pandas vs. Carga masiva transaccional (executemany)",
        "dataset_filas": len(fact),
        "escenarios": [
            {
                "escenario": "E1: Inserción unitaria con autocommit por fila (5,348 transacciones)",
                **m1,
                "filas_por_segundo": round(len(fact) / (m1["mediana_ms"] / 1000.0), 0),
                "speedup_vs_e1": 1.0,
            },
            {
                "escenario": "E2: Carga pandas to_sql por lotes (chunksize=500, 1 transacción)",
                **m2,
                "filas_por_segundo": round(len(fact) / (m2["mediana_ms"] / 1000.0), 0),
                "speedup_vs_e1": round(m1["mediana_ms"] / m2["mediana_ms"], 2),
            },
            {
                "escenario": "E3: Carga masiva vectorizada executemany en 1 transacción atómica",
                **m3,
                "filas_por_segundo": round(len(fact) / (m3["mediana_ms"] / 1000.0), 0),
                "speedup_vs_e1": round(m1["mediana_ms"] / m3["mediana_ms"], 2),
            },
        ],
    }


def main():
    t0 = time.perf_counter()
    resultados = {
        "suite": "Experimentos de Business Intelligence (BI) — Unity Perú SAC",
        "fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "python_version": sys.version.split()[0],
        "experimentos": [
            exp_bi_01_johana(),
            exp_bi_02_leoneld(),
            exp_bi_03_camila(),
            exp_bi_04_mishell(),
            exp_bi_05_diego(),
        ],
        "tiempo_total_suite_s": round(time.perf_counter() - t0, 3),
    }
    out = SALIDAS / "resultados_experimentos_bi.json"
    out.write_text(json.dumps(resultados, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n=== Suite BI finalizada en {resultados['tiempo_total_suite_s']} s -> {out} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
