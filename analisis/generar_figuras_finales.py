#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generar_figuras_finales.py — Genera los diagramas de arquitectura integrada,
arquitectura Medallion Big Data, EDA distribuido, Dashboard Databricks, Genie IA
y los 10 gráficos comparativos de los experimentos (5 BI + 5 Big Data),
respetando las capturas originales de Power BI del equipo.
"""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle

RAIZ = Path(__file__).resolve().parents[1]
EVID_BI = RAIZ / "evidencias"
EVID_BD = RAIZ / "bigdata" / "evidencias"
EVID_EXP = RAIZ / "experimentos" / "evidencias"
EVID_BD.mkdir(parents=True, exist_ok=True)
EVID_EXP.mkdir(parents=True, exist_ok=True)

MED = json.loads((RAIZ / "bigdata" / "salidas" / "medallion_resultados.json").read_text(encoding="utf-8"))
EXP_BI = json.loads((RAIZ / "experimentos" / "salidas" / "resultados_experimentos_bi.json").read_text(encoding="utf-8"))
EXP_BD = json.loads((RAIZ / "experimentos" / "salidas" / "resultados_experimentos_bigdata.json").read_text(encoding="utf-8"))

AZUL = "#0B2545"
AZUL2 = "#1B6CA8"
CELESTE = "#8FB8DE"
AMBAR = "#E09F3E"
VERDE = "#2A9D8F"
ROJO = "#C44536"
MORADO = "#5E548E"
GRIS = "#5C677D"
FONDO = "#F4F6F8"
MESES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Set"]

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 9,
    "axes.edgecolor": "#CBD5E1",
    "axes.labelcolor": "#1E293B",
    "text.color": "#1E293B",
    "axes.titlesize": 10.5,
    "axes.titleweight": "bold",
    "savefig.dpi": 260,
    "figure.dpi": 120,
})


def caja(ax, x, y, w, h, titulo, detalle="", color=AZUL2, tc="white", fs=8.8, fs2=7.2, ec="none", lw=0):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.004,rounding_size=0.35",
        linewidth=lw, edgecolor=ec, facecolor=color, zorder=2
    ))
    ax.text(
        x + w / 2, y + h * (0.68 if detalle else 0.5),
        titulo, ha="center", va="center", color=tc, fontsize=fs, fontweight="bold", zorder=3
    )
    if detalle:
        ax.text(
            x + w / 2, y + h * 0.31,
            detalle, ha="center", va="center", color=tc, fontsize=fs2, zorder=3, linespacing=1.32
        )


def flecha(ax, x1, y1, x2, y2, color=AZUL, lw=1.7):
    ax.add_patch(FancyArrowPatch(
        (x1, y1), (x2, y2),
        arrowstyle="-|>", mutation_scale=13, linewidth=lw, color=color, zorder=4, shrinkA=2, shrinkB=2
    ))


def guardar(fig, ruta: Path):
    fig.savefig(ruta, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print("  Generada:", ruta.relative_to(RAIZ))


# =============================================================================
# 1. Arquitectura Integrada BI + Big Data (Capítulo III, §3.3)
# =============================================================================
def gen_arquitectura_integrada():
    fig, ax = plt.subplots(figsize=(12.4, 6.8))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 68)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    ax.text(2, 65.2, "Arquitectura Tecnológica Integrada: Solución BI + Solución Big Data — Unity Perú SAC",
            fontsize=12, fontweight="bold", color=AZUL)
    ax.text(2, 62.7, "Coexistencia e integración de analítica corporativa (SQL Server + Power BI) y procesamiento distribuido Lakehouse (Databricks + PySpark + Delta + Genie)",
            fontsize=8.2, color=GRIS)

    # Encabezados de columnas
    cols = [(2, 20, "1. FUENTES HETEROGÉNEAS"), (25, 47, "2. CAPAS DE PROCESAMIENTO Y ALMACENAMIENTO"), (75, 23, "3. CONSUMO Y DECISIONES")]
    for x, w, t in cols:
        ax.add_patch(Rectangle((x, 57.5), w, 3.6, facecolor="#E2E8F0", edgecolor="none"))
        ax.text(x + w / 2, 59.3, t, ha="center", va="center", fontsize=8.5, fontweight="bold", color=AZUL)

    # Fuentes
    caja(ax, 2, 46.5, 20, 9.2, "ALMACEN_ANONIMO", "SQL Server OLTP · 11 tablas\n5,348 movimientos · 24 CMP\n3,685 lotes · 5 proyectos", AZUL, fs=8.4, fs2=6.8)
    caja(ax, 2, 35.5, 20, 9.2, "Plan_Demanda_2026.xlsx", "Planificación táctica Excel\n63 registros (7 categorías × 9 meses)", AZUL2, fs=8.2, fs2=6.8)
    caja(ax, 2, 24.5, 20, 9.2, "Sensores_Almacen_2026.txt", "Telemetría horaria IoT (TXT/CSV)\n25,824 lecturas Temp/Humedad", "#2B7A78", fs=8.0, fs2=6.8)
    caja(ax, 2, 13.5, 20, 9.2, "Conteos_Ciclicos_2026.txt", "Auditoría física mensual (TXT)\n72 conteos sistema vs. físico", "#3A506B", fs=8.0, fs2=6.8)

    # Carril Superior: Solución BI
    ax.add_patch(FancyBboxPatch((25, 36.2), 47, 19.6, boxstyle="round,pad=0.004,rounding_size=0.4",
                                facecolor="#EFF6FF", edgecolor="#93C5FD", linewidth=1.2, zorder=1))
    ax.text(26.2, 53.8, "CARRIL A — SOLUCIÓN BI (Corte Táctico Diario: SQL Server + ETL Python + Power BI)",
            fontsize=8.0, fontweight="bold", color=AZUL)
    caja(ax, 26.5, 38.2, 20.5, 13.8, "ETL Python v2.0", "pandas + SQLAlchemy + pyodbc\n5 fases · Reglas RQ-01..RQ-06\nCálculo FIFO Dias_Permanencia\n1 Transacción ACID + Log JSON", AZUL2, fs=8.4, fs2=6.7)
    caja(ax, 49.5, 38.2, 21.0, 13.8, "DM_ALMACEN_ANONIMO", "SQL Server · Modelo Estrella Kimball\n1 Hecho (Fact_Movimiento: 5,348)\n9 Dimensiones + 4 Staging\n11 Claves Foráneas verificadas", AZUL, fs=8.3, fs2=6.7)
    flecha(ax, 47.2, 45.1, 49.3, 45.1)

    # Carril Inferior: Solución Big Data
    ax.add_patch(FancyBboxPatch((25, 12.8), 47, 21.4, boxstyle="round,pad=0.004,rounding_size=0.4",
                                facecolor="#FFFBEB", edgecolor="#FCD34D", linewidth=1.2, zorder=1))
    ax.text(26.2, 32.1, "CARRIL B — SOLUCIÓN BIG DATA (Databricks + PySpark 3.5 + Medallion)",
            fontsize=8.0, fontweight="bold", color="#92400E")
    caja(ax, 26.5, 15.0, 13.8, 15.0, "Capa Bronze", "Datos crudos + linaje\nDelta / Parquet\n31,331 registros\nmultiformato", "#B45309", fs=8.3, fs2=6.6)
    caja(ax, 41.6, 15.0, 13.8, 15.0, "Capa Silver", "PySpark DataFrames\nfilter, na, dropDuplicates\njoin, partición por mes\nCalidad + EDA Spark", "#64748B", fs=8.3, fs2=6.6)
    caja(ax, 56.7, 15.0, 13.8, 15.0, "Capa Gold", "Tablas Delta negocio\nkpi_mensual · categoria\nproyectos · telemetría\nBroadcast + AQE", AMBAR, fs=8.3, fs2=6.6)
    flecha(ax, 40.5, 22.5, 41.4, 22.5, color="#92400E")
    flecha(ax, 55.6, 22.5, 56.5, 22.5, color="#92400E")

    # Consumo
    caja(ax, 75, 38.2, 23, 13.8, "Dashboard Power BI", "Modo Importación · 12 medidas DAX\n3 Vistas: Gerencial · Operativa · Proyectos\nDecisiones: compra, FIFO y SLA", AZUL, fs=8.3, fs2=6.7)
    caja(ax, 75, 23.8, 23, 10.4, "Databricks Dashboards", "Visualizaciones Lakeview sobre Gold\nMonitoreo escalable de stock, costos\ny telemetría IoT multianual", VERDE, fs=8.3, fs2=6.6)
    caja(ax, 75, 12.8, 23, 9.4, "Databricks Genie (IA)", "Consultas en lenguaje natural\nsobre tablas Unity Catalog Gold\n(Declarado en Anexo I)", MORADO, fs=8.3, fs2=6.6)

    # Flechas desde fuentes hacia ambos carriles
    for yy in (51.0, 40.0, 29.0, 18.0):
        flecha(ax, 22.2, yy, 25.0, 45.0 if yy >= 35 else 22.5, color=AZUL2 if yy >= 35 else "#B45309", lw=1.3)
    flecha(ax, 22.2, 51.0, 25.0, 24.0, color="#B45309", lw=1.2)
    flecha(ax, 22.2, 29.0, 25.0, 43.0, color=AZUL2, lw=1.2)

    flecha(ax, 70.7, 45.1, 74.8, 45.1, color=AZUL)
    flecha(ax, 70.7, 24.5, 74.8, 28.5, color=VERDE)
    flecha(ax, 70.7, 19.5, 74.8, 17.5, color=MORADO)

    # Franja inferior de conciliación y gobierno
    ax.add_patch(FancyBboxPatch((2, 2.2), 96, 8.2, boxstyle="round,pad=0.004,rounding_size=0.35",
                                facecolor="#F1F5F9", edgecolor="#94A3B8", linewidth=1.0))
    ax.text(4, 8.2, "Punto de Integración, Gobierno y Conciliación Cruzada (BI ↔ Big Data):",
            fontsize=8.5, fontweight="bold", color=AZUL)
    ax.text(4, 4.7, "• Ambas soluciones consumen las mismas fuentes anonimizadas (Ley N.° 29733) y concilian exactamente: 5,348 movimientos · TPP = 3.868701 días · Rotación = 0.9775 · S/ 812,060.\n"
                    "• Seguridad: credenciales en variables de entorno (.gitignore), separación de esquemas (unity_bronze / silver / gold) y trazabilidad completa en GitHub + logs JSON.",
            fontsize=7.3, color="#334155", linespacing=1.4)

    guardar(fig, EVID_BI / "fig01_arquitectura_integrada.png")


# =============================================================================
# 2. Arquitectura Big Data Medallion en Databricks (Capítulo III, §3.5.2)
# =============================================================================
def gen_arquitectura_bigdata_medallion():
    fig, ax = plt.subplots(figsize=(12.0, 5.6))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 56)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    ax.text(2, 53.0, "Arquitectura de la Solución Big Data en Databricks — Patrón Medallion (Bronze · Silver · Gold)",
            fontsize=11.5, fontweight="bold", color=AZUL)
    ax.text(2, 50.4, "Procesamiento distribuido con Apache Spark 3.5 (PySpark DataFrames + Spark SQL + Delta Lake + Genie IA)",
            fontsize=8.2, color=GRIS)

    caja(ax, 2, 14, 17, 33, "Fuentes de Datos\n(DBFS / Volumes)",
         "• Fact_Movimiento\n  (5,348 filas)\n• Catálogo OLTP\n  (24 CMP, 3,685 lotes)\n• Plan_Demanda\n  (63 filas XLSX)\n• Sensores IoT\n  (25,824 filas TXT)\n• Conteos Cíclicos\n  (72 filas TXT)",
         AZUL, fs=8.6, fs2=6.9)

    caja(ax, 22.5, 14, 17.5, 33, "Capa BRONZE\n(unity_bronze.*)",
         "Ingesta raw fiel al origen\ncon spark.read.csv()\n\nMetadatos de auditoría:\n• _fuente\n• _ts_ingesta\n\nFormato: Delta / Parquet\nSin pérdida de registros\n(31,331 filas totales)",
         "#B45309", fs=8.6, fs2=6.9)

    caja(ax, 43.5, 14, 18.5, 33, "Capa SILVER\n(unity_silver.*)",
         "EDA + Limpieza PySpark:\n• describe(), printSchema()\n• na.drop() / na.fill()\n• filter(Cantidad > 0)\n• dropDuplicates()\n• withColumnRenamed()\n• join() con dimensiones\n• partitionBy('mes')\n(Snappy + Predicate Pushdown)",
         "#475569", fs=8.6, fs2=6.7)

    caja(ax, 65.5, 14, 16.5, 33, "Capa GOLD\n(unity_gold.*)",
         "Agregaciones de negocio:\n• kpi_mensual (Window)\n• kpi_categoria\n• stock_componentes\n• consumo_proyectos\n• telemetria_almacen\n\nBroadcastHashJoin +\nAQE optimizado",
         AMBAR, fs=8.6, fs2=6.8)

    caja(ax, 85, 31.5, 13.5, 15.5, "Databricks\nDashboard",
         "Tablas y gráficos\ninteractivos para\nGerencia, Logística\ny Proyectos",
         VERDE, fs=8.4, fs2=6.7)

    caja(ax, 85, 14, 13.5, 15.5, "Databricks\nGenie (IA)",
         "Consultas NL2SQL\nen lenguaje natural\nsobre tablas Gold\nverificadas",
         MORADO, fs=8.4, fs2=6.7)

    flecha(ax, 19.2, 30.5, 22.3, 30.5)
    flecha(ax, 40.2, 30.5, 43.3, 30.5)
    flecha(ax, 62.2, 30.5, 65.3, 30.5)
    flecha(ax, 82.2, 37.5, 84.8, 39.0, color=VERDE)
    flecha(ax, 82.2, 23.5, 84.8, 22.0, color=MORADO)

    ax.add_patch(FancyBboxPatch((2, 2.5), 96.5, 8.5, boxstyle="round,pad=0.004,rounding_size=0.3",
                                facecolor="#F8FAFC", edgecolor="#CBD5E1", lw=1))
    ax.text(3.8, 8.4, "Persistencia y Gobernanza en Unity Catalog / Hive Metastore:", fontsize=8.2, fontweight="bold", color=AZUL)
    ax.text(3.8, 4.9, "Esquemas desacoplados (unity_bronze, unity_silver, unity_gold) · Compresión Snappy (−98.8 % vs CSV) · Pipeline end-to-end: 28.49 s.",
            fontsize=7.2, color="#334155")

    guardar(fig, EVID_BD / "fig_bd01_arquitectura_medallion.png")


# =============================================================================
# 3. EDA y Limpieza en PySpark (Capítulo VI, Solución Big Data)
# =============================================================================
def gen_eda_spark():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.4, 4.2))
    fig.patch.set_facecolor("white")

    # Panel 1: Tramos de permanencia calculados en Capa Silver
    tramos = MED["eda"]["tramos_permanencia"]
    salidas_tramos = [t for t in tramos if t["tramo_permanencia"] != "INGRESO_LOTE"]
    orden = ["RAPIDO_0_3D", "NORMAL_4_7D", "MODERADO_8_10D", "RETENIDO_MAS_10D"]
    etiquetas = ["Rápido\n(0 a 3 días)", "Normal\n(4 a 7 días)", "Moderado\n(8 a 10 días)", "Retenido\n(> 10 días)"]
    mapa_t = {t["tramo_permanencia"]: t["movimientos"] for t in salidas_tramos}
    vals = [mapa_t.get(k, 0) for k in orden]
    colores = [VERDE, AZUL2, AMBAR, ROJO]

    bars = ax1.bar(etiquetas, vals, color=colores, width=0.56)
    total_s = sum(vals)
    for b, v in zip(bars, vals):
        pct = v / total_s * 100
        ax1.text(b.get_x() + b.get_width() / 2, v + 18, f"{v:,}\n({pct:.1f}%)", ha="center", fontsize=7.6, fontweight="bold")
    ax1.set_ylim(0, max(vals) * 1.24)
    ax1.set_ylabel("Número de despachos (salidas)")
    ax1.set_title("EDA PySpark: Distribución de salidas por tramo FIFO", loc="left")
    ax1.spines[["top", "right"]].set_visible(False)
    ax1.grid(axis="y", color="#E2E8F0")
    ax1.set_axisbelow(True)

    # Panel 2: Perfil de nulidad semántica en Bronze vs Silver
    cols_n = ["Cantidad /\nCosto", "Id_Producto /\nId_Fecha", "Id_Proveedor\n(Salidas)", "Id_Proyecto\n(Ingresos)", "Dias_Permanencia\n(Ingresos)"]
    nulos_bronze = [0, 0, 1663, 3685, 3685]
    nulos_silver = [0, 0, 0, 0, 3685]  # En Silver se imputan códigos opcionales y se preserva NULL en Dias_Permanencia de ingresos por diseño
    x = np.arange(len(cols_n))
    ax2.bar(x - 0.18, nulos_bronze, 0.35, label="Capa Bronze (Crudo)", color="#94A3B8")
    ax2.bar(x + 0.18, nulos_silver, 0.35, label="Capa Silver (Post na.fill / diseño)", color=AZUL)
    for xi, (vb, vs) in enumerate(zip(nulos_bronze, nulos_silver)):
        ax2.text(xi - 0.18, vb + 60, str(vb), ha="center", fontsize=7, color="#475569")
        ax2.text(xi + 0.18, vs + 60, str(vs), ha="center", fontsize=7, color=AZUL, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels(cols_n, fontsize=7.5)
    ax2.set_ylim(0, 4300)
    ax2.set_ylabel("Cantidad de registros nulos")
    ax2.set_title("Tratamiento de valores nulos: Bronze vs. Silver", loc="left")
    ax2.legend(frameon=False, fontsize=8)
    ax2.spines[["top", "right"]].set_visible(False)
    ax2.grid(axis="y", color="#E2E8F0")
    ax2.set_axisbelow(True)

    fig.suptitle("Análisis Exploratorio (EDA) y Preparación de Calidad sobre DataFrames de Apache Spark",
                 fontsize=11.5, fontweight="bold", color=AZUL, y=1.03)
    guardar(fig, EVID_BD / "fig_bd02_eda_calidad_spark.png")


# =============================================================================
# 4. Dashboard Analítico de Databricks sobre Capa Gold (Lakeview UI)
# =============================================================================
def gen_dashboard_databricks():
    fig = plt.figure(figsize=(12.5, 7.0))
    fig.patch.set_facecolor("#F8FAFC")

    # Barra superior estilo Databricks Lakeview
    top = fig.add_axes([0.0, 0.91, 1.0, 0.09])
    top.axis("off")
    top.add_patch(Rectangle((0, 0), 1, 1, facecolor="#1B3139"))
    top.add_patch(Rectangle((0, 0), 0.008, 1, facecolor="#FF3621"))  # Acento rojo Databricks
    top.text(0.022, 0.62, "MAQUETA de dashboard · Unity Perú SAC — basada en resultados Spark locales",
             color="white", fontsize=12, fontweight="bold", va="center")
    top.text(0.022, 0.24, "Ilustración generada con Python; no es una captura de Databricks · Corte: Ene–Set 2026",
             color="#CBD5E1", fontsize=7.8, va="center")
    top.text(0.98, 0.50, "No desplegado en Databricks", color="#34D399", fontsize=8.5, fontweight="bold", ha="right", va="center")

    # Tarjetas KPI Gold
    kpis = [
        ("Movimientos Procesados", "5,348", "unity_silver.movimientos", "#1B3139"),
        ("TPP Promedio FIFO", "3.87 días", "Meta ≤ 45 d · Exacto 3.8687", VERDE),
        ("Rotación de Flujo", "0.9775", "15,078 sal. / 15,425 ent.", AZUL2),
        ("Inversión Retenida", "S/ 812,060", "Stock neto: +347 u.", AMBAR),
        ("Exactitud & Stock Mín.", "96.55 %", "24/24 CMP sobre mínimo", "#6366F1"),
    ]
    for i, (tit, val, sub, col) in enumerate(kpis):
        ax_k = fig.add_axes([0.02 + i * 0.194, 0.755, 0.182, 0.13])
        ax_k.axis("off")
        ax_k.add_patch(FancyBboxPatch((0, 0), 1, 1, boxstyle="round,pad=0.01,rounding_size=0.05",
                                      facecolor="white", edgecolor="#E2E8F0", lw=1.0))
        ax_k.add_patch(Rectangle((0, 0.90), 1, 0.10, facecolor=col))
        ax_k.text(0.07, 0.68, tit, fontsize=7.6, color=GRIS, fontweight="bold")
        ax_k.text(0.07, 0.38, val, fontsize=15, color="#0F172A", fontweight="bold")
        ax_k.text(0.07, 0.13, sub, fontsize=6.8, color=GRIS)

    gm = MED["gold_mensual"]
    gc = MED["gold_categoria"]
    gp = MED["gold_proyectos"]

    # Visual 1: Flujo mensual + Stock Acumulado (Window Function en Gold)
    ax1 = fig.add_axes([0.045, 0.41, 0.43, 0.29])
    x = np.arange(len(gm))
    ax1.bar(x - 0.19, [r["unidades_entradas"] for r in gm], 0.38, color=AZUL2, label="Entradas (u)")
    ax1.bar(x + 0.19, [r["unidades_salidas"] for r in gm], 0.38, color=AMBAR, label="Salidas (u)")
    ax1_t = ax1.twinx()
    ax1_t.plot(x, [r["stock_acumulado"] for r in gm], color=VERDE, marker="o", lw=2.0, label="Stock neto acum.")
    ax1_t.set_ylabel("Stock acumulado (u)", fontsize=7.5, color=VERDE)
    ax1_t.tick_params(labelsize=7, colors=VERDE)
    ax1_t.set_ylim(0, 450)
    ax1.set_xticks(x)
    ax1.set_xticklabels(MESES, fontsize=7.5)
    ax1.set_ylim(0, 2250)
    ax1.set_title("unity_gold.kpi_mensual — Flujo E/S y Stock Acumulado (Window)", loc="left", fontsize=8.8, color=AZUL)
    ax1.legend(frameon=False, fontsize=7, loc="upper left", ncol=2)
    ax1.spines[["top"]].set_visible(False)
    ax1_t.spines[["top"]].set_visible(False)
    ax1.grid(axis="y", color="#F1F5F9")

    # Visual 2: Costo de entradas e inversión retenida por categoría
    ax2 = fig.add_axes([0.585, 0.41, 0.39, 0.29])
    cats = [r["categoria"] for r in gc][::-1]
    costos_m = [r["costo_entradas"] / 1e6 for r in gc][::-1]
    tpp_c = [r["tpp_dias"] for r in gc][::-1]
    ax2.barh(cats, costos_m, color="#FF3621", alpha=0.85, height=0.6)
    for i, (cm, td) in enumerate(zip(costos_m, tpp_c)):
        ax2.text(cm + 0.3, i, f"S/ {cm:.2f} M  (TPP {td:.2f} d)", va="center", fontsize=7.0, color="#1E293B")
    ax2.set_xlim(0, 25.5)
    ax2.set_xlabel("Costo de entradas movilizado (Millones de S/)", fontsize=7.5)
    ax2.set_title("unity_gold.kpi_categoria — Exposición Financiera y TPP por Categoría", loc="left", fontsize=8.8, color=AZUL)
    ax2.tick_params(labelsize=7.5)
    ax2.spines[["top", "right"]].set_visible(False)
    ax2.grid(axis="x", color="#F1F5F9")

    # Visual 3: Consumo por proyecto crítico
    ax3 = fig.add_axes([0.045, 0.07, 0.43, 0.25])
    proys = [f"{r['codigo_proyecto']} · {r['nombre_proyecto'][:22]}" for r in gp][::-1]
    unids_p = [r["unidades_consumidas"] for r in gp][::-1]
    costos_p = [r["costo_consumido"] / 1e6 for r in gp][::-1]
    ax3.barh(proys, unids_p, color=AZUL, height=0.5)
    for i, (u, cp) in enumerate(zip(unids_p, costos_p)):
        ax3.text(u + 60, i, f"{u:,} u  (S/ {cp:.2f} M)", va="center", fontsize=7.2, fontweight="bold", color=AZUL)
    ax3.set_xlim(0, 6600)
    ax3.set_xlabel("Unidades despachadas", fontsize=7.5)
    ax3.set_title("unity_gold.consumo_proyectos — Demanda y Costo por Proyecto", loc="left", fontsize=8.8, color=AZUL)
    ax3.tick_params(labelsize=7.3)
    ax3.spines[["top", "right"]].set_visible(False)
    ax3.grid(axis="x", color="#F1F5F9")

    # Visual 4: Cumplimiento del Plan Mensual (VM Entradas y Salidas)
    ax4 = fig.add_axes([0.545, 0.07, 0.43, 0.25])
    vme = [r["vm_entradas_pct"] for r in gm]
    vms = [r["vm_salidas_pct"] for r in gm]
    ax4.bar(x - 0.18, vme, 0.36, color=VERDE, label="VM Entradas (%)")
    ax4.bar(x + 0.18, vms, 0.36, color=AZUL2, label="VM Salidas (%)")
    ax4.axhline(0, color="#64748B", lw=0.9)
    ax4.axhspan(-10, 10, color=VERDE, alpha=0.06)
    ax4.set_xticks(x)
    ax4.set_xticklabels(MESES, fontsize=7.5)
    ax4.set_ylim(-6, 6)
    ax4.set_ylabel("Variación %", fontsize=7.5)
    ax4.set_title("unity_gold.kpi_mensual — Variación Mensual frente al Plan (Meta ±10 %)", loc="left", fontsize=8.8, color=AZUL)
    ax4.legend(frameon=False, fontsize=7, ncol=2, loc="upper left")
    ax4.spines[["top", "right"]].set_visible(False)
    ax4.grid(axis="y", color="#F1F5F9")

    guardar(fig, EVID_BD / "fig_bd03_dashboard_databricks.png")


# =============================================================================
# 5. Evidencia Visual de Consultas Analíticas con Databricks Genie (IA)
# =============================================================================
def gen_genie_ia():
    fig, ax = plt.subplots(figsize=(11.6, 5.2))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 52)
    ax.axis("off")
    fig.patch.set_facecolor("#F8FAFC")

    ax.add_patch(FancyBboxPatch((1.5, 1.5), 97, 49, boxstyle="round,pad=0.004,rounding_size=0.5",
                                facecolor="white", edgecolor="#CBD5E1", lw=1.2))
    ax.add_patch(Rectangle((1.5, 44.5), 97, 6.0, facecolor="#1B3139"))
    ax.text(4, 47.5, "MAQUETA · consulta propuesta para Genie — no es una captura ni una respuesta de IA",
            color="white", fontsize=10.5, fontweight="bold", va="center")

    # Pregunta usuario
    caja(ax, 5, 36.8, 90, 5.6,
         "Pregunta: ¿Qué categoría tecnológica concentra la mayor inversión retenida y cuál es su rotación y TPP en 2026?",
         "", color="#EEF2FF", tc="#1E1B4B", fs=8.0, ec="#C7D2FE", lw=1)

    # Respuesta Genie + SQL generado
    ax.add_patch(FancyBboxPatch((4, 5.0), 53, 29.8, boxstyle="round,pad=0.004,rounding_size=0.4",
                                facecolor="#F8FAFC", edgecolor="#E2E8F0", lw=1))
    ax.text(5.5, 32.2, "Ejemplo de consulta SQL propuesta (no generada por Genie):",
            fontsize=7.8, fontweight="bold", color=MORADO)
    sql_txt = (
        "SELECT categoria, costo_entradas, inversion_retenida,\n"
        "       ROUND(costo_entradas/SUM(costo_entradas) OVER()*100,1) AS pct,\n"
        "       rotacion_flujo, tpp_dias\n"
        "FROM unity_gold.kpi_categoria\n"
        "ORDER BY inversion_retenida DESC;"
    )
    ax.add_patch(Rectangle((5.5, 20.5), 50, 10.0, facecolor="#0F172A"))
    ax.text(6.5, 25.5, sql_txt, family="DejaVu Sans Mono", fontsize=6.5, color="#E2E8F0", va="center", linespacing=1.35)

    hallazgo = (
        "Interpretación manual de resultados Spark locales:\n"
        "• Energía lidera la inversión retenida con S/ 307,500 (37.9 % del saldo)\n"
        "  y S/ 19.27 millones en entradas (45.1 % del valor movilizado).\n"
        "• Su rotación es 0.9838 (en meta 0.95–1.05) y su TPP es 3.45 días.\n"
        "• Siguen Almacenamiento (S/ 122,800) e Infraestructura (S/ 119,850)."
    )
    ax.text(5.5, 12.2, hallazgo, fontsize=7.1, color="#1E293B", va="center", linespacing=1.42)

    # Mini tabla de salida a la derecha
    ax.add_patch(FancyBboxPatch((59, 5.0), 37.0, 29.8, boxstyle="round,pad=0.004,rounding_size=0.4",
                                facecolor="white", edgecolor="#E2E8F0", lw=1))
    ax.text(60.5, 32.2, "Datos locales por categoría:", fontsize=7.9, fontweight="bold", color=AZUL)
    headers = ["Categoría", "Inv. Retenida", "% Valor", "TPP"]
    xs = [60.5, 73.5, 84.5, 91.0]
    for xh, h in zip(xs, headers):
        ax.text(xh, 28.8, h, fontsize=7.2, fontweight="bold", color=AZUL2)
    ax.plot([60.0, 95.0], [27.6, 27.6], color="#CBD5E1", lw=0.9)
    for idx, r in enumerate(sorted(MED["gold_categoria"], key=lambda r: r["inversion_retenida"], reverse=True)):
        yy = 25.3 - idx * 2.8
        pct_v = r["costo_entradas"] / 42772640.0 * 100
        ax.text(xs[0], yy, r["categoria"], fontsize=7.1, color="#1E293B")
        ax.text(xs[1], yy, f"S/ {r['inversion_retenida']:,.0f}", fontsize=7.1, color="#1E293B")
        ax.text(xs[2], yy, f"{pct_v:.1f} %", fontsize=7.1, color="#1E293B")
        ax.text(xs[3], yy, f"{r['tpp_dias']:.2f} d", fontsize=7.1, color=VERDE, fontweight="bold")

    guardar(fig, EVID_BD / "fig_bd04_genie_consulta_ia.png")


# =============================================================================
# 6. Los 10 Gráficos de Experimentación Individual (5 BI + 5 Big Data)
# =============================================================================
def gen_graficos_experimentos():
    bi_list = EXP_BI["experimentos"]
    bd_list = EXP_BD["experimentos"]

    # --- EXP-BI-01 (Johana): OLTP vs Estrella vs Estrella Indexada ---
    e = bi_list[0]
    fig, ax = plt.subplots(figsize=(8.6, 4.0))
    nombres = ["E1: OLTP 3FN\n(7 JOINs)", "E2: Modelo Estrella\n(3 JOINs sin índice)", "E3: Estrella Kimball\n+ Índices Cobertura"]
    vals = [s["mediana_ms"] for s in e["escenarios"]]
    stds = [s["std_ms"] for s in e["escenarios"]]
    sp = [s["speedup_vs_e1"] for s in e["escenarios"]]
    bars = ax.bar(nombres, vals, yerr=stds, capsize=5, color=[ROJO, AZUL2, VERDE], width=0.5)
    for b, v, s_i in zip(bars, vals, sp):
        ax.text(b.get_x() + b.get_width() / 2, v + 4.5, f"{v:.2f} ms\n({s_i:.2f}x)", ha="center", fontsize=8, fontweight="bold")
    ax.set_ylim(0, max(vals) * 1.32)
    ax.set_ylabel("Latencia mediana de consulta (ms)")
    ax.set_title("EXP-BI-01 (Quispe J.) — SQLite local: 3FN simulado vs. Estrella (26,740 filas)", loc="left")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#E2E8F0"); ax.set_axisbelow(True)
    guardar(fig, EVID_EXP / "fig_exp_bi_01_johana.png")

    # --- EXP-BI-02 (Leoneld): Inyección de defectos de calidad ---
    e = bi_list[1]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10.2, 3.9))
    noms = ["E1: Limpio\n(0 defectos)", "E2: Moderado\n(+100 defectos)", "E3: Severo\n(+540 defectos)"]
    carg = [s["registros_validos_cargados"] for s in e["escenarios"]]
    rech = [s["registros_rechazados"] for s in e["escenarios"]]
    x = np.arange(3)
    a1.bar(x, carg, 0.5, label="Válidos cargados (5,348)", color=VERDE)
    a1.bar(x, rech, 0.5, bottom=carg, label="Defectos rechazados (100 % detectados)", color=ROJO)
    for xi, (c, r) in enumerate(zip(carg, rech)):
        a1.text(xi, c + r + 110, f"Rech: {r}", ha="center", fontsize=7.8, fontweight="bold", color=ROJO if r else GRIS)
    a1.set_xticks(x); a1.set_xticklabels(noms, fontsize=8)
    a1.set_ylim(0, 6800); a1.set_ylabel("Registros")
    a1.set_title("Aislamiento de defectos RQ-01..RQ-04", loc="left")
    a1.legend(frameon=False, fontsize=7.5)
    a1.spines[["top", "right"]].set_visible(False)

    comp = [s["completitud_lote_pct"] for s in e["escenarios"]]
    lat = [s["latencia_validacion_ms"] for s in e["escenarios"]]
    a2.plot(noms, comp, marker="o", lw=2.2, color=AZUL, label="Completitud del lote (%)")
    a2.axhline(98.0, color=ROJO, ls="--", lw=1.1, label="Umbral RNF-04 (98 %)")
    for xi, (cp, lt) in enumerate(zip(comp, lat)):
        a2.text(xi, cp + 1.0, f"{cp:.2f} %\n({lt:.1f} ms)", ha="center", fontsize=7.5, color=AZUL)
    a2.set_ylim(86, 104); a2.set_ylabel("Completitud (%)")
    a2.set_title("Activación de alerta RNF-04 y TPP invariante (3.8687 d)", loc="left")
    a2.legend(frameon=False, fontsize=7.5, loc="lower left")
    a2.spines[["top", "right"]].set_visible(False); a2.grid(axis="y", color="#E2E8F0")
    fig.suptitle("EXP-BI-02 (Sotelo L.) — Robustez del ETL ante Inyección Controlada de Defectos",
                 fontsize=10.8, fontweight="bold", color=AZUL, y=1.02)
    guardar(fig, EVID_EXP / "fig_exp_bi_02_leoneld.png")

    # --- EXP-BI-03 (Camila): Medidas DAX / Tabulares ---
    e = bi_list[2]
    fig, ax = plt.subplots(figsize=(8.8, 4.0))
    noms = ["E1: Iteración pandas\n(análogo FILTER + SUMX)", "E2: Banderas pandas\n(análogo CALCULATE)", "E3: Cubo Pre-agregado\n(Mes × Categoría)"]
    vals = [s["mediana_ms"] for s in e["escenarios"]]
    bars = ax.bar(noms, vals, color=[ROJO, VERDE, AZUL2], width=0.48)
    ax.set_yscale("log")
    ax.axhline(3000, color=ROJO, ls="--", lw=1.1, label="Límite máximo RNF-01 (3,000 ms)")
    for b, v, s in zip(bars, vals, e["escenarios"]):
        ax.text(b.get_x() + b.get_width() / 2, v * 1.38, f"{v:.2f} ms\n({s['speedup_vs_e1']}x)", ha="center", fontsize=7.8, fontweight="bold")
    ax.set_ylim(0.05, 15000)
    ax.set_ylabel("Tiempo de evaluación en escala log (ms)")
    ax.set_title("EXP-BI-03 (Montalvo C.) — Evaluación de Medidas Analíticas sobre 53,480 Movimientos", loc="left")
    ax.legend(frameon=False, loc="upper right")
    ax.spines[["top", "right"]].set_visible(False); ax.grid(axis="y", color="#E2E8F0", which="major")
    guardar(fig, EVID_EXP / "fig_exp_bi_03_camila.png")

    # --- EXP-BI-04 (Mishell): Escalamiento FIFO BI ---
    e = bi_list[3]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10.2, 3.9))
    movs = [s["movimientos"] for s in e["escenarios"]]
    tms = [s["mediana_ms"] for s in e["escenarios"]]
    thr = [s["throughput_mov_s"] / 1e6 for s in e["escenarios"]]
    a1.plot(movs, tms, marker="o", lw=2.2, color=AZUL2)
    for m, t in zip(movs, tms):
        a1.annotate(f"{t:.1f} ms", (m, t), xytext=(0, 8), textcoords="offset points", ha="center", fontsize=7.5)
    a1.set_xlabel("Movimientos procesados"); a1.set_ylabel("Tiempo mediano (ms)")
    a1.set_ylim(0, max(tms) * 1.25)
    a1.set_title("Crecimiento lineal O(N) del algoritmo FIFO", loc="left")
    a1.spines[["top", "right"]].set_visible(False); a1.grid(color="#E2E8F0")

    etq = [f"x{s['factor']}\n({s['movimientos']//1000}k)" for s in e["escenarios"]]
    a2.bar(etq, thr, color=VERDE, width=0.5)
    for i, th in enumerate(thr):
        a2.text(i, th + 0.05, f"{th:.2f} M/s", ha="center", fontsize=7.6, fontweight="bold")
    a2.set_ylim(0, 2.5); a2.set_ylabel("Throughput (Millones mov/s)")
    a2.set_title("Throughput sostenido e invarianza TPP = 3.8687 d", loc="left")
    a2.spines[["top", "right"]].set_visible(False); a2.grid(axis="y", color="#E2E8F0")
    fig.suptitle("EXP-BI-04 (Arroyo M.) — Escalamiento Volumétrico del Cálculo de Permanencia FIFO en BI",
                 fontsize=10.8, fontweight="bold", color=AZUL, y=1.02)
    guardar(fig, EVID_EXP / "fig_exp_bi_04_mishell.png")

    # --- EXP-BI-05 (Diego): Optimización de Carga Transaccional ---
    e = bi_list[4]
    fig, ax = plt.subplots(figsize=(8.8, 4.0))
    noms = ["E1: Autocommit por fila\n(5,348 transacciones)", "E2: pandas to_sql\n(chunk=500, 1 transacción)", "E3: Batch executemany\n(1 transacción atómica)"]
    vals = [s["mediana_ms"] for s in e["escenarios"]]
    bars = ax.bar(noms, vals, color=[ROJO, AZUL2, VERDE], width=0.48)
    ax.set_yscale("log")
    for b, v, s in zip(bars, vals, e["escenarios"]):
        ax.text(b.get_x() + b.get_width() / 2, v * 1.32, f"{v:.1f} ms\n({s['speedup_vs_e1']}x · {int(s['filas_por_segundo']):,} filas/s)",
                ha="center", fontsize=7.6, fontweight="bold")
    ax.set_ylim(5, 12000)
    ax.set_ylabel("Tiempo de escritura en disco (ms, escala log)")
    ax.set_title("EXP-BI-05 (Cordova D.) — Optimización de Carga Transaccional de Fact_Movimiento", loc="left")
    ax.spines[["top", "right"]].set_visible(False); ax.grid(axis="y", color="#E2E8F0", which="major")
    guardar(fig, EVID_EXP / "fig_exp_bi_05_diego.png")

    # --- EXP-BD-01 (Johana): CSV vs JSON vs Parquet Snappy ---
    e = bd_list[0]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10.4, 3.9))
    noms = ["CSV\n(Texto plano)", "JSON\n(Semiestructurado)", "Parquet + Snappy\n(Columnar Medallion)"]
    sizes = [s["tamano_disco_kb"] / 1024.0 for s in e["escenarios"]]
    reads = [s["lectura_mediana_ms"] for s in e["escenarios"]]
    b1 = a1.bar(noms, sizes, color=[AZUL2, AMBAR, VERDE], width=0.5)
    for b, sz in zip(b1, sizes):
        a1.text(b.get_x() + b.get_width() / 2, sz + 1.2, f"{sz:.2f} MB", ha="center", fontsize=7.8, fontweight="bold")
    a1.set_ylim(0, max(sizes) * 1.22); a1.set_ylabel("Tamaño en almacenamiento (MB)")
    a1.set_title("Huella en disco (106,960 filas): −98.8 % en Parquet", loc="left")
    a1.spines[["top", "right"]].set_visible(False); a1.grid(axis="y", color="#E2E8F0")

    b2 = a2.bar(noms, reads, color=[AZUL2, AMBAR, VERDE], width=0.5)
    for b, rd, s in zip(b2, reads, e["escenarios"]):
        a2.text(b.get_x() + b.get_width() / 2, rd + 28, f"{rd:.1f} ms\n({s['speedup_lectura_vs_csv']}x)", ha="center", fontsize=7.8, fontweight="bold")
    a2.set_ylim(0, max(reads) * 1.26); a2.set_ylabel("Latencia de consulta selectiva (ms)")
    a2.set_title("Lectura con Predicate Pushdown + Column Pruning", loc="left")
    a2.spines[["top", "right"]].set_visible(False); a2.grid(axis="y", color="#E2E8F0")
    fig.suptitle("EXP-BD-01 (Quispe J.) — Evaluación local Spark de formatos de persistencia",
                 fontsize=10.8, fontweight="bold", color=AZUL, y=1.02)
    guardar(fig, EVID_EXP / "fig_exp_bd_01_johana.png")

    # --- EXP-BD-02 (Leoneld): Python UDF vs Spark SQL vs API Nativa ---
    e = bd_list[1]
    fig, ax = plt.subplots(figsize=(8.8, 4.0))
    noms = ["E1: Python UDF\n(Serialización Py4J)", "E2: Spark SQL\n(CASE WHEN Catalyst)", "E3: API DataFrames\n(F.when JVM Nativo)"]
    vals = [s["mediana_ms"] for s in e["escenarios"]]
    stds = [s["std_ms"] for s in e["escenarios"]]
    bars = ax.bar(noms, vals, yerr=stds, capsize=5, color=[ROJO, VERDE, AZUL2], width=0.48)
    for b, v, s in zip(bars, vals, e["escenarios"]):
        ax.text(b.get_x() + b.get_width() / 2, v + 45, f"{v:.1f} ms ({s['speedup_vs_udf']}x)\n{int(s['throughput_filas_s']):,} filas/s",
                ha="center", fontsize=7.8, fontweight="bold")
    ax.set_ylim(0, max(vals) * 1.35)
    ax.set_ylabel("Tiempo mediano de transformación (ms)")
    ax.set_title("EXP-BD-02 (Sotelo L.) — Transformación en Capa Silver sobre 160,440 Registros", loc="left")
    ax.spines[["top", "right"]].set_visible(False); ax.grid(axis="y", color="#E2E8F0")
    guardar(fig, EVID_EXP / "fig_exp_bd_02_leoneld.png")

    # --- EXP-BD-03 (Camila): Partition Pruning + Cache ---
    e = bd_list[2]
    fig, ax = plt.subplots(figsize=(8.8, 4.0))
    noms = ["E1: CSV Crudo sin Partición\n(Full Scan × 4 visuales)", "E2: Parquet Particionado\n(Partition Pruning mes=9)", "E3: Partición + Caché\n(MEMORY_AND_DISK)"]
    vals = [s["mediana_ms"] for s in e["escenarios"]]
    bars = ax.bar(noms, vals, color=[ROJO, AZUL2, VERDE], width=0.48)
    for b, v, s in zip(bars, vals, e["escenarios"]):
        ax.text(b.get_x() + b.get_width() / 2, v + 110, f"{v:.1f} ms\n({s['speedup_vs_e1']}x más rápido)",
                ha="center", fontsize=7.8, fontweight="bold")
    ax.set_ylim(0, max(vals) * 1.25)
    ax.set_ylabel("Latencia acumulada de 4 consultas (ms)")
    ax.set_title("EXP-BD-03 (Montalvo C.) — Consultas secuenciales simuladas (427,840 filas)", loc="left")
    ax.spines[["top", "right"]].set_visible(False); ax.grid(axis="y", color="#E2E8F0")
    guardar(fig, EVID_EXP / "fig_exp_bd_03_camila.png")

    # --- EXP-BD-04 (Mishell): Escalamiento Big Data hasta 3.11M registros ---
    e = bd_list[3]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10.4, 3.9))
    etq = [f"x{s['escala']}\n({s['registros_totales']/1e6:.2f}M)" if s["registros_totales"] >= 1e6 else f"x{s['escala']}\n({s['registros_totales']//1000}k)"
           for s in e["escenarios"]]
    lats = [s["mediana_ms"] for s in e["escenarios"]]
    thrs = [s["throughput_reg_s"] / 1e6 for s in e["escenarios"]]
    a1.plot(etq, lats, marker="o", lw=2.2, color=AZUL)
    for i, l in enumerate(lats):
        a1.text(i, l + 15, f"{l:.1f} ms", ha="center", fontsize=7.8, fontweight="bold", color=AZUL)
    a1.set_ylim(0, max(lats) * 1.28); a1.set_ylabel("Latencia total del job Spark (ms)")
    a1.set_title("Tiempo observado por escala (una ejecución local)", loc="left")
    a1.spines[["top", "right"]].set_visible(False); a1.grid(color="#E2E8F0")

    b2 = a2.bar(etq, thrs, color=AMBAR, width=0.5)
    for b, th in zip(b2, thrs):
        a2.text(b.get_x() + b.get_width() / 2, th + 0.25, f"{th:.2f} M/s", ha="center", fontsize=7.8, fontweight="bold")
    a2.set_ylim(0, max(thrs) * 1.25); a2.set_ylabel("Throughput (Millones de registros/s)")
    a2.set_title("Throughput aparente por escala", loc="left")
    a2.spines[["top", "right"]].set_visible(False); a2.grid(axis="y", color="#E2E8F0")
    fig.suptitle("EXP-BD-04 (Arroyo M.) — Escalamiento Volumétrico en PySpark hasta 3,117,200 Registros",
                 fontsize=10.8, fontweight="bold", color=AZUL, y=1.02)
    guardar(fig, EVID_EXP / "fig_exp_bd_04_mishell.png")

    # --- EXP-BD-05 (Diego): Shuffle Partitions y BroadcastHashJoin ---
    e = bd_list[4]
    fig, ax = plt.subplots(figsize=(9.2, 4.1))
    noms = [
        "E1: SortMergeJoin\n(200 part. Default)",
        "E2: SortMergeJoin\n(64 particiones)",
        "E3: SortMergeJoin\n(4 particiones)",
        "E4: BroadcastHashJoin\n+ AQE (4 particiones)",
    ]
    vals = [s["mediana_ms"] for s in e["escenarios"]]
    bars = ax.bar(noms, vals, color=[ROJO, AMBAR, AZUL2, VERDE], width=0.5)
    for b, v, s in zip(bars, vals, e["escenarios"]):
        ax.text(b.get_x() + b.get_width() / 2, v + 75, f"{v:.1f} ms\n({s['speedup_vs_default']}x)",
                ha="center", fontsize=7.8, fontweight="bold")
    ax.set_ylim(0, max(vals) * 1.24)
    ax.set_ylabel("Latencia mediana del Join + Agregación Gold (ms)")
    ax.set_title("EXP-BD-05 (Cordova D.) — Optimización de Shuffle Partitions y Broadcast Join (213,920 filas)", loc="left")
    ax.spines[["top", "right"]].set_visible(False); ax.grid(axis="y", color="#E2E8F0")
    guardar(fig, EVID_EXP / "fig_exp_bd_05_diego.png")


if __name__ == "__main__":
    print("Generando figuras integradas, Big Data y experimentos...")
    gen_arquitectura_integrada()
    gen_arquitectura_bigdata_medallion()
    gen_eda_spark()
    gen_dashboard_databricks()
    gen_genie_ia()
    gen_graficos_experimentos()
    print("Todas las figuras fueron generadas exitosamente.")
