# Guía de ejecución y capturas por integrante

Proyecto Unity Perú SAC · NRC 26055 · 2026-2

**Estado de evidencia:** las cifras del repositorio proceden de ejecuciones locales de Python/SQLite y PySpark, no de Databricks o Power BI. Las imágenes BD03 y BD04 son maquetas, no capturas. Las asignaciones son propuestas: cada estudiante debe ejecutar, registrar fecha/entorno y conservar sus propias capturas. No firmar ni afirmar una actividad no realizada.

## Preparación compartida
1. Clonar el repositorio y seguir `REPLICACION.md`. Las corridas BI se hacen sobre archivos CSV con Python y SQLite; no requieren SQL Server. La solución BI real se reconstruye con SQL Server/ETL/Power BI siguiendo la guía.
2. Crear entorno Python e instalar dependencias; ejecutar `python bigdata/pipeline_medallion_spark.py` y comprobar `bigdata/salidas/medallion_resultados.json`. Esta versión local usa Parquet, no Delta.
3. En Databricks clonar el repositorio como carpeta Git y abrir los notebooks 01–04. Subir todos los archivos `powerbi/respaldo/*.csv` y `datos/*.txt` a un Volume accesible y editar `BASE_DATOS` y `BASE_RESPALDO` en notebook 01. La plataforma, permisos, catálogo y costo dependen del entorno. Ejecutar 01→02→03 con acceso a crear esquemas/tablas Delta. No se ha validado la ejecución en Databricks.
4. Crear el dashboard manualmente en Databricks SQL / AI/BI con `unity_gold.kpi_mensual`, `unity_gold.kpi_categoria` y `unity_gold.consumo_proyectos`. Para Genie, crear un espacio con acceso a Gold y SQL Warehouse, si está disponible; contrastar cada respuesta con SQL y registrar limitaciones.
5. Para experimentos ejecutar `python experimentos/bi/ejecutar_experimentos_bi.py` y `python experimentos/bigdata/ejecutar_experimentos_bigdata.py`. Son suites completas y sobrescriben JSON y gráficos si se regeneran. Respaldar resultados antes de ejecutar. En Databricks, ajustar `REPO` en el notebook 04 y ejecutar solo la sección asignada; los archivos del repositorio deben ser accesibles al driver. Las pruebas usan `/tmp` local: requieren un cómputo de un solo nodo con filesystem local accesible; adaptar almacenamiento y medición antes de usar multinodo o serverless.
6. En cada captura incluir la aplicación, fecha/hora o log, escenario y resultado; ocultar credenciales y datos personales. Guardar PNG sin recortes engañosos. Mantener métricas, configuración y hashes asociados a la ejecución; no copiar capturas de otro miembro.

## 1. Quispe Ruiz, Johana Carolina

### EXP-BI-01 — BI
Evaluación de latencia analítica: Esquema OLTP Normalizado (3FN) vs. Modelo Estrella vs. Estrella con Índices de Cobertura

1. Revisar el script `experimentos/bi/ejecutar_experimentos_bi.py`, función `exp_bi_01_*`.
2. Desde raíz ejecutar la suite BI; localizar `EXP-BI-01` en `experimentos/salidas/resultados_experimentos_bi.json`.
3. Comparar todos los escenarios y guardar `CAP-JOHANA-BI-01.png` (código/configuración) y `CAP-JOHANA-BI-02.png` (resultados). Capturar SQL en el script, los tres tiempos y diagrama del modelo; son consultas en SQLite, no mediciones SQL Server.
4. El gráfico generado está en `experimentos/evidencias/fig_exp_bi_01_johana.png`.

### EXP-BD-01 — Big Data
Evaluación de formatos de persistencia en la Arquitectura Medallion: CSV vs. JSON vs. Parquet Columnar (Snappy)

1. Revisar la función `exp_bd_01_*` en `experimentos/bigdata/ejecutar_experimentos_bigdata.py`.
2. Ejecutar la suite local o la celda correspondiente del notebook 04 en Databricks, tras configurar `REPO`.
3. Guardar `CAP-JOHANA-BD-01.png` (código/configuración) y `CAP-JOHANA-BD-02.png` (resultado por escenario). Comparar tamaños y lecturas CSV/JSON/Parquet; capturar los tres resultados y rutas de archivos.
4. Comparar con `experimentos/salidas/resultados_experimentos_bigdata.json` y `experimentos/evidencias/fig_exp_bd_01_johana.png`; los tiempos no tienen por qué coincidir.

**Registro individual a completar:** fecha/hora; versión o commit; SO y CPU; versión de Spark/Python; volumen; configuración; repeticiones; resultados, interpretación, archivos y firma del integrante. Documentar cualquier discrepancia.

## 2. Sotelo Espinoza, Leoneld Mathias

### EXP-BI-02 — BI
Evaluación de robustez y sensibilidad de las reglas de calidad del ETL ante inyección controlada de anomalías de datos

1. Revisar el script `experimentos/bi/ejecutar_experimentos_bi.py`, función `exp_bi_02_*`.
2. Desde raíz ejecutar la suite BI; localizar `EXP-BI-02` en `experimentos/salidas/resultados_experimentos_bi.json`.
3. Comparar todos los escenarios y guardar `CAP-LEONELD-BI-01.png` (código/configuración) y `CAP-LEONELD-BI-02.png` (resultados). Capturar la inyección de defectos, tabla de tres escenarios y CSV de rechazados; son reglas experimentales, no una ejecución del ETL de producción.
4. El gráfico generado está en `experimentos/evidencias/fig_exp_bi_02_leoneld.png`.

### EXP-BD-02 — Big Data
Optimización de transformaciones de limpieza y reglas de negocio en Capa Silver: Python UDF vs. Spark SQL vs. Expresiones Nativas Catalyst

1. Revisar la función `exp_bd_02_*` en `experimentos/bigdata/ejecutar_experimentos_bigdata.py`.
2. Ejecutar la suite local o la celda correspondiente del notebook 04 en Databricks, tras configurar `REPO`.
3. Guardar `CAP-LEONELD-BD-01.png` (código/configuración) y `CAP-LEONELD-BD-02.png` (resultado por escenario). Capturar código UDF, CASE WHEN y F.when, sus resultados y planes explain; verificar cada conteo por clase: el JSON actual solo registra el total de filas y no prueba equivalencia por clase.
4. Comparar con `experimentos/salidas/resultados_experimentos_bigdata.json` y `experimentos/evidencias/fig_exp_bd_02_leoneld.png`; los tiempos no tienen por qué coincidir.

**Registro individual a completar:** fecha/hora; versión o commit; SO y CPU; versión de Spark/Python; volumen; configuración; repeticiones; resultados, interpretación, archivos y firma del integrante. Documentar cualquier discrepancia.

## 3. Montalvo Abrego, Camila Nadine

### EXP-BI-03 — BI
Optimización de tiempos de respuesta en medidas del modelo analítico: Iteradores de fila vs. Filtro por banderas booleanas vs. Cubo pre-agregado

1. Revisar el script `experimentos/bi/ejecutar_experimentos_bi.py`, función `exp_bi_03_*`.
2. Desde raíz ejecutar la suite BI; localizar `EXP-BI-03` en `experimentos/salidas/resultados_experimentos_bi.json`.
3. Comparar todos los escenarios y guardar `CAP-CAMILA-BI-01.png` (código/configuración) y `CAP-CAMILA-BI-02.png` (resultados). Capturar iteración Python, operación vectorizada y cubo, con sus tres tiempos; son análogos y no mediciones DAX.
4. El gráfico generado está en `experimentos/evidencias/fig_exp_bi_03_camila.png`.

### EXP-BD-03 — Big Data
Aceleración de consultas interactivas para Dashboards en Databricks y Genie: Parquet Plano vs. Partition Pruning por Mes vs. Caché en Memoria

1. Revisar la función `exp_bd_03_*` en `experimentos/bigdata/ejecutar_experimentos_bigdata.py`.
2. Ejecutar la suite local o la celda correspondiente del notebook 04 en Databricks, tras configurar `REPO`.
3. Guardar `CAP-CAMILA-BD-01.png` (código/configuración) y `CAP-CAMILA-BD-02.png` (resultado por escenario). Capturar código de lectura CSV, Parquet particionado y caché, resultados y plan de partición. Comparar también los valores agregados completos; el resumen actual solo cuenta resultados. Las cuatro consultas son secuenciales, no usuarios concurrentes.
4. Comparar con `experimentos/salidas/resultados_experimentos_bigdata.json` y `experimentos/evidencias/fig_exp_bd_03_camila.png`; los tiempos no tienen por qué coincidir.

**Registro individual a completar:** fecha/hora; versión o commit; SO y CPU; versión de Spark/Python; volumen; configuración; repeticiones; resultados, interpretación, archivos y firma del integrante. Documentar cualquier discrepancia.

## 4. Arroyo Julca, Mishell Alexandra Belen

### EXP-BI-04 — BI
Análisis de escalabilidad progresiva e invarianza de KPIs en el algoritmo de trazabilidad FIFO (x1, x5, x10 y x25)

1. Revisar el script `experimentos/bi/ejecutar_experimentos_bi.py`, función `exp_bi_04_*`.
2. Desde raíz ejecutar la suite BI; localizar `EXP-BI-04` en `experimentos/salidas/resultados_experimentos_bi.json`.
3. Comparar todos los escenarios y guardar `CAP-MISHELL-BI-01.png` (código/configuración) y `CAP-MISHELL-BI-02.png` (resultados). Capturar las cuatro escalas, TPP y tiempos en consola o JSON.
4. El gráfico generado está en `experimentos/evidencias/fig_exp_bi_04_mishell.png`.

### EXP-BD-04 — Big Data
Evaluación de escalabilidad masiva del pipeline PySpark ante crecimiento volumétrico (x1 = 31 mil hasta x100 = 3.11 millones de registros)

1. Revisar la función `exp_bd_04_*` en `experimentos/bigdata/ejecutar_experimentos_bigdata.py`.
2. Ejecutar la suite local o la celda correspondiente del notebook 04 en Databricks, tras configurar `REPO`.
3. Guardar `CAP-MISHELL-BD-01.png` (código/configuración) y `CAP-MISHELL-BD-02.png` (resultado por escenario). Capturar el código de escalamiento, las cuatro escalas y resultados; observar que Spark puede optimizar los datos sintéticos.
4. Comparar con `experimentos/salidas/resultados_experimentos_bigdata.json` y `experimentos/evidencias/fig_exp_bd_04_mishell.png`; los tiempos no tienen por qué coincidir.

**Registro individual a completar:** fecha/hora; versión o commit; SO y CPU; versión de Spark/Python; volumen; configuración; repeticiones; resultados, interpretación, archivos y firma del integrante. Documentar cualquier discrepancia.

## 5. Cordova Paz, Diego Jason

### EXP-BI-05 — BI
Optimización de la fase de carga del ETL al Datamart: Autocommit unitario vs. Bloques pandas vs. Carga masiva transaccional (executemany)

1. Revisar el script `experimentos/bi/ejecutar_experimentos_bi.py`, función `exp_bi_05_*`.
2. Desde raíz ejecutar la suite BI; localizar `EXP-BI-05` en `experimentos/salidas/resultados_experimentos_bi.json`.
3. Comparar todos los escenarios y guardar `CAP-DIEGO-BI-01.png` (código/configuración) y `CAP-DIEGO-BI-02.png` (resultados). Capturar escenarios de carga SQLite, tiempos y cantidad de filas; no extrapolar a SQL Server.
4. El gráfico generado está en `experimentos/evidencias/fig_exp_bi_05_diego.png`.

### EXP-BD-05 — Big Data
Optimización de arquitectura de ejecución en Spark: Particiones de Shuffle y BroadcastHashJoin vs. SortMergeJoin en Capa Gold

1. Revisar la función `exp_bd_05_*` en `experimentos/bigdata/ejecutar_experimentos_bigdata.py`.
2. Ejecutar la suite local o la celda correspondiente del notebook 04 en Databricks, tras configurar `REPO`.
3. Guardar `CAP-DIEGO-BD-01.png` (código/configuración) y `CAP-DIEGO-BD-02.png` (resultado por escenario). Capturar configuraciones de particiones y planes físicos; comprobar en Spark UI si el plan usa broadcast y registrar AQE; el JSON actual solo cuenta grupos y no verifica valores ni plan físico.
4. Comparar con `experimentos/salidas/resultados_experimentos_bigdata.json` y `experimentos/evidencias/fig_exp_bd_05_diego.png`; los tiempos no tienen por qué coincidir.

**Registro individual a completar:** fecha/hora; versión o commit; SO y CPU; versión de Spark/Python; volumen; configuración; repeticiones; resultados, interpretación, archivos y firma del integrante. Documentar cualquier discrepancia.

## Capturas grupales adicionales
- `CAP-GRUPO-BRONZE.png`: notebook 01 con conteos y esquema; `CAP-GRUPO-SILVER.png`: notebook 02 con limpieza y reglas.
- `CAP-GRUPO-GOLD.png`: notebook 03 con tablas Gold y conciliación.
- `CAP-GRUPO-DASHBOARD.png`: dashboard real de Databricks con consultas y filtros; `CAP-GRUPO-GENIE.png`: pregunta, SQL generado y comparación manual si se dispone de Genie.
- `CAP-GRUPO-POWERBI.png`: PBIX real con sus tres páginas; `CAP-GRUPO-ETL.png`: consola y log del ETL real. Estas capturas están pendientes hasta que el equipo las obtenga.
