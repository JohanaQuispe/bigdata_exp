# Guion de defensa — Unity Perú SAC: BI + Big Data

## Presentación sugerida (5 minutos)

1. **Problema y objetivo.** Integrar movimientos y fuentes complementarias para apoyar decisiones de almacén con indicadores confiables y evaluaciones reproducibles.
2. **Solución BI.** Explicar el grano de `Fact_Movimiento`, dimensiones, reglas ETL y vistas reales del PBIX. Mostrar los gráficos de BI existentes sin añadir un análisis independiente de anomalías.
3. **Solución Big Data.** Mostrar el diagrama Medallion: ingesta Bronze, limpieza Silver y agregados Gold. Explicar `select`, `filter`, `na`, `groupBy` y el flujo local Parquet. Proponer el despliegue Delta y dashboard en Databricks, sin afirmar que se realizó.
4. **Resultados.** Mostrar `bigdata/salidas/medallion_resultados.json`: 5 348 movimientos, 15 425 entradas, 15 078 salidas y TPP 3,868701; la conciliación es contra respaldo BI, no una consulta en vivo.
5. **Experimentos y límites.** Cada integrante explica sus dos pruebas, condiciones, al menos tres escenarios, resultados, gráfico y límites. Los datos ampliados son sintéticos; SQLite/pandas son aproximaciones, no SQL Server/DAX. Una correlación de parámetros no prueba causalidad.
6. **Cierre y ética.** Señalar validaciones pendientes en Databricks, aprobación de integrantes y declaración de IA. Las maquetas de dashboard y Genie no son capturas reales.

## Demostración sugerida (10 minutos)

- 2 min: abrir el PBIX y recorrer las páginas gerencial, operativa y proyectos.
- 2 min: mostrar ETL, log de corrida anterior y evidencia de calidad; si hay SQL Server disponible, ejecutar conforme a `REPLICACION.md` sin mostrar credenciales.
- 2 min: mostrar pipeline Spark local, resultados Gold y conciliación; o demostrar notebooks en Databricks si el equipo ya los ejecutó.
- 3 min: cinco integrantes presentan BI y Big Data, escenarios y capturas propias conforme a `GUIA_CAPTURAS_Y_EXPERIMENTOS_POR_INTEGRANTE.md`.
- 1 min: límites, recomendaciones, uso de IA y retroalimentación.

## Asignaciones

| Integrante | BI | Big Data |
|---|---|---|
| Johana Quispe | 01 · SQLite y modelos | 01 · formatos |
| Leoneld Sotelo | 02 · calidad | 02 · transformaciones |
| Camila Montalvo | 03 · patrones pandas análogos a DAX | 03 · formato, partición y caché |
| Mishell Arroyo | 04 · escalas FIFO | 04 · escalas Spark sintéticas |
| Diego Cordova | 05 · carga SQLite | 05 · joins y particiones |

**Antes de defender:** cada participante debe ejecutar y revisar su trabajo, guardar capturas, verificar fechas y valores de su corrida. No presentar como real una captura o resultado que no se produjo.
