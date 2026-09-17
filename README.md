# Trabajo Final Big_DATA MODELO ENERGÉTICO ARAGÓN
Especialización en Análisis de Datos · Big Data · UNAULA
Valentina Mesa Álvarez · Ricardo Builes Escobar · septiembre de 2026
# 1. Resumen
Proyecto de Big Data desarrollado en Databricks para predecir el consumo energético de edificios de Aragón a partir de variables disponibles antes de una certificación. Se implementó una arquitectura Medallion con Delta Lake y Unity Catalog, pipeline idempotente de ingesta, reglas de calidad con cuarentena, validación de invariantes y trazabilidad mediante MLflow. Con 192.145 registros, el modelo de Gradient Boosting obtuvo MAE de 71,71 kWh/m²·año, RMSE de 99,42 y R² de 0,294, mejorando en 18,6 % la línea base por zona climática. La clasificación derivada alcanzó 59,2 % de precisión y 33,1 % de recall, por lo que el modelo se plantea como herramienta de priorización de inspecciones. El proyecto documenta además tratamiento de datos faltantes, control de fuga de datos, decisiones de diseño, correcciones del pipeline, visualización en Databricks/Power BI y pasos de reproducibilidad.
# 2. Planteamiento del Problema
El registro de Aragón es un censo de trámites administrativos y no un censo completo del parque edificado: solo aparecen inmuebles que han sido certificados, normalmente asociados a operaciones de venta o alquiler. Esto introduce un sesgo hacia el mercado inmobiliario activo.
La variable objetivo es el consumo de energía primaria no renovable, expresado en kWh/m²·año. Las variables utilizadas para predecirlo son aquellas disponibles antes de una visita técnica: año de construcción, superficie útil, tipo de edificio, provincia y zona climática. La zona climática tuvo que derivarse a partir de la provincia y el municipio porque no venía directamente en el dataset.
# 3. Arquitectura y Pipeline
La solución implementa una arquitectura Medallion en Databricks con cuatro etapas principales: ingesta Bronze, transformación y calidad Silver, validación de invariantes y construcción de features Gold. Unity Catalog proporciona gobierno, permisos, linaje y auditoría; Delta Lake aporta almacenamiento transaccional.
Bronze conserva el archivo original intacto y lee todas las columnas como texto para evitar pérdidas silenciosas, especialmente con valores que utilizan coma decimal. Cada fila incorpora información de linaje, incluyendo lote, archivo de origen, huella SHA-256 e instante de carga.
El pipeline es idempotente mediante control de lotes por huella del archivo. La ejecución se orquesta con cuatro tareas: 01_bronze_ingestion, 02_silver_transformation, 03_validate_silver y 04_gold_features. Si la validación falla, el flujo se detiene antes de actualizar Gold.
<img width="953" height="668" alt="image" src="https://github.com/user-attachments/assets/3243fc3c-81ae-411b-9e07-4738abdfcf74" />
Figura — Historial del job CEE_Aragon_ETL_Pipeline y comportamiento de la validación.
# 4. Calidad de los datos
Las filas que incumplen reglas no se eliminan: se envían a una tabla de cuarentena con el identificador de la regla que provocó el rechazo. Se aplicaron reglas sobre año de construcción, año de emisión, superficie, emisiones de CO₂ y consumo, incluyendo validaciones de conversión numérica.
La ingesta quedó reconciliada exactamente: 189.441 registros válidos + 2.704 en cuarentena = 192.145 registros de origen. Además, los 76 inmuebles sin letra de clasificación se conservaron porque el consumo objetivo sí estaba disponible; el guion de clasificación se normalizó a nulo.
<img width="953" height="534" alt="image" src="https://github.com/user-attachments/assets/7a582cd0-9292-4830-8ada-d94fc5d20b34" />
Figura — Reglas y distribución de filas rechazadas por calidad.
<img width="953" height="381" alt="image" src="https://github.com/user-attachments/assets/bc1ff40d-caf0-45e3-96f4-e84edae3852e" />
Figura — Reconciliación de registros de origen, válidos y en cuarentena
# 5. Modelo y Resultados
Se separaron 151.640 registros para entrenamiento y 37.801 para prueba desde el principio, manteniendo el conjunto de prueba fuera del entrenamiento. La línea base fue el consumo medio por zona climática, con MAE de 88,09 kWh/m²·año.
<img width="842" height="187" alt="image" src="https://github.com/user-attachments/assets/e7702279-8250-42b9-8584-8b1aaaf0b82e" />
Gradient boosting obtuvo un MAE de 71,71 kWh/m²·año, equivalente a un error medio aproximado del 28 % respecto a un consumo medio de 256 kWh/m²·año. El R² de 0,294 indica que el modelo explica el 29 % de la variación; el resto depende de factores que el catastro no registra, como instalaciones térmicas, cerramientos y uso del edificio. Las ejecuciones y métricas quedaron registradas en MLflow.
<img width="938" height="466" alt="image" src="https://github.com/user-attachments/assets/992ce68a-38a4-492c-8f07-8da425d54b80" />
Figura — Comparación del error absoluto medio de los algoritmos frente a la línea base.
<img width="938" height="358" alt="image" src="https://github.com/user-attachments/assets/bb939f69-0253-4a25-a358-94d3f501eb39" />
Figura — Matriz de confusión y métricas de clasificación derivadas del consumo predicho.
<img width="938" height="393" alt="image" src="https://github.com/user-attachments/assets/316cae76-fc7a-460c-8d63-ada925ecc137" />
Figura — Comparación de priorización del modelo frente a una selección aleatoria.
# 6. Clasificación y Utilidad
Aunque el objetivo principal es continuo, el consumo predicho se convirtió en una decisión binaria de ineficiencia para obtener métricas de clasificación. El umbral se definió por zona climática usando el percentil 95 del consumo de los edificios clasificados A–D, evitando utilizar un único umbral absoluto.
En el conjunto de prueba, el modelo alcanzó precisión de 59,2 %, recall de 33,1 % y exactitud de 79,7 %. Como la clase eficiente domina el conjunto, la exactitud por sí sola resulta poco informativa: un modelo trivial que siempre predijera 'eficiente' alcanzaría 77,4 %. La utilidad principal está en priorizar inspecciones: el modelo encontró una proporción de inmuebles ineficientes 2,6 veces superior a la selección aleatoria.
# 7. Prevención de Fuga de Datos
Se excluyeron del entrenamiento las emisiones de CO₂, las clasificaciones A–G, las fechas de emisión y expiración, los identificadores y el estado del edificio. Las emisiones están directamente relacionadas con el consumo y las clasificaciones son derivaciones del objetivo; además, estos datos no estarían disponibles antes de una visita a un inmueble sin certificar.
La prevención se implementa mediante una lista blanca explícita de variables en Gold. Silver conserva los datos originales para trazabilidad y otros usos, pero Gold define exactamente qué variables puede ver el modelo. Así, un R² elevado causado por información del propio objetivo no se interpreta como una mejora real.
# 8. Consumo y visualización
Las tablas Gold se exponen mediante un dashboard nativo de Databricks, Power BI conectado directamente al SQL Warehouse y un tablero web independiente. Power BI utiliza las mismas tablas del Lakehouse, evitando exportaciones intermedias y manteniendo la trazabilidad del pipeline.
<img width="938" height="459" alt="image" src="https://github.com/user-attachments/assets/e59eec16-2bea-46af-9c8b-b6c2d5b39029" />
Figura — Dashboard nativo de Databricks sobre las tablas Gold.
<img width="938" height="577" alt="image" src="https://github.com/user-attachments/assets/a83757af-e10a-4142-86ff-ec1d2d56169a" />
Figura — Conexión de Power BI al SQL Warehouse y modelo semántico.
<img width="938" height="508" alt="image" src="https://github.com/user-attachments/assets/a1c7c657-e11a-41e9-9dc5-8aaa200fc615" />
Figura — Conexión de Power BI al SQL Warehouse y modelo semántico.
<img width="938" height="493" alt="image" src="https://github.com/user-attachments/assets/a1ddfa6f-c81c-4cde-9b68-420ef455e9b8" />
Figura — Informe de Power BI y superficies de consumo del modelo.
# 9. Correcciones y aprendizajes
Durante el desarrollo se identificaron y corrigieron problemas del pipeline. La regla inicial de valores atípicos fue sustituida por el rango físico [20, 1000] kWh/m²·año. El umbral inicial para clasificar ineficiencia generaba una matriz degenerada, por lo que se reemplazó por el percentil 95 de A–D por zona. La validación basada en RDD se reescribió con DataFrame para hacerla compatible con cómputo serverless.
También se comprobó que las coordenadas del origen no eran utilizables: una de sus componentes estaba completamente vacía. Por ello, la ubicación se representó mediante provincia, municipio y zona climática. 
Estos cambios muestran que las métricas y reglas deben validarse con criterios de dominio y no solo por obtener resultados aparentemente mejores.
# 10. Limitaciones y próximos pasos
El dataset representa trámites y tiene sesgo hacia inmuebles que participaron en el mercado inmobiliario. La zona climática es una aproximación que no incorpora completamente la corrección altitudinal. La ubicación precisa no está disponible. El recall de 33,1 % impide utilizar el modelo como único filtro, y su desempeño varía entre zonas con diferentes cantidades de observaciones.
Las predicciones actuales corresponden al conjunto de prueba y sirven para evaluación, no para producción. Un despliegue real requeriría cargar el universo de inmuebles no certificados desde Catastro. Como siguientes pasos se plantea enriquecer los datos con Catastro, separar los pipelines de entrenamiento e inferencia e incorporar variables de la envolvente del edificio.
# 11. Reproducibilidad
El catálogo cee_aragonv2 contiene landing/raw, Bronze, Silver, cuarentena, dimensión municipal y las tablas Gold de features, parque municipal y predicciones. Los notebooks principales son:
•	01_Bronze_Ingesta_Certificados — ingesta y capa cruda.
•	02_Calidad_Datos_Bronze — perfilado.
•	03_Silver_Certificados — reglas, cuarentena y reconciliación.
•	validate_silver_certificados — invariantes y corte del job.
•	04_Zona_Climatica_Gold — dimensión y tablas de consumo.
•	05_ML_Prediccion_Consumo — entrenamiento y evaluación.
Para reproducirlo: cargar el CSV del registro en landing/raw, ejecutar la ingesta Bronze, lanzar el job CEE_Aragon_ETL_Pipeline y ejecutar el notebook de predicción. El modelo y sus métricas se registran en MLflow para comparar ejecuciones posteriores.
