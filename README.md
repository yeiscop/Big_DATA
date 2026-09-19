# Trabajo Final Big Data/ Luis Hernando Galvis Trujillo - Luis Mauricio Rivera Ruiz

## 1. Caso del negocio

Una empresa de esencias naturales cuenta con treinta puntos de venta en el país y en cada una de las tiendas dispone de 1500 productos. Semanalmente, la empresa distribuye en camiones los productos desde sus bodegas a cada uno de los 30 puntos de venta.

A través de un sistema de forecasting, suministrado por el área de pronósticos de la empresa, se tiene una proyección de ventas con base al historial de los últimos 9 meses registrados.

Para hacer seguimiento a las ventas se creó un data frame en el que se registran cada uno de los productos, por tienda, y se establece su proyección de venta, su nivel de inventario en las tiendas, o incluso si se encuentra en los puntos de venta o si el producto está siendo transportado desde la bodega ha hacia la tienda.

La empresa diseñó un sistema para identificar el nivel de inventario en cada uno de sus puntos de venta. Se establecieron 5 indicadores de colores para mostrar el nivel del stock de cada producto en las diferentes tiendas.

Tres indicadores señalan el comportamiento natural del inventario de las tiendas:

El indicador Tope Verde refleja un nivel óptimo de inventario de un producto en una tienda.
El indicador Tope Amarillo alerta que un producto está por debajo del nivel óptimo, por lo que necesita reestablecer se en el inventario de la tienda.
El indicador Tope Rojo alerta que un producto está a punto de agotarse del inventario de una tienda.

Los otros dos indicadores muestran escenarios que deben evitarse en los inventarios de las tiendas:
El indicador azul muestra que hay un sobrestock de un producto en el inventario de una tienda.
El indicador negro señala que se agotó totalmente un producto del inventario inventario de una tienda.

### Problema:
En el análisis operacional de la cadena de suministro de la organización, se evidencia una alta variabilidad en los niveles de inventario por punto de venta, caracterizada por la dificultad de mantener un stock de seguridad óptimo para cada producto por tienda. Esta discrepancia responde a la falta de alineación entre los niveles actuales de abastecimiento y las variables clave de decisión, tales como la demanda o ventas promedio, el lead time o tiempo de transporte, el inventario disponible en bodega central y el inventario existente en punto de venta.
Esta descalibración genera dos fenómenos opuestos e ineficientes en la red de distribución:
Sobrestock recurrente: Se registran puntos de venta con nula rotación de determinados productos que, no obstante, continúan recibiendo despachos semanales desde la bodega central.
Agotamiento de inventario (Stockouts): Se identifican tiendas en las cuales la demanda supera la oferta disponible antes de finalizar el ciclo semanal, generando periodos desabastecidos hasta la siguiente ventana de reposición.
En la metodología DDMRP, cada SKU se clasifica en una zona de buffer representada por colores: 

<img width="748" height="457" alt="image" src="https://github.com/user-attachments/assets/579d783b-9be7-4e7f-bc6e-69451ae869d5" />

### Objetivos:

**Objetivo General**

-Construir un modelo que permita identificar los productos en sobrestock o que se encuentren agotados en los 30 puntos de venta.

**Objetivos Especificos**

-Replantear el abastecimiento que realiza la empresa en los 30 puntos de venta para llevarlos a un nivel más óptimo.

-Determinar qué alerta del buffer arrojan los productos nuevos que se decidan lanzar a la venta.

-Comprobar si el pronóstico que el departamento de planeación genera para la empresa es correcto.

### Solución
Construir un modelo de clasificación supervisado que aprende de 45,000 registros históricos (30 tiendas × 1,500 SKUs) y predice el Color de Buffer de cada combinación SKU-tienda usando variables operativas, de inventario y económicas.

### Valor entregado
Automatización de la clasificación de buffers para miles de SKUs.
Detección temprana de productos que migrarán a zonas críticas (ROJO) o excedentes (AZUL/NEGRO).
Priorización de órdenes de reabastecimiento basada en predicción, no en reacción.
Sistema de predicción para productos nuevos que aún no tienen historial de clasificación.

## 2. Análisis Económico
La implementación de un modelo de clasificación automática del Color de Buffer DDMRP genera beneficios económicos directos en la gestión de inventario y permite una gestión anticipada del riesgo de stock.

### Reducción de costos operativos
#### Antes del modelo:

-Clasificación manual de buffers SKU por SKU con reglas fijas

-Análisis reactivo — se actúa solo cuando el inventario ya llegó a ROJO o AZUL

-Procesos repetitivos de revisión de topes y niveles de stock

-Decisiones de reabastecimiento basadas en criterio subjetivo del operario

#### Con el modelo en Databricks:

-La clasificación del Color de Buffer se automatiza mediante Machine Learning (Random Forest)

-Se reducen tiempos de análisis y carga operativa del equipo de inventario

-El sistema predice el color para 1,500 SKUs × 30 tiendas en una sola inferencia

Esto implica una disminución significativa de costos operativos y mayor eficiencia en la gestión de inventario.

#### Reducción de capital inmovilizado por sobre-stock
El dataset revela que un porcentaje significativo de productos presenta -Sobre_Stock_Unid > 0, es decir, unidades que exceden el tope verde y generan capital inmovilizado sin rotación. La variable Costo_Sobre_Stock cuantifica el costo financiero directo de este exceso.

#### El análisis muestra que:

-El Costo_Inventario varía desde COP 23,991 hasta COP 53,618,544 por SKU-tienda

-Sobre_Stock_Unid es la variable más determinante del color de buffer (correlación 0.72 con el target)

-Existen productos con ADU muy bajo pero alto inventario, señalando sobre-stock no detectado

Gracias al modelo se pueden aplicar estrategias preventivas:

-Detección temprana de productos que migrarán a AZUL o NEGRO antes de que el sobre-stock se acumule

-Priorización de acciones de liquidación o promoción para productos con alta probabilidad de excedente

-Reducción de pedidos innecesarios al identificar SKUs que ya tienen cobertura suficiente

#### Reducción de pérdidas por quiebre de stock

Así mismo, el modelo permite identificar productos con alta probabilidad de caer en ROJO antes de que el quiebre ocurra.

Gracias a esto se pueden aplicar estrategias preventivas:

-Reabastecimiento anticipado de productos que el modelo clasifica con alta probabilidad de ROJO

-Priorización de órdenes de compra según urgencia predicha, no según reacción al faltante

-Reducción de ventas perdidas por falta de inventario disponible

#### Optimización del pronóstico de demanda (ADU)

 Se incluye un análisis de validación del ADU que identifica:

-**Alta variabilidad en ADU por SKU** — recomienda suavizado estacional del pronóstico

-**Días de inventario promedio altos** — el ADU podría estar subestimado, generando sobre-stock

-**Inconsistencias ADU vs Inventario** — productos con demanda baja y stock alto (o viceversa)

Corregir estas inconsistencias mejora la precisión de los topes de buffer y reduce el costo asociado a decisiones erróneas de reposición.

Impacto financiero resumido

<img width="796" height="245" alt="image" src="https://github.com/user-attachments/assets/f5737d9d-606b-4dea-bb62-e9f2296070bf" />


## 3. Arquitectura Propuesta

El pipeline sigue una arquitectura medallón en Databricks:

<img width="1024" height="572" alt="image" src="https://github.com/user-attachments/assets/0d7cf933-17d2-4c4a-a406-e51a34f03ad4" />

### Capas de datos

<img width="806" height="190" alt="image" src="https://github.com/user-attachments/assets/2745245e-9ae5-427d-b08c-18a8eda17a4d" />

### Tecnologías

<img width="572" height="366" alt="image" src="https://github.com/user-attachments/assets/191b0b64-3a5c-41ce-8de1-28998440328a" />

## 4. Pipeline de Ingesta de Datos

<img width="1919" height="929" alt="image" src="https://github.com/user-attachments/assets/dd5c96a0-d721-4b2f-aa5d-fae91a08f9bd" />

### Flujo de datos

<img width="752" height="447" alt="image" src="https://github.com/user-attachments/assets/0be1e559-35d4-4971-a289-be520b8b95bc" />

### Esquema de datos (Bronze Layer)

<img width="681" height="561" alt="image" src="https://github.com/user-attachments/assets/74c0d7d9-f4b4-4c5e-ac5c-fb4ac616f2d0" />

## 5. Modelo de Ciencia de Datos Aplicado

### Enfoque
Se utilizó un enfoque de clasificación multiclase supervisada con la siguiente metodología CRISP-DM adaptada:

### Análisis Exploratorio de Datos (EDA)
Distribución del target: El dataset presenta desbalanceo de clases:
Clase	Proporción

AMARILLO	44.1%

VERDE	25.6%

AZUL	25.2%

ROJO	4.9%

NEGRO	0.3%

Las clases ROJO y NEGRO son minoritarias, lo que se tiene en cuenta con un split estratificado.

**Matriz de correlación**: Se analizó la correlación entre las 16 variables numéricas para detectar multicolinealidad.

### Detección de Target Leakage
Se calculó la correlación absoluta de cada feature numérica con el target codificado. Las features más correlacionadas:

Variable	Correlación con Target

Sobre_Stock_Unid	0.719

Costo_Sobre_Stock	0.603

Unidades_a_Enviar	0.425

ADU	0.350

Tope_Rojo	0.337

Tope_Amarillo	0.336

Tope_Verde	0.336

Ninguna superó el umbral de 0.95, por lo que no se detectó leakage severo.

### División Train/Test

Estrategia: Split estratificado (stratify=y) para mantener la distribución de clases.

Proporción: 80% entrenamiento (36,000) / 20% test (9,000).

Semilla: random_state=42 para reproducibilidad.

## 6. Modelo: Random Forest Classifier
### Hiperparámetros:

<img width="295" height="227" alt="image" src="https://github.com/user-attachments/assets/21359560-e8a8-4e99-9cd9-5e37b1fbf53f" />

### Tracking con MLflow:

Experimento: DDMRP_Classification
Se registran parámetros, métricas (accuracy), firma del modelo (infer_signature) y ejemplo de entrada.
Serialización con cloudpickle para evitar problemas de trust en skops.

### Evaluación

<img width="385" height="147" alt="image" src="https://github.com/user-attachments/assets/854318d9-0a38-4bdc-b4a7-23276599535c" />

**Métricas adicionales**: Se generó classification_report (precision, recall, f1-score por clase) y matriz de confusión visualizada con heatmap.

Top 6 Variables Determinantes (Gold Layer)
Las 6 variables más importantes según feature_importances_ del Random Forest:

**Ranking	Variable	Importancia**

<img width="505" height="336" alt="image" src="https://github.com/user-attachments/assets/78978ee8-4537-46b8-a558-8ff425d47064" />

La variable Sobre_Stock_Unid es la más determinante por un margen significativo, lo que confirma que el sobre-stock es el principal driver del color de buffer.

### Sistema de Predicción para Productos Nuevos
Se incluye una función hacer_prediccion() que permite ingresar los datos de un SKU nuevo y obtener:

La clase predicha (Color de Buffer).
Las probabilidades por clase con su porcentaje.
Una visualización de barras horizontales con colores representativos de cada zona.

## 7. Visualizaciones

Se desarrolló un dashboard en Databricks Dashboard para visualizar los resultados generados por el modelo 

Link de las visualizaciones https://dbc-76dc07b7-aa46.cloud.databricks.com/dashboardsv3/01f1aeaf6c1c14da8dd11b074e42a208/published?o=7474658879432411

<img width="1697" height="440" alt="image" src="https://github.com/user-attachments/assets/09f5abfb-8f78-45a2-90e8-f81c22e3b36d" />

<img width="1312" height="457" alt="image" src="https://github.com/user-attachments/assets/44c30473-9dcf-4dc5-982d-ad97cf31bb6c" />

### Buffer Color Distribution (Gráfica de Pastel)
Muestra la distribución proporcional de los 5 estados de buffer DDMRP en todo el inventario

### Buffer Status by Store (Barras Apiladas)
Compara el estado del buffer entre tiendas, permitiendo identificar cuáles tienen más productos en ROJO (crítico) o NEGRO (sobre-stock excesivo)

### Total Inventory Cost (Contador)
Valor total del inventario en COP (Pesos Colombianos) en todas las tiendas

### Total Overstock Cost (Contador)
Costo del capital inmovilizado en productos con sobre-stock (zona AZUL/NEGRO)

### Inventory Cost by Store (Gráfica de Barras)
Compara el valor del inventario por tienda. Identifica cuáles tiendas tienen mayor capital invertido en inventario

### Overstock Cost by Store (Gráfica de Barras)
Muestra qué tiendas tienen mayor costo de sobre-stock. Prioriza acciones correctivas de liquidación/promoción

### Avg Inventory Days by Store (Gráfica de Barras)
Días promedio de cobertura de inventario por tienda. Valores altos indican rotación lenta o ADU subestimado

### Units to Send by Store (Gráfica de Barras)
Priorización de reabastecimiento. Muestra qué tiendas necesitan más unidades urgentemente (productos en ROJO/AMARILLO)

### Data Table (Tabla Detallada)
Vista detallada de todos los SKUs con sus métricas DDMRP para análisis granular
