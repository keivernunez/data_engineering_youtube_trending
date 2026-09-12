# 🚀 LinkedIn Post — YouTube Trending Data Pipeline

---

## 📝 Post Principal (versión completa)

---

Hace unas semanas me propuse un reto: construir un **pipeline de Data Engineering end-to-end en AWS** desde cero — completamente serverless, automatizado y con arquitectura de producción.

Hoy lo comparto. 👇

---

### 🎬 El proyecto: YouTube Trending Data Pipeline

Un pipeline que ingesta datos de **tendencias de YouTube en tiempo real** para **10 países** (US, CA, GB, DE, FR, IN, JP, KR, MX, RU), los transforma en 3 capas de calidad creciente y genera tablas analíticas listas para consumo — **todo sin un solo servidor que administrar**.

---

### 🏗️ Arquitectura Medallion en AWS

**Bronze → Silver → Gold**

📌 **Bronze Layer**: AWS Lambda llama a la YouTube Data API v3 cada 6 horas, y almacena el JSON crudo en S3 con particionado Hive-style (`region/date/hour`).

📌 **Silver Layer**: Dos procesos corren en **paralelo** via Step Functions:
- Una Lambda transforma los datos de referencia (categorías) a Parquet usando `awswrangler`
- Un **Glue Job PySpark** aplica schema enforcement, cleansing, feature engineering (`like_ratio`, `engagement_rate`) y deduplicación con window functions

📌 **Gold Layer**: Un segundo Glue Job genera **3 tablas analíticas**:
- `trending_analytics`: KPIs diarios por región
- `channel_analytics`: ranking de canales con `rank_in_region`
- `category_analytics`: cuota de views por categoría (`view_share_pct`)

---

### ⚙️ Stack Técnico

🔷 **Orquestación**: AWS Step Functions (7 estados, retry logic, parallel branches)
🔷 **Ingesta**: AWS Lambda + YouTube Data API v3
🔷 **Almacenamiento**: Amazon S3 (3 buckets, Hive partitioning)
🔷 **ETL**: AWS Glue + PySpark (2 jobs)
🔷 **Catálogo**: AWS Glue Data Catalog (3 databases)
🔷 **Consulta**: Amazon Athena (SQL serverless sobre Parquet)
🔷 **Calidad de datos**: Lambda dedicada con 5 tipos de checks (row count, null%, schema, value range, freshness)
🔷 **Alertas**: Amazon SNS (success + 4 tipos de failure notifications)
🔷 **Scheduling**: Amazon EventBridge Scheduler
🔷 **Seguridad**: IAM con mínimo privilegio (3 políticas)
🔷 **Lenguaje**: Python 3.x · PySpark · pandas · awswrangler

---

### 💡 Lo que más me gustó construir

**El gate de calidad de datos.**

Antes de que los datos lleguen a la capa Gold, una Lambda ejecuta 5 validaciones sobre Silver usando Athena. Si alguna falla, Step Functions toma el camino de error y **nunca se ejecuta el Gold job**. Los datos Gold son siempre confiables o no existen — no hay término medio.

Eso es lo que diferencia un pipeline de juguete de uno de producción.

---

### 📊 Resultado

Datos de YouTube frescos cada 6 horas, en **Parquet comprimido con Snappy**, consultables con SQL estándar desde Athena, para 10 países simultáneamente. Sin servidores. Sin intervención manual.

---

🔗 Código completo disponible en GitHub: **github.com/tu-usuario/data-engeneering-aws-pipeline**

---

💬 ¿Estás trabajando con datos de APIs en AWS? ¿Qué desafíos has encontrado en la capa de calidad de datos? Me encantaría leer tu experiencia en los comentarios.

---

**#DataEngineering #AWS #Python #PySpark #ETL #Serverless #DataPipeline #AwsGlue #StepFunctions #MedallionArchitecture #DataQuality #LakeHouse #Athena #Lambda #CloudComputing #Portfolio**

---
---

## 📝 Post Alternativo (versión corta — carrusel)

*Ideal para un carrusel de imágenes con la arquitectura*

---

**Construí un pipeline de Data Engineering serverless en AWS 🚀**

Stack: Lambda · Glue · Step Functions · S3 · Athena · SNS

¿El resultado? Datos de YouTube en tendencia para 10 países, actualizados cada 6h, en Parquet, listos para analizar con SQL.

Arquitectura Medallion: Bronze (JSON raw) → Silver (Parquet limpio) → Gold (tablas analíticas)

Lo más interesante: un gate de calidad de datos que bloquea la capa Gold si los datos no cumplen los estándares. Así se hace en producción.

👇 Enlace al repositorio en los comentarios

**#DataEngineering #AWS #Serverless #Python #ETL**

---
---

## 📝 Post Alternativo (versión técnica — para audiencia DE/ML)

---

**Detalles técnicos de mi pipeline YouTube Trending en AWS 🔧**

Algunos patrones de ingeniería que implementé y por qué:

**1. Paralelismo en Step Functions**
En lugar de ejecutar la Lambda de referencia y el Glue Job de ETL en secuencia, los puse en un estado `Parallel`. Esto redujo el tiempo total del pipeline en ~40% porque ambos procesos son independientes.

**2. Idempotencia en escrituras**
La Lambda de referencia usa `awswrangler.s3.to_parquet(mode="overwrite_partitions")`. Si el pipeline se ejecuta dos veces el mismo día, no duplica datos — sobreescribe solo la partición afectada.

**3. Deduplicación con window functions en PySpark**
```python
window = Window.partitionBy("video_id", "region", "trending_date_parsed")
           .orderBy(col("_processed_at").desc())
df = df.withColumn("_row_num", row_number().over(window))
       .filter(col("_row_num") == 1)
```
Garantiza exactamente un registro por (video, región, fecha) incluso con múltiples ejecuciones.

**4. Schema detection automático**
El Glue Job detecta si viene de la API de YouTube (columnas como `snippet.title`) o del CSV de Kaggle (columnas flat). Mismo job, dos formatos de entrada.

**5. Feature engineering en la capa Silver**
`engagement_rate = (likes + dislikes + comments) / views * 100`
Calculado en Silver para que Gold pueda agregar directamente sin recalcular.

🔗 github.com/tu-usuario/data-engeneering-aws-pipeline

**#DataEngineering #AWS #PySpark #Glue #StepFunctions #DataQuality #Python**

---

