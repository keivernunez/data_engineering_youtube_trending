# 🔬 Ideas de Data Science sobre la Capa Gold — YouTube Trending Pipeline

> Las tablas `trending_analytics`, `channel_analytics` y `category_analytics` en la capa Gold contienen un dataset rico y multidimensional: engagement temporal, rankings de canales, cuota de categorías por región. A continuación, ideas concretas y accionables para explotar estos datos con técnicas de Data Science y Machine Learning.

---

## 1. 📈 Predicción de Tendencias (Time Series Forecasting)

### Problema
¿Cuántos videos en tendencia habrá mañana en cada región? ¿Cuál será el engagement promedio esperado?

### Dataset
`trending_analytics` — serie temporal diaria por región.

### Approach técnico
- **Features**: `total_views`, `avg_engagement_rate`, `unique_categories`, day_of_week, is_weekend, región
- **Modelos sugeridos**:
  - **Prophet** (Meta) — captura estacionalidad semanal y anual fácilmente
  - **ARIMA/SARIMA** — baseline sólido para series univariadas por región
  - **LightGBM con lag features** — para modelado multivariado
- **Target**: `avg_engagement_rate` del día siguiente o `total_views`

```python
# Ejemplo con Prophet
from prophet import Prophet

df_us = trending_df[trending_df['region'] == 'us'][['trending_date_parsed', 'avg_engagement_rate']]
df_us.columns = ['ds', 'y']

model = Prophet(weekly_seasonality=True, yearly_seasonality=True)
model.fit(df_us)
future = model.make_future_dataframe(periods=30)
forecast = model.predict(future)
```

### Valor de negocio
Anticipar picos de engagement para planificar publicaciones o campañas de contenido.

---

## 2. 🤖 Clasificación de Canales (Canal Clustering)

### Problema
¿Qué tipos de canales aparecen en trending? ¿Existen clusters naturales por comportamiento?

### Dataset
`channel_analytics` — métricas por canal y región.

### Features
- `avg_engagement_rate`
- `times_trending`
- `avg_views_per_video`
- `peak_views / avg_views_per_video` (ratio de concentración)
- `last_trending - first_trending` (días activo en trending)
- `categories` (codificado como one-hot o TF-IDF)

### Approach técnico
```python
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

# Features numéricas
features = ['avg_engagement_rate', 'times_trending', 'avg_views_per_video']
X = channel_df[features].fillna(0)
X_scaled = StandardScaler().fit_transform(X)

# Elbow method para k óptimo
inertias = [KMeans(n_clusters=k, random_state=42).fit(X_scaled).inertia_ for k in range(2, 10)]

# Clustering final
kmeans = KMeans(n_clusters=4, random_state=42)
channel_df['cluster'] = kmeans.fit_predict(X_scaled)
```

### Clusters esperados
- 🔥 **Virales ocasionales**: pocas apariciones, views extremos
- 📺 **Consistentes**: muchas apariciones, engagement estable
- 🌱 **Emergentes**: pocas apariciones recientes, alto engagement por video
- 📉 **Declining**: alto historial, bajo engagement reciente

### Valor de negocio
Segmentación de creadores para estrategias de marketing / partnership.

---

## 3. 💬 Análisis de Sentimiento y NLP en Títulos

### Problema
¿Los títulos más positivos generan más engagement? ¿Qué palabras clave caracterizan el trending?

### Dataset
Enriquecer `clean_statistics` (Silver) con análisis de NLP sobre la columna `title`.

### Approach técnico
```python
from transformers import pipeline
import pandas as pd

# Sentiment analysis con HuggingFace (modelo multilingüe)
sentiment = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")

# Aplicar sobre muestra
df['sentiment_score'] = df['title'].apply(lambda x: sentiment(x[:512])[0]['score'])
df['sentiment_label'] = df['title'].apply(lambda x: sentiment(x[:512])[0]['label'])

# Correlación con engagement
df.groupby('sentiment_label')['engagement_rate'].mean()
```

### Análisis adicionales
- **TF-IDF + clustering** de títulos por categoría para detectar fórmulas de éxito
- **Análisis de longitud del título** vs engagement
- **Detección de clickbait** con regex patterns ("¿PUEDES CREER...?", "INCREÍBLE...")
- **Word clouds** por región y categoría

### Valor de negocio
Guiar a creadores sobre qué características de título correlacionan con más views.

---

## 4. 🌍 Análisis Geográfico Comparativo

### Problema
¿Qué categorías dominan en cada país? ¿Hay diferencias culturales en el consumo de contenido?

### Dataset
`category_analytics` — `view_share_pct` por categoría, región y fecha.

### Approach técnico
```python
import plotly.express as px

# Heatmap de view_share_pct por región y categoría
pivot = category_df.groupby(['region', 'category_name'])['view_share_pct'].mean().unstack()

fig = px.imshow(
    pivot,
    title="Cuota de Views por Categoría y Región",
    color_continuous_scale='Viridis',
    labels=dict(x="Categoría", y="Región", color="% de Views")
)
```

### Análisis complementarios
- **Análisis de correlación entre regiones**: ¿Qué tan similares son US y CA? ¿Y JP vs KR?
- **UMAP/t-SNE** de regiones basado en el perfil de categorías para visualizar similaridad cultural
- **Análisis temporal**: ¿Cambia la distribución de categorías entre semana y fin de semana?

### Valor de negocio
Estrategia de localización de contenido para marcas globales. Identificar nichos poco saturados por región.

---

## 5. ⚠️ Detección de Anomalías en Engagement

### Problema
¿Cuándo un video tiene un comportamiento estadísticamente anormal (viral inesperado, compra de views, etc.)?

### Dataset
`trending_analytics` + `clean_statistics` (Silver).

### Approach técnico
```python
from sklearn.ensemble import IsolationForest
import numpy as np

# Features para detección de anomalías
features = ['views', 'likes', 'comment_count', 'engagement_rate', 'like_ratio']
X = stats_df[features].fillna(0)

# Isolation Forest
clf = IsolationForest(contamination=0.05, random_state=42)
stats_df['anomaly'] = clf.fit_predict(X)
stats_df['anomaly_score'] = clf.decision_function(X)

# Videos anómalos
anomalies = stats_df[stats_df['anomaly'] == -1].sort_values('anomaly_score')
```

### También explorar
- **Z-score** sobre `views` dentro de cada categoría y región
- **DBSCAN** para detectar grupos inusuales de videos
- **Regla IQR** para outliers en `engagement_rate` por categoría

### Valor de negocio
Alertas tempranas de videos virales, detección de fraude (compra de views), monitoreo de crisis de reputación.

---

## 6. 🏅 Sistema de Recomendación de Canales / Contenido

### Problema
¿Qué canales debería seguir un usuario interesado en cierta categoría en cierta región?

### Dataset
`channel_analytics` + `category_analytics`.

### Approach técnico — Content-Based Filtering
```python
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

# Representar cada canal como un vector de sus categorías y métricas
channel_df['profile'] = (
    channel_df['categories'] + ' ' +
    channel_df['region'] + ' ' +
    (channel_df['avg_engagement_rate'] > threshold).map({True: 'high_engagement', False: 'low_engagement'})
)

vectorizer = TfidfVectorizer()
tfidf_matrix = vectorizer.fit_transform(channel_df['profile'])
similarity = cosine_similarity(tfidf_matrix)

# Canales más similares a un canal dado
def get_recommendations(channel_name, top_n=5):
    idx = channel_df[channel_df['channel_title'] == channel_name].index[0]
    sim_scores = sorted(enumerate(similarity[idx]), key=lambda x: x[1], reverse=True)[1:top_n+1]
    return channel_df.iloc[[i[0] for i in sim_scores]][['channel_title', 'region', 'avg_engagement_rate']]
```

### Valor de negocio
Descubrimiento de contenido, recomendaciones para marketers buscando influencers similares a un referente.

---

## 7. 📉 Análisis de Estacionalidad y Ciclos de Vida

### Problema
¿Cuánto tiempo permanece un video en trending? ¿Hay categorías con ciclos de vida más cortos?

### Dataset
`clean_statistics` (Silver) — múltiples registros del mismo `video_id` en fechas distintas.

### Approach técnico
```python
# Calcular duración en trending por video
video_lifecycle = stats_df.groupby(['video_id', 'title', 'category_name', 'region']).agg(
    days_trending=('trending_date_parsed', lambda x: (x.max() - x.min()).days + 1),
    peak_views=('views', 'max'),
    first_day=('trending_date_parsed', 'min'),
    last_day=('trending_date_parsed', 'max'),
).reset_index()

# ¿Qué categorías tienen videos con mayor longevidad en trending?
video_lifecycle.groupby('category_name')['days_trending'].mean().sort_values(ascending=False)

# ¿Hay diferencia entre regiones?
video_lifecycle.groupby(['region', 'category_name'])['days_trending'].mean().unstack()
```

### Análisis avanzado
- **Curvas de decaimiento** de views/día usando regresión exponencial
- **Supervivencia en trending** con análisis de Kaplan-Meier (tiempo hasta salir del trending)

### Valor de negocio
Planificación de campañas: saber cuánto tiempo un video "vive" en trending por categoría permite optimizar el gasto publicitario.

---

## 8. 🎯 Modelo de Predicción de Éxito Temprano

### Problema
En las primeras 24h de trending, ¿podemos predecir si un video llegará al Top 10 de su región?

### Dataset
`clean_statistics` (Silver) con los primeros registros de cada video.

### Features (primeras 24h)
- `views` en hora 0, 6, 12, 24
- `engagement_rate` inicial
- `like_ratio` inicial
- `category_id`
- `region`
- `hour` de publicación
- `day_of_week` de publicación

### Approach técnico
```python
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split

# Target: ¿llegó al Top 10 en algún momento? (usando rank_in_region de channel_analytics)
# Features del primer día de trending

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = GradientBoostingClassifier(n_estimators=200, max_depth=5, random_state=42)
model.fit(X_train, y_train)

# Feature importance
importance_df = pd.DataFrame({
    'feature': feature_names,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)
```

### Valor de negocio
Identificar videos virales temprano para maximizar el impacto de inversión publicitaria en el momento preciso.

---

## 📊 Stack Sugerido para Implementar estos Proyectos

| Herramienta | Uso |
|---|---|
| **Amazon Athena** | Extracción de datos desde Gold/Silver |
| **AWS Glue** | Feature engineering a escala |
| **Amazon SageMaker** | Entrenamiento y despliegue de modelos |
| **Amazon QuickSight** | Visualización de resultados |
| **pandas / polars** | Análisis exploratorio local |
| **scikit-learn** | Modelos clásicos de ML |
| **Prophet / statsmodels** | Series de tiempo |
| **HuggingFace Transformers** | NLP sobre títulos |
| **Plotly / Seaborn** | Visualizaciones interactivas |

---

## 🗺️ Roadmap Sugerido

```
1. EDA completo con Athena + pandas
     ↓
2. Time Series Forecasting (trending_analytics)
     ↓
3. Análisis Geográfico + Clustering de Canales
     ↓
4. NLP sobre títulos (Silver)
     ↓
5. Detección de Anomalías
     ↓
6. Modelo de predicción de éxito temprano (el más avanzado)
```

---

> 💡 **Tip de arquitectura**: Para correr estos análisis a escala, conecta Amazon SageMaker directamente a las tablas Gold via Athena usando `boto3` + `awswrangler`. No necesitas mover los datos — el cómputo va a los datos.

