# 📌 Nombre del Proyecto
- Exploración de Datos Espaciales – SpaceX 
 

## 📝 Descripción del Proyecto  
Proyecto enfocado en la ingesta, transformación y análisis de datos públicos de SpaceX a través de su API oficial.  

Se implementa un proceso ETL para extraer información sobre lanzamientos, cohetes y destinos, almacenarla en PostgreSQL y realizar análisis exploratorio y predictivo mediante visualizaciones interactivas y modelos de machine learning.

---


## 🎯 Objetivos  
### Objetivo General  
Diseñar una arquitectura de datos que permita analizar patrones históricos de lanzamientos de SpaceX y generar insights predictivos.

### Objetivos Específicos  
- Construir un **Extractor ETL** con manejo de errores y validación de datos.  
- Diseñar un esquema optimizado en **PostgreSQL** para almacenar la información de la API.  
- Desarrollar un **Dashboard en Streamlit** con visualizaciones interactivas.  
- Implementar modelos de **Machine Learning** para análisis predictivo.  
- Contenerizar la solución completa mediante **Docker Compose**.  

---




## 🛠️ Herramientas  
- **VS Code** – Editor de código principal  
- **Python** – Lenguaje de programación base  
- **Docker** – Contenerización de la aplicación  
- **WSL (Windows Subsystem for Linux)** – Entorno Linux en Windows  
- **PostgreSQL** – Base de datos relacional  
- **Jupyter Notebook** – Análisis y visualización interactiva de datos  



## 📁 Estructura del Proyecto  
```
spacex/
├── etl/
│   └── extractor.py
├── database/
│   └── schema.sql
├── dashboard/
│   └── app.py
├── notebooks/
│   └── analisis_ml.ipynb
├── data/
├── docker-compose.yml
├── Dockerfile
└── README.md
```



## 👥 Autores  
- Elkin Stiven Contreras Rojas  
- Alejandro Ortiz Vargas  


