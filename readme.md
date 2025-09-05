# ¡Bienvenido a Nuestra Base de Datos de Prueba para MCP PostgreSQL!

Esta solución de prueba ha sido diseñada para organizar y gestionar datos de manera clara y sencilla. ¡No te preocupes si no eres técnico! Aquí te explicamos en términos simples de qué se trata y cómo puedes usarla.

## ¿Qué es Esto?

Este proyecto recopila y organiza información proveniente de diversas fuentes, principalmente de la **API pública de SIMEM**. Utilizamos Python para transformar y almacenar estos datos en una base de datos PostgreSQL, permitiéndonos presentar información organizada y confiable.

## ¿De Dónde Vienen los Datos?

- **Fuente Principal**: La información se obtiene de la [API pública de SIMEM](https://www.simem.co/backend-files/api/PublicData).
- **Extracción de Datos**: El script `simem_synchronization.py` se encarga de consultar la API, obtener los registros y guardarlos en archivos JSON dentro de la carpeta `data/simem/<dataset_id>`.
- **Propósito**: Estos datos nos ayudan a crear tablas con información sobre agentes, empresas, actividades, contratos y tipos de mercado, garantizando la procedencia e integridad de la información.

## ¿Cómo Funciona?

Aunque el proceso interno es técnico, en resumen se realizan los siguientes pasos:

1. **Recolección de Datos**: Se consulta la API de SIMEM para obtener la información necesaria.
2. **Organización**: Se transforma y organiza la información para que sea fácilmente comprensible y utilizable.
3. **Almacenamiento**: Se crean y llenan tablas en PostgreSQL, estableciendo además las relaciones necesarias entre ellas.

## ¿Cómo Puedo Usarlo?

Para interactuar con este proyecto, sigue estos pasos sencillos:

1. **Preparar el Entorno**: Asegúrate de tener instalado Python 3.x y los paquetes necesarios.
2. **Configurar la Conexión**: Revisa el archivo `config.py` para verificar que la cadena de conexión a PostgreSQL es correcta.
3. **Ejecutar el Script para Crear las Tablas**:
   
   ```bash
   python db.py
   ```
   
   Esto creará y llenará automáticamente las tablas de la base de datos.
4. **Extraer Datos Actualizados de SIMEM**:
   
   ```bash
   python simem_synchronization.py
   ```
   
   Este script descargará y almacenará nuevos datos de la API de SIMEM.

## Requisitos Básicos

No necesitas ser un experto para usar este proyecto. Solo asegúrate de tener instalado lo siguiente:

- Python 3.x
- Los siguientes paquetes de Python: Pandas, Numpy, SQLAlchemy y Tqdm

Puedes instalar estos paquetes ejecutando:

```bash
pip install -r requirements.txt
```

## Más Información

Para conocer detalles técnicos sobre el funcionamiento interno (como la transformación y gestión de datos), consulta la documentación técnica interna del proyecto o revisa el código fuente directamente.

---

© 2025 - Base de Datos de Prueba para MCP PostgreSQL
