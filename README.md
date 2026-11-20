# Spark Standalone: Análisis de Inventario con PySpark

**Alumno:** Daniel Croche Soto
**Módulo:** Spark Standalone 
##Pasos
### 1. Instalar Java, Python y herramientas

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk python3-venv python3-pip wget git
```

### 2. Clonar el repositorio y crear el entorno virtual

```bash
git clone <https://github.com/DanCro21/Spark.git>
cd Spark
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark
python -c "import pyspark; print(pyspark.__version__)"
```

`pip install pyspark` 

### 3. Descargar los datasets

```bash
mkdir -p DATOS
wget "https://github.com/josepgarcia/datos/raw/refs/heads/main/stocks.txt"  -O ./DATOS/stocks.txt
wget "https://github.com/josepgarcia/datos/raw/refs/heads/main/persons.csv" -O ./DATOS/persons.csv

head DATOS/stocks.txt
head DATOS/persons.csv
```

### 4. Ejecutar

```bash
python Ejercicios.py
python proyecto_stocks.py
```

Al final de `proyecto_stocks.py` el programa espera un Enter antes de cerrar Spark. Mientras tanto se puede abrir la **Spark UI** en `http://localhost:4040`.

---




### Ejercicio 6: Optimización, evitar múltiples `split()`

**Problema de eficiencia:** el código original hace `x.split(",")` en tres `map` distintos (nombres, edades y ciudades). Cada línea se divide **3 veces**, y cada RDD resultante vuelve a leer y procesar el archivo cuando se ejecuta una acción.

**Solución:** parsear cada línea una sola vez, guardar la tupla completa y derivar las columnas de ese RDD.

```python
def parsear(linea):
    nombre, edad, genero, salario = linea.split(",")
    return (nombre, int(edad), genero, int(salario))

datos = sin_header.map(parsear).cache()
nombres  = datos.map(lambda t: t[0])
edades   = datos.map(lambda t: t[1])
generos  = datos.map(lambda t: t[2])
salarios = datos.map(lambda t: t[3])
```

El `persons.csv` real tiene 4 columnas, así que se extraen nombre, edad, género y salario en lugar de ciudad.

**Por qué es más eficiente:** el `split` se ejecuta una vez por línea y no tres. Además, `cache()` evita releer y reparsear el archivo cada vez que se usa `datos`.

### Ejercicio 7: Debugging

| # | Error | Por qué es un error | Corrección |
|---|---|---|---|
| 1 | Falta obtener el SparkContext | `sc` nunca se define, así que `sc.textFile` lanza `NameError` | `sc = spark.sparkContext` |
| 2 | Ruta local sin `file://` ni ruta absoluta | Spark puede buscar el archivo en HDFS o en otra ruta y lanza `FileNotFoundException` | `"file://" + os.path.abspath("./datos.csv")` |
| 3 | `collect()` sobre un dataset grande | Trae todos los datos al driver y puede causar `OutOfMemoryError` | `take(n)` o `foreach()` |
| 4 | No se cierra la sesión | Quedan recursos y el puerto 4040 ocupados | `spark.stop()` |

```python
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Debug").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile("file://" + os.path.abspath("./DATOS/persons.csv"))
for linea in rdd.take(10):
    print(linea)
spark.stop()
```

---

## Proyecto: Análisis de Inventario (`proyecto_stocks.py`)

## Preguntas de reflexión

### 1. Eficiencia: ¿por qué es importante hacer `split()` una sola vez por línea? ¿Cuánto tiempo ahorrarías con 1 millón de líneas?

Porque `split()` es una operación que se repite por cada línea del archivo. Con tres `map` independientes (Ejercicio 6), un millón de líneas implica **3 millones de splits**; con un solo `split` son **1 millón**. El costo de parseo baja a una tercera parte, es decir, se ahorra alrededor del **66 %** de ese trabajo. Además, sin `cache()` cada RDD derivado vuelve a leer el archivo desde disco.

### 2. Particionamiento: ¿cuántas particiones tiene tu RDD? ¿Cómo afecta al rendimiento? ¿Qué pasaría con solo 1 partición?

Se comprueba con `rdd.getNumPartitions()`. Con `textFile` sobre un archivo pequeño como `stocks.txt`, Spark usa por defecto un mínimo de **2 particiones**. Con `sc.parallelize`, el número depende de los núcleos disponibles en `local[*]`.

Cada partición se procesa como una tarea, y las tareas de un mismo *stage* corren en paralelo. Con más particiones (hasta cierto punto) se aprovechan más núcleos. Con **una sola partición** hay una única tarea en un solo núcleo, sin paralelismo, y todo el dato pasa por un solo proceso, lo que aumenta el uso de memoria. Con demasiadas particiones muy pequeñas aparece sobrecarga de planificación.

Con 30 registros la diferencia es imperceptible, pero con millones de líneas sí importaría. En el Ejercicio 1 configuré `spark.sql.shuffle.partitions=8`, un parámetro que aplica a shuffles de DataFrames y Spark SQL, no a los de `reduceByKey` sobre RDDs.

### 3. Lazy Evaluation: ¿en qué momento se ejecutaron realmente las transformaciones? ¿Qué ventajas tiene este modelo?

Las transformaciones (`map`, `filter`, `reduceByKey`, `sortBy`, `join`...) solo construyen un plan de ejecución (el *lineage*) y no procesan datos. El trabajo ocurre cuando se llama a una **acción**:

- En el proyecto, la primera acción es `rdd.take(5)` (Paso 1), que lee solo lo necesario.
- `rdd_parseado.count()` (Paso 2) ejecuta `map` y `filter` sobre todo el archivo y, con `cache()`, deja el resultado en memoria.
- Los pasos siguientes se ejecutan con `collect()`, `take()`, `first()` y `saveAsTextFile()`.

**Ventajas:** Spark ve todo el plan antes de ejecutarlo, encadena `map` y `filter` en una sola pasada, evita calcular lo que no se necesita (por ejemplo `take(5)` no procesa todo el archivo) y puede reconstruir particiones perdidas gracias al lineage.

### 4. Memoria: ¿qué pasaría si intentaras `collect()` en un RDD de 10 GB? ¿Cómo evitarías este problema?

`collect()` trae **todos** los datos al driver, cuya memoria por defecto es de aproximadamente 1 GB. Con 10 GB el programa fallaría con `OutOfMemoryError` o con un `Py4JJavaError`, como en el Error 4 del Notion y en el Ejercicio 7.

Para evitarlo:

- Usar `take(n)`, `first()` o `sample()` para revisar una muestra.
- Reducir los datos en el clúster antes de traerlos (`reduceByKey`, `aggregateByKey`, `filter`) y recoger solo el resultado, que suele ser pequeño.
- Guardar resultados a disco con `saveAsTextFile()` en lugar de traerlos al driver.
- Procesar por bloques con `foreach` o `toLocalIterator()`.
- Como último recurso, aumentar la memoria del driver (`spark.driver.memory`).

En el proyecto, `collect()` solo se usa sobre resultados ya agregados (por categoría), que son muy pequeños.

### 5. Optimización: si tuvieras que procesar 100x más datos, ¿qué cambiarías? ¿Usarías más particiones? ¿Cachearías algún RDD?

- **Más particiones:** sí, ajustándolas al número de núcleos del clúster (con `repartition`), para aprovechar el paralelismo sin crear tareas diminutas.
- **Caché:** sí, `rdd_parseado`, porque se reutiliza en los pasos 3, 4, 5 y 6. Con datos que no caben en memoria usaría `persist(StorageLevel.MEMORY_AND_DISK)` y liberaría con `unpersist()` al terminar.
- **Agregaciones:** mantener `reduceByKey` y `aggregateByKey` (combinan datos antes del shuffle) y evitar `groupByKey`. Además, calcular máximo, mínimo y promedio en una sola pasada con `aggregateByKey` en lugar de tres recorridos y un `join`.
- **Parseo:** mantener un único `split` por línea y evitar `collect()` sobre grandes volúmenes.
- **Formato y motor:** migrar a DataFrames o Spark SQL (optimizador Catalyst) y leer formatos columnares como Parquet en lugar de texto plano.
- **Almacenamiento:** mover los datos a HDFS y ejecutar en un clúster en lugar de `local[*]`.

### 6. HDFS vs Local: ¿cuáles son las ventajas de usar HDFS en lugar de archivos locales para este análisis?

- **Almacenamiento distribuido y escalable:** el archivo se divide en bloques repartidos por varios nodos, así que puede superar la capacidad de un solo disco.
- **Tolerancia a fallos:** cada bloque se replica (por defecto 3 veces), por lo que la caída de un nodo no pierde datos.
- **Localidad de datos:** Spark puede ejecutar cada tarea en el nodo donde está el bloque, lo que reduce el tráfico de red. Además, cada bloque se convierte naturalmente en una partición para lectura paralela.
- **Acceso común:** todos los workers ven la misma ruta (`hdfs://...`). Con `file://`, el archivo tendría que existir en la misma ruta en cada máquina del clúster.

Para este análisis, con 30 registros y ejecución local, un archivo local con `file://` es suficiente. Las ventajas de HDFS se notarían con volúmenes grandes y en un clúster.