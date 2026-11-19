import os
from pyspark.sql import SparkSession

print("Ejercicio 1: SparkSession personalizada")
spark = (SparkSession.builder
        .appName("MiPrimerAnalisis")
        .master("local[*]")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())
sc = spark.sparkContext

print("Nombre app:", sc.appName)
print("Versión Spark:", spark.version)
print("Memoria executor:", spark.conf.get("spark.executor.memory"))
print("Particiones shuffle:", spark.conf.get("spark.sql.shuffle.partitions"))
print("Master:", sc.master)

print("Ejercicio 2: operaciones básicas con RDDs")
numeros = list(range(1, 21))
rdd = sc.parallelize(numeros)
print("Total de elementos:", rdd.count())        
print("Primeros 5:", rdd.take(5))                
print("Suma:", rdd.sum())                      
print("Máximo:", rdd.max(), "Mínimo:", rdd.min()) 

print("Ejercicio 3: map y filter")
numeros = sc.parallelize(range(1, 51))
por_tres = numeros.map(lambda x: x * 3)
mayores_50 = por_tres.filter(lambda x: x > 50)
print("Total mayores a 50:", mayores_50.count())  
print("Primeros 10:", mayores_50.take(10))        

print("Ejercicio 4: pares clave-valor ")
ventas = [("Laptop", 1200), ("Mouse", 25), ("Laptop", 1100),
        ("Teclado", 75), ("Mouse", 30), ("Laptop", 1300)]
rdd_ventas = sc.parallelize(ventas)

total_por_producto = rdd_ventas.reduceByKey(lambda a, b: a + b)
print("Total por producto:", dict(total_por_producto.collect()))

num_ventas = rdd_ventas.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
print("Número de ventas:", dict(num_ventas.collect()))

mejor = total_por_producto.max(key=lambda x: x[1])
print("Producto con mayor total:", mejor)         

print("Ejercicio 5: procesamiento de CSV")
ruta = "file://" + os.path.abspath("./DATOS/persons.csv")
rdd_csv = sc.textFile(ruta)

encabezado = rdd_csv.first()
sin_header = rdd_csv.filter(lambda l: l != encabezado)

def nombre_edad(linea):
    partes = linea.split(",")          
    return (partes[0], int(partes[1]))

nombres_edades = sin_header.map(nombre_edad)
mayores_30 = nombres_edades.filter(lambda x: x[1] > 30)
print("Mayores de 30:", mayores_30.collect())   
print("Total:", mayores_30.count())              

print("Ejercicio 6: optimización") 
def parsear(linea):
    nombre, edad, genero, salario = linea.split(",")
    return (nombre, int(edad), genero, int(salario))

datos = sin_header.map(parsear).cache()
nombres  = datos.map(lambda t: t[0])
edades   = datos.map(lambda t: t[1])
generos  = datos.map(lambda t: t[2])
salarios = datos.map(lambda t: t[3])

print(datos.take(3))

print("Ejercicio 7: debugging")
# Error 1: faltaba obtener el SparkContext (sc no existía)
# Error 2: ruta local sin prefijo file:// ni ruta absoluta
# Error 3: collect() trae todo al driver; se usa take() o foreach()
# Error 4: no se cerraba la sesión
spark7 = SparkSession.builder.appName("Debug").getOrCreate()
sc7 = spark7.sparkContext
rdd7 = sc7.textFile("file://" + os.path.abspath("./DATOS/persons.csv"))
for linea in rdd7.take(10):
    print(linea)
spark7.stop()