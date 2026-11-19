import os, shutil
from pyspark.sql import SparkSession

# Paso 1: Carga
spark = (SparkSession.builder
        .appName("AnalisisInventario")
        .master("local[*]")
        .config("spark.executor.memory", "512m")
        .getOrCreate())
sc = spark.sparkContext

path_final = "file://" + os.path.abspath("./DATOS/stocks.txt")
rdd = sc.textFile(path_final)
print(rdd.take(5))

# Paso 2: Parseo
def parsear_linea(linea):
    try:
        p = linea.split(",")
        return (int(p[0]), p[1].strip(), p[2].strip(), int(p[3]), float(p[4]))
    except Exception:
        return None   

rdd_parseado = rdd.map(parsear_linea).filter(lambda x: x is not None).cache()
total_registros = rdd_parseado.count()
print(f"Líneas válidas: {total_registros}")   # debe dar 30

# Paso 3: Analisis por simbolo
cat_precio = rdd_parseado.map(lambda x: (x[2], x[4]))
precio_max = cat_precio.reduceByKey(max)
precio_min = cat_precio.reduceByKey(min)
precio_promedio = (cat_precio
    .aggregateByKey((0.0, 0),
                    lambda acc, v: (acc[0] + v, acc[1] + 1),
                    lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda s: s[0] / s[1]))

print("\nPrecio máximo por categoría:")
for c, p in sorted(precio_max.collect()):
    print(f"  {c}: ${p:.2f}")
print("\nPrecio mínimo por categoría:")
for c, p in sorted(precio_min.collect()):
    print(f"  {c}: ${p:.2f}")
print("\nPrecio promedio por categoría:")
for c, p in sorted(precio_promedio.collect()):
    print(f"  {c}: ${p:.2f}")

# Paso 4: Analisis de volumen
cantidad_por_producto = (rdd_parseado.map(lambda x: (x[1], x[3]))
                        .reduceByKey(lambda a, b: a + b))
top_productos = cantidad_por_producto.sortBy(lambda x: x[1], ascending=False)
print("\nTop 10 productos con mayor cantidad:")
for nombre, cant in top_productos.take(10):
    print(f"  {nombre}: {cant:,} unidades")

# Paso 5: Analisis avanzado
volatilidad = (precio_max.join(precio_min)
               .mapValues(lambda mm: (mm[0] - mm[1]) / mm[1] * 100))
mas_volatiles = volatilidad.sortBy(lambda x: x[1], ascending=False).take(5)
mas_estables  = volatilidad.sortBy(lambda x: x[1], ascending=True).take(5)
print("\nCategorías más volátiles:")
for c, v in mas_volatiles:
    print(f"  {c}: {v:.2f}%")
print("\nCategorías más estables:")
for c, v in mas_estables:
    print(f"  {c}: {v:.2f}%")

# Paso 6: guardar resultados
os.makedirs("RESULTADOS", exist_ok=True)
for nombre, r in [("cantidad_por_producto", cantidad_por_producto),
                ("stats_por_categoria",
                precio_max.join(precio_min).join(precio_promedio))]:
    destino = os.path.abspath(f"RESULTADOS/{nombre}")
    shutil.rmtree(destino, ignore_errors=True)   # saveAsTextFile falla si existe
    r.saveAsTextFile("file://" + destino)

categorias_unicas = cat_precio.keys().distinct().count()
cat_mayor_precio = precio_promedio.sortBy(lambda x: x[1], ascending=False).first()
prod_mayor_cant = top_productos.first()
cat_mas_volatil = mas_volatiles[0]

print("\n" + "=" * 60)
print("RESUMEN EJECUTIVO - ANÁLISIS DE INVENTARIO")
print("=" * 60)
print(f"\nTotal de registros procesados: {total_registros:,}")
print(f"Número de categorías únicas: {categorias_unicas}")
print(f"Categoría con mayor precio promedio: {cat_mayor_precio[0]} (${cat_mayor_precio[1]:.2f})")
print(f"Producto con mayor cantidad: {prod_mayor_cant[0]} ({prod_mayor_cant[1]:,} unidades)")
print(f"Categoría más variable en precio: {cat_mas_volatil[0]} ({cat_mas_volatil[1]:.2f}%)")

spark.stop()