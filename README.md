# nastavnik_dz_YetAnotherGoida

### Решение

Установим из `docker-compose.yaml` файла необходимые сервисы

Подготовим HDFS:

Создадим директорию
```powershell
docker exec namenode hdfs dfs -mkdir -p /tmp/sandbox_zeppelin
```
Установите права на запись 
```powershell
docker exec namenode hdfs dfs -chmod 777 /tmp/sandbox_zeppelin
```
Проверим 
```powershell
docker exec namenode hdfs dfs -ls /tmp/sandbox_zeppelin
```

Откроем UI Zeppelin http://localhost:8082

настроим интерпретатор Spark в Zeppelin, добавим:

- `org.apache.hadoop:hadoop-client:3.2.1`

- `zeppelin.interpreter.connect.timeout = 300000`
(таймаут подключения до 5 минут)

- `spark.hadoop.fs.defaultFS = hdfs://namenode:8020`
(для указания HDFS)

<img src="./assets/2026-03-03 183938.jpg" width="700"> 

<img src="./assets/2026-03-03 183959.jpg" width="700"> 


Проверим что spark работает

<img src="./assets/2026-03-03 175057.jpg" width="700"> 

Создадим ноутбук `mart_city_top_products` и создадим таблицы в первой ячейке
```python
%spark.pyspark
from pyspark.sql import functions as F

users = spark.createDataFrame(
    [
        ("u1", "Berlin"),
        ("u2", "Berlin"),
        ("u3", "Munich"),
        ("u4", "Hamburg")
    ],
    ["user_id", "city"]
)

orders = spark.createDataFrame(
    [
        ("o1", "u1", "p1", 2, 10.0),
        ("o2", "u1", "p2", 1, 30.0),
        ("o3", "u2", "p1", 1, 10.0),
        ("o4", "u2", "p3", 5, 7.0),
        ("o5", "u3", "p2", 3, 30.0),
        ("o6", "u3", "p3", 1, 7.0),
        ("o7", "u4", "p1", 10, 10.0)
    ],
    ["order_id", "user_id", "product_id", "qty", "price"]
)

products = spark.createDataFrame(
    [
        ("p1", "Ring VOLA"),
        ("p2", "Ring POROG"),
        ("p3", "Ring TISHINA")
    ],
    ["product_id", "product_name"]
)

# Проверяем
print("Users:")
users.show()
print("Orders:")
orders.show()
print("Products:")
products.show()
```
<img src="./assets/2026-03-03 175914.jpg" width="700"> 
<img src="./assets/2026-03-03 175937.jpg" width="700"> 

Расчёт revenue
```python
%spark.pyspark
orders_with_revenue = orders.withColumn("revenue", F.col("qty") * F.col("price"))
orders_with_revenue.show()
```
<img src="./assets/2026-03-03 180031.jpg" width="700"> 

Объединение таблиц (join)
```python
%spark.pyspark
orders_users = orders_with_revenue.join(users, on="user_id", how="left")
full_df = orders_users.join(products, on="product_id", how="left")
full_df.show()
```
<img src="./assets/2026-03-03 180147.jpg" width="700"> 

Агрегация по городу и товару
```%spark.pyspark
agg_df = full_df.groupBy("city", "product_id", "product_name").agg(
    F.count("*").alias("orders_cnt"),
    F.sum("qty").alias("qty_sum"),
    F.sum("revenue").alias("revenue_sum")
)
agg_df.show()
```

<img src="./assets/2026-03-03 180546.jpg" width="700"> 

Топ 2 товара по выручке в каждом городе
```python
%spark.pyspark
from pyspark.sql.window import Window

window_spec = Window.partitionBy("city").orderBy(F.col("revenue_sum").desc())

top_df = agg_df.withColumn("rn", F.row_number().over(window_spec)) \
               .filter(F.col("rn") <= 2) \
               .drop("rn")
top_df.show()
```

<img src="./assets/2026-03-03 180649.jpg" width="700"> 

Запись в HDFS
```python
%spark.pyspark
hdfs_path = "hdfs://namenode:8020/tmp/sandbox_zeppelin/mart_city_top_products/"
top_df.write.mode("overwrite").parquet(hdfs_path)
print(f"Data written to {hdfs_path}")
```
<img src="./assets/2026-03-03 183203.jpg" width="700"> 

Чтение обратно и проверка
```python
%spark.pyspark
check_df = spark.read.parquet("hdfs://namenode:8020/tmp/sandbox_zeppelin/mart_city_top_products/")
check_df.show()
```

<img src="./assets/2026-03-03 183351.jpg" width="700"> 

Посмотрим наличие файлов

<img src="./assets/2026-03-03 183514.jpg" width="700"> 
