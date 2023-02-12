from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, round, row_number, lag

spark = SparkSession.builder.appName("Covid data app").master("local[2]").getOrCreate()

# считываем csv в dataframe
df = spark.read.option("inferSchema", True).option("header", True).csv("owid-covid-data.csv")

"""
-------------------------------------------
Задание #1
Выберите 15 стран с наибольшим процентом переболевших на 31 марта (в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)
"""
print("============")
print("task 1")

#фильтруем по дате, выбираем нужные колонки, считаем процент, сортируем процент по убыванию, ограничиваемся 15 записями, пишем в task_1.csv
df.where(to_date(col('date')) == '2020-03-31').select("iso_code", "location", (round(col("total_cases") *100 / col("population"), 2)).alias("percent")).orderBy(col("percent").desc()).limit(15).coalesce(1).write.option("header", True).csv("task_1.csv")

"""
-------------------------------------------
Задание #2
Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021 в отсортированном порядке по убыванию
(в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)
"""
print("============")
print("task 2")

# выбираем нужные колонки, фильтруем по диапазону дат, для каждой из стран ранжируем по кол-ву новых случаев сортированных по убыванию, оставляем строки только с максимальным значением новых случаев для каждой из стран, 
# сортируем по убыванию, ограничиваемся 10 записями, пишем в task_2.csv 
df.select(to_date(col("date")).alias("date"), "location", "new_cases").where((col("date") >= '2021-03-24') & (col("date") <= '2021-03-31')).withColumn("row", row_number().over(Window.partitionBy("location").orderBy(col("new_cases").desc()))).where(col("row") == 1).drop("row").orderBy(col("new_cases").desc()).limit(10).coalesce(1).write.option("header", True).csv("task_2.csv")

"""
-------------------------------------------
Задание #3
Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. (например: в россии вчера было 9150 , сегодня 8763, итог: -387) (в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)
"""
print("============")
print("task 3")

# выбираем нужные колонки, фильтруем по диапазону дат (8 дней, чтобы осталось значение предыдущего дня для 2021-03-24), получаем значения предыдущего дня, убираем строчку вне нужного диапазона дат, считаем дельту, пишем в task_3.csv 
df.select(to_date(col("date")).alias("date"), col("new_cases").alias("new_cases_now"), "location").where((col("location") == "Russia") & ((col("date") >= '2021-03-23') & (col("date") <= '2021-03-31'))).withColumn("new_cases_prev", lag("new_cases_now").over(Window.partitionBy("location").orderBy(col("date").asc()))).na.drop().select("date", "new_cases_prev", "new_cases_now", (col("new_cases_now") - col("new_cases_prev")).alias("delta")).coalesce(1).write.option("header", True).csv("task_3.csv")

print("============")
spark.stop()
