from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType,FloatType
from faker import Faker
from pyspark.sql.functions import sum,desc
import random
from datetime import datetime,timedelta
import item

# Создание объекта SparkSession
spark = SparkSession.builder \
    .appName("StudentDataGeneration") \
    .getOrCreate()

#список из 10 товаров
data4 = [
    {"tovar_id": 1,"name":"Свинина"},
    {"tovar_id": 2,"name":"Говядина"},
    {"tovar_id": 3,"name":"Баранина"},
    {"tovar_id": 4,"name": "Индейка" },
    {"tovar_id": 5,"name": "Кета" },
    {"tovar_id": 6,"name": "Горбуша" },
    {"tovar_id": 7,"name": "Лосось" },
    {"tovar_id": 8,"name": "Щука" },
    {"tovar_id": 9,"name": "Окунь" },
    {"tovar_id": 10,"name": "Сельдь" },
]

#columns4 = ["tovar_id","name"]

names = [ item["name"] for item in data4 ]

# Определение схемы покупок для DataFrame
schema5 = StructType([
    StructField("ID", IntegerType(), nullable=False),
    StructField("DataProduct", DateType(), nullable=False),
    StructField("UserId", IntegerType(), nullable=False),
    StructField("Product", StringType(), nullable=False),
    StructField("Quantity", IntegerType(), nullable=False),
    StructField("Price", IntegerType(), nullable=False),

    ])

# Инициализация Faker для генерации данных
fake = Faker('Ru_RU')

# Задаем начальную и конечную даты
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

# Расчет количества дней между датами
delta = end_date - start_date
random_days = random.randint(0, delta.days)

# Генерация случайной даты
random_date = start_date + timedelta(days=random_days)

print(f"Случайная дата: {random_date.strftime('%Y-%m-%d')}")

#Генерация случайных данных
b = int(input("Введите число - границу массива"))
print(b)
data5 = []
for i in range(1, b):
    id = i
    userid = random.randint(1, 10)
    j = random.randint(1, 9)
    #product = names[random.randint(1, 10)]
    product = names[j]

    # Расчет количества дней между датами
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)

    # Генерация случайной даты
    random_date = start_date + timedelta(days=random_days)
    dataproduct = random_date

    quantity = random.randint(1, 12)
    price = random.randint(100, 2000)

    data5.append((id, dataproduct,userid, product, quantity, price))

# Создание DataFrame
df5 = spark.createDataFrame(data5, schema5)

# Показ первых 5 строк DataFrame
df5.show(5)

# выгрузка в csv
df5.coalesce(1).write.csv("data2.csv", header = True )

# Остановите SparkSession
spark.stop()