from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce
from functools import reduce
from pyspark.sql import DataFrame

def main():
    # Инициализация Spark сессии
    spark = SparkSession.builder \
        .appName("PostgresToSnowflakeETL") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()

    # Конфигурация подключения к PostgreSQL
    pg_url = "jdbc:postgresql://host.docker.internal:5432/spark_db"
    pg_properties = {
        "user": "spark_user",
        "password": "spark_password",
        "driver": "org.postgresql.Driver"
    }

    # Загрузка исходных данных
    print("Загрузка данных из PostgreSQL...")
    mock_data = spark.read.jdbc(url=pg_url, table="mock_data", properties=pg_properties)
    mock_data.cache()
    #Знаю, что задание облегчили до звезды, но я сделал по изначальной модели с прошлой лабы, а там была снежинка.
    # 1. Создание таблицы d_day
    print("Создание таблицы d_day...")
    days = [(1, 'Monday'), (2, 'Tuesday'), (3, 'Wednesday'), (4, 'Thursday'),
            (5, 'Friday'), (6, 'Saturday'), (7, 'Sunday')]
    d_day = spark.createDataFrame(days, ["DayID", "DayName"])
    
    # 2. Создание таблицы d_month
    print("Создание таблицы d_month...")
    months = [(i+1, month) for i, month in enumerate([
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ])]
    d_month = spark.createDataFrame(months, ["MonthID", "MonthName"])

    # 3. Создание таблицы d_time
    print("Создание таблицы d_time...")
    dates_df = mock_data.select("sale_date").distinct() \
        .union(mock_data.select("product_release_date").distinct()) \
        .union(mock_data.select("product_expiry_date").distinct()) \
        .distinct()
    
    d_time = dates_df.withColumnRenamed("sale_date", "dt") \
        .withColumn("DateID", F.monotonically_increasing_id()) \
        .withColumn("DayID", F.dayofweek("dt") + 1) \
        .withColumn("MonthID", F.month("dt")) \
        .withColumn("Year", F.year("dt")) \
        .select("DateID", "dt", "DayID", "MonthID", "Year")

    # 4. Создание справочников товаров
    print("Создание справочников товаров...")
    dm_product_category = mock_data.select("product_category").distinct() \
        .withColumn("product_category_id", F.monotonically_increasing_id()) \
        .select("product_category_id", F.col("product_category").alias("product_category_name"))

    dm_pet_category = mock_data.select("pet_category").distinct() \
        .withColumn("pet_category_id", F.monotonically_increasing_id()) \
        .select("pet_category_id", F.col("pet_category").alias("pet_category_name"))

    dm_product_color = mock_data.select("product_color").distinct() \
        .withColumn("product_color_id", F.monotonically_increasing_id()) \
        .select("product_color_id", F.col("product_color").alias("product_color_name"))

    dm_product_brand = mock_data.select("product_brand").distinct() \
        .withColumn("product_brand_id", F.monotonically_increasing_id()) \
        .select("product_brand_id", F.col("product_brand").alias("product_brand_name"))

    dm_product_material = mock_data.select("product_material").distinct() \
        .withColumn("product_material_id", F.monotonically_increasing_id()) \
        .select("product_material_id", F.col("product_material").alias("product_material_name"))

    # 5. Создание таблицы dm_country 
    print("Создание таблицы dm_country...")
    country_columns = ["seller_country", "customer_country", "store_country", "supplier_country"]
    country_dfs = [
        mock_data.select(F.col(col).alias("country_name")).filter(F.col(col).isNotNull())
        for col in country_columns
    ]
    dm_country = reduce(DataFrame.unionAll, country_dfs).distinct() \
        .withColumn("country_id", F.row_number().over(Window.orderBy("country_name")))

    # 6. Создание таблицы dm_city 
    print("Создание таблицы dm_city...")
    city_columns = ["store_city", "supplier_city"]
    city_dfs = [
        mock_data.select(F.col(col).alias("city_name")).filter(F.col(col).isNotNull())
        for col in city_columns
    ]
    dm_city = reduce(DataFrame.unionAll, city_dfs).distinct() \
        .withColumn("city_id", F.row_number().over(Window.orderBy("city_name")))

    # 7. Создание таблицы dm_kind (типы питомцев)
    print("Создание таблицы dm_kind...")
    dm_kind = mock_data.select("customer_pet_type").distinct() \
    .withColumn("kind_id", F.monotonically_increasing_id()) \
    .withColumnRenamed("customer_pet_type", "kind_name") \
    .select("kind_id", "kind_name")

    # 8. Создание таблицы dm_breed (породы)
    print("Создание таблицы dm_breed...")
    dm_breed = mock_data.select("customer_pet_breed").distinct() \
    .withColumn("breed_id", F.monotonically_increasing_id()) \
    .withColumnRenamed("customer_pet_breed", "breed_name") \
    .select("breed_id", "breed_name")

   # 9. Создание таблицы dm_pet 
    print("Создание таблицы dm_pet...")
    dm_pet = mock_data.select(
        "customer_pet_type",
        "customer_pet_breed", 
        "customer_pet_name"
    ).distinct() \
    .join(
        dm_kind.hint("broadcast"),
        F.col("customer_pet_type") == F.col("kind_name"),
        "left"
    ) \
    .join(
        dm_breed.hint("broadcast"),
        F.col("customer_pet_breed") == F.col("breed_name"),
        "left"
    ) \
    .withColumn("pet_id", F.monotonically_increasing_id()) \
    .select(
        "pet_id",
        F.col("kind_id").alias("type_id"),
        "breed_id",
        F.col("customer_pet_name").alias("pet_name")
    )

    # 10. Создание таблицы dm_customer
    print("Создание таблицы dm_customer...")
    print("Создание таблицы dm_customer...")
    dm_customer = mock_data.select(
        F.col("id").alias("customer_id"),
        "source",
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_breed",
        "customer_pet_name"
    ).distinct()

    # 10.1. Получаем kind_id из dm_kind
    dm_customer = dm_customer.alias("base").join(
        F.broadcast(dm_kind.alias("kind")),
        F.col("base.customer_pet_type") == F.col("kind.kind_name"),
        "left"
    ).select(
        "base.*",
        F.col("kind.kind_id").alias("type_id") 
    )

    # 10.2. Получаем breed_id из dm_breed
    dm_customer = dm_customer.alias("with_type").join(
        F.broadcast(dm_breed.alias("breed")),
        F.col("with_type.customer_pet_breed") == F.col("breed.breed_name"),
        "left"
    ).select(
        "with_type.*",
        F.col("breed.breed_id")  
    )

    # 10.3. Ищем pet_id в dm_pet
    dm_customer = dm_customer.alias("with_breed").join(
        F.broadcast(dm_pet.alias("pet")),
        (F.col("with_breed.type_id") == F.col("pet.type_id")) &
        (F.col("with_breed.breed_id") == F.col("pet.breed_id")) &
        (F.col("with_breed.customer_pet_name") == F.col("pet.pet_name")),
        "left"
    ).select(
        "with_breed.*",
        F.col("pet.pet_id")
    )

    # 10.4. Получаем country_id из dm_country
    dm_customer = dm_customer.alias("with_pet").join(
        F.broadcast(dm_country.alias("country")),
        F.col("with_pet.customer_country") == F.col("country.country_name"),
        "left"
    ).select(
        "with_pet.customer_id",
        "with_pet.source",
        "with_pet.customer_first_name",
        "with_pet.customer_last_name",
        "with_pet.customer_age",
        "with_pet.customer_email",
        "with_pet.customer_postal_code",
        F.col("country.country_id"),
        F.col("with_pet.pet_id")
    )

    # 10.5. Результирующая схема
    dm_customer = dm_customer.select(
        "customer_id",
        "source",
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "country_id",
        "customer_postal_code",
        "pet_id"
    )


    # 11. Создание таблицы dm_sellers
    print("Создание таблицы dm_sellers...")
    dm_sellers = mock_data.select(
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code"
    ).distinct() \
    .join(dm_country, F.col("seller_country") == F.col("country_name"), "left") \
    .withColumn("seller_id", F.monotonically_increasing_id()) \
    .select(
        "seller_id",
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "country_id",
        "seller_postal_code"
    )

    # 12. Создание таблицы dm_products 
    print("Создание таблицы dm_products...")
    dm_products = mock_data.select(
        "product_name",
        "product_category",
        "product_price",
        "product_quantity",
        "pet_category",
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date"
    ).distinct() \
    .join(
        dm_product_category,
        F.col("product_category") == F.col("product_category_name"),
        "left"
    ) \
    .join(
        dm_pet_category,
        F.col("pet_category") == F.col("pet_category_name"),
        "left"
    ) \
    .join(
        dm_product_color,
        F.col("product_color") == F.col("product_color_name"),
        "left"
    ) \
    .join(
        dm_product_brand,
        F.col("product_brand") == F.col("product_brand_name"),
        "left"
    ) \
    .join(
        dm_product_material,
        F.col("product_material") == F.col("product_material_name"),
        "left"
    ) \
    .join(
        d_time.alias("release_time"),
        F.col("product_release_date") == F.col("release_time.dt"),
        "left"
    ) \
    .withColumn("product_release_date_id", F.col("release_time.DateID")) \
    .drop("release_time.dt", "release_time.DayID", "release_time.MonthID", "release_time.Year") \
    .join(
        d_time.alias("expiry_time"),
        F.col("product_expiry_date") == F.col("expiry_time.dt"),
        "left"
    ) \
    .withColumn("product_expiry_date_id", F.col("expiry_time.DateID")) \
    .drop("expiry_time.dt", "expiry_time.DayID", "expiry_time.MonthID", "expiry_time.Year") \
    .withColumn("product_id", F.monotonically_increasing_id()) \
    .select(
        "product_id",
        "product_name",
        "product_category_id",
        "product_price",
        "product_quantity",
        "pet_category_id",
        "product_weight",
        "product_color_id",
        "product_size",
        "product_brand_id",
        "product_material_id",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date_id",
        "product_expiry_date_id"
    )

    # 13. Создание таблицы dm_stores
    print("Создание таблицы dm_stores...")
    dm_stores = mock_data.select(
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email"
    ).distinct() \
    .join(dm_country, F.col("store_country") == F.col("country_name"), "left") \
    .join(dm_city, F.col("store_city") == F.col("city_name"), "left") \
    .withColumn("store_id", F.monotonically_increasing_id()) \
    .select(
        "store_id",
        "store_location",
        "city_id",
        "store_state",
        "country_id",
        "store_phone",
        "store_email"
    )

    # 14. Создание таблицы dm_supplier
    print("Создание таблицы dm_supplier...")
    dm_supplier = mock_data.select(
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country"
    ).distinct() \
    .join(dm_country, F.col("supplier_country") == F.col("country_name"), "left") \
    .join(dm_city, F.col("supplier_city") == F.col("city_name"), "left") \
    .withColumn("supplier_id", F.monotonically_increasing_id()) \
    .select(
        "supplier_id",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "city_id",
        "country_id"
    )

    
    # 15. Создание фактовой таблицы sales 
    print("Создание таблицы dm_stores...")
    sales = (
        mock_data.alias("md")
        .select(
            "sale_date",
            "id",
            "source",
            "seller_email",
            "product_name",
            "product_category",
            "product_price",
            "store_email",
            "supplier_email",
            "sale_quantity",
            "sale_total_price"
        )
        .distinct()
        .join(
            d_time.alias("sale_time"),
            F.col("md.sale_date") == F.col("sale_time.dt"),
            "left"
        )
        .withColumn("sale_date_id", F.col("sale_time.DateID"))
        .drop("sale_time.dt", "sale_time.DayID", "sale_time.MonthID", "sale_time.Year")
        .join(
            dm_customer.alias("cust"),
            (F.col("md.id") == F.col("cust.customer_id")) & 
            (F.col("md.source") == F.col("cust.source")),
            "left"
        )
        .join(
            dm_sellers.alias("sell"),
            F.col("md.seller_email") == F.col("sell.seller_email"),
            "left"
        )
        .join(
            dm_products.alias("prod"),
            (F.col("md.product_name") == F.col("prod.product_name")) & 
            (F.col("md.product_price") == F.col("prod.product_price")),
            "left"
        )
        # Явно подключаем dm_product_category через broadcast
        .join(
            F.broadcast(dm_product_category).alias("dpc"),
            F.col("md.product_category") == F.col("dpc.product_category_name"),
            "left"
        )
        .where(F.col("prod.product_category_id") == F.col("dpc.product_category_id"))
        .join(
            dm_stores.alias("stor"),
            F.col("md.store_email") == F.col("stor.store_email"),
            "left"
        )
        .join(
            dm_supplier.alias("supp"),
            F.col("md.supplier_email") == F.col("supp.supplier_email"),
            "left"
        )
        .withColumn("sale_id", F.monotonically_increasing_id())
        .select(
            "sale_id",
            "sale_date_id",
            F.col("cust.customer_id").alias("sale_customer_id"),
            "md.source",
            F.col("sell.seller_id").alias("sale_seller_id"),
            F.col("prod.product_id").alias("sale_product_id"),
            F.col("stor.store_id").alias("sales_store_id"),
            F.col("supp.supplier_id").alias("sales_supplier_id"),
            "sale_quantity",
            "sale_total_price"
        )
    )

    # Функция для записи данных в PostgreSQL
    def write_to_postgres(df, table_name):
        print(f"Запись таблицы {table_name} в PostgreSQL...")
        df.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", table_name) \
            .option("user", pg_properties["user"]) \
            .option("password", pg_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

  

    
    tables = [
        (d_day, "d_day"),
        (d_month, "d_month"),
        (d_time, "d_time"),
        (dm_product_material,"dm_product_material"),
        (dm_product_color,"dm_product_color"),
        (dm_product_category,"dm_product_category"),
        (dm_product_brand,"dm_product_brand"),
        (dm_pet_category,"dm_pet_category"),
        (dm_country, "dm_country"),
        (dm_city, "dm_city"),
        (dm_kind, "dm_kind"),
        (dm_breed, "dm_breed"),
        (dm_pet, "dm_pet"),
        (dm_customer, "dm_customer"),
        (dm_sellers, "dm_sellers"),
        (dm_products, "dm_products"),
        (dm_stores, "dm_stores"),
        (dm_supplier, "dm_supplier"),
        (sales, "sales")
    ]


    for df, table in tables:
        write_to_postgres(df, table)
        
  
    print("ETL процесс успешно завершен!")

if __name__ == "__main__":
    main()