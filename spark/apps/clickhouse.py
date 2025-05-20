from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("SalesAnalytics") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()

def load_data_from_postgres(spark):
    pg_url = "jdbc:postgresql://host.docker.internal:5432/spark_db"
    pg_properties = {
        "user": "spark_user",
        "password": "spark_password",
        "driver": "org.postgresql.Driver"
    }
    
    d_day = spark.read.jdbc(url=pg_url, table="d_day", properties=pg_properties).cache()
    d_month = spark.read.jdbc(url=pg_url, table="d_month", properties=pg_properties).cache()
    d_time = spark.read.jdbc(url=pg_url, table="d_time", properties=pg_properties).cache()
    dm_product_material = spark.read.jdbc(url=pg_url, table="dm_product_material", properties=pg_properties).cache()
    dm_product_color = spark.read.jdbc(url=pg_url, table="dm_product_color", properties=pg_properties).cache()
    dm_product_category = spark.read.jdbc(url=pg_url, table="dm_product_category", properties=pg_properties).cache()
    dm_product_brand = spark.read.jdbc(url=pg_url, table="dm_product_brand", properties=pg_properties).cache()
    dm_pet_category = spark.read.jdbc(url=pg_url, table="dm_pet_category", properties=pg_properties).cache()
    dm_country = spark.read.jdbc(url=pg_url, table="dm_country", properties=pg_properties).cache()
    dm_city = spark.read.jdbc(url=pg_url, table="dm_city", properties=pg_properties).cache()
    dm_kind = spark.read.jdbc(url=pg_url, table="dm_kind", properties=pg_properties).cache()
    dm_breed = spark.read.jdbc(url=pg_url, table="dm_breed", properties=pg_properties).cache()
    dm_pet = spark.read.jdbc(url=pg_url, table="dm_pet", properties=pg_properties).cache()
    dm_customer = spark.read.jdbc(url=pg_url, table="dm_customer", properties=pg_properties).cache()
    dm_sellers = spark.read.jdbc(url=pg_url, table="dm_sellers", properties=pg_properties).cache()
    dm_products = spark.read.jdbc(url=pg_url, table="dm_products", properties=pg_properties).cache()
    dm_stores = spark.read.jdbc(url=pg_url, table="dm_stores", properties=pg_properties).cache()
    dm_supplier = spark.read.jdbc(url=pg_url, table="dm_supplier", properties=pg_properties).cache()
    sales = spark.read.jdbc(url=pg_url, table="sales", properties=pg_properties).cache()


    tables = {
        "sales": sales,
        "dm_products": dm_products,
        "dm_product_category": dm_product_category,
        "dm_customer": dm_customer,
        "dm_country": dm_country,
        "d_time": d_time,
        "dm_stores": dm_stores,
        "dm_city": dm_city,
        "dm_supplier": dm_supplier,
        "dm_sellers": dm_sellers,
        "dm_product_brand":dm_product_brand
    }
    
    return tables

def create_product_analytics(sales, products, product_category,product_brand):
    # Топ-10 самых продаваемых продуктов
    top_products = sales.groupBy("sale_product_id") \
        .agg(
            F.sum("sale_quantity").alias("total_quantity"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("*").alias("sales_count")
        ) \
        .join(products, sales.sale_product_id == products.product_id) \
        .select(
            "product_id",
            "product_name",
            "total_quantity",
            "total_revenue",
            "sales_count",
            "product_rating",
            "product_reviews"
        ) \
        .orderBy(F.desc("total_quantity")) \
        .limit(10)
    
    # Общая выручка по категориям продуктов
    revenue_by_category = sales.join(products, sales.sale_product_id == products.product_id) \
        .join(product_category, products.product_category_id == product_category.product_category_id) \
        .groupBy("product_category_name") \
        .agg(F.sum("sale_total_price").alias("category_revenue")) \
        .orderBy(F.desc("category_revenue"))
    
    #Средний рейтинг и количество отзывов для каждого продукта.
    avg_rating_by_product = products.join(product_category, products.product_category_id==product_category.product_category_id) \
        .join(product_brand,products.product_brand_id==product_brand.product_brand_id) \
        .select(
            "product_id",
            "product_name",
            "product_brand_name",
            "product_category_name",
            "product_rating",
            "product_reviews"
        ) \
        .orderBy("product_id")
    



    return {
        "product_top_10": top_products,
        "product_revenue_by_category": revenue_by_category,
        "avg_rating_by_product": avg_rating_by_product
    }

def create_customer_analytics(sales, customers, country):
    # Топ-10 клиентов по сумме покупок
    
    aggregated_sales = sales.groupBy("sale_customer_id", "source") \
        .agg(
            F.sum("sale_total_price").alias("total_spent"),
            F.count("*").alias("purchase_count"),
            F.avg("sale_total_price").alias("avg_check")
        ).alias("agg_sales") 
    
  
    customers_alias = customers.alias("cust")
    
   
    top_customers = aggregated_sales.join(
        customers_alias,
        (F.col("agg_sales.sale_customer_id") == F.col("cust.customer_id")) &
        (F.col("agg_sales.source") == F.col("cust.source"))
    ).join(
        country, 
        F.col("cust.country_id") == country.country_id
    ).select(
        F.col("cust.customer_id"),  
        F.col("cust.source"),
        F.concat(F.col("cust.customer_first_name"), F.lit(" "), F.col("cust.customer_last_name")).alias("customer_name"),
        F.col("agg_sales.total_spent"),
        F.col("agg_sales.purchase_count"),
       "country_name"
    ).orderBy(F.desc("total_spent")) \
     .limit(10)
    
    # Распределение клиентов по странам 
    customers_by_country = customers.join(country, customers.country_id == country.country_id) \
        .groupBy("country_name") \
        .agg(F.count("*").alias("customer_count")) \
        .orderBy(F.desc("customer_count"))
    
    # Средний чек для каждого клиента 
    avg_customer_check = aggregated_sales.join(
        customers_alias,
        (F.col("agg_sales.sale_customer_id") == F.col("cust.customer_id")) &
        (F.col("agg_sales.source") == F.col("cust.source"))
    ).join(
        country, 
        F.col("cust.country_id") == country.country_id
    ).select(
        F.col("cust.customer_id"),  
        F.col("cust.source"),
        F.concat(F.col("cust.customer_first_name"), F.lit(" "), F.col("cust.customer_last_name")).alias("customer_name"),
        F.col("agg_sales.avg_check"),
        "country_name"
    ).orderBy(F.col("cust.customer_id"))

    return {
        "customer_top_10": top_customers,
        "customer_by_country": customers_by_country,
        "avg_customer_check": avg_customer_check
    }

def create_time_analytics(sales, time):
    # Месячные и годовые тренды продаж+Средний размер заказа по месяцам+Сравнение выручки за разные периоды.
    sales_with_date = sales.join(time, sales.sale_date_id == time.DateID)

    monthly_window = Window.orderBy("Year", "MonthID")
    
    monthly_trends = sales_with_date.groupBy("Year", "MonthID") \
        .agg(
            F.sum("sale_total_price").alias("monthly_revenue"),
            F.count("*").alias("sales_count"),
            F.avg("sale_total_price").alias("avg_order_size")
        ) \
        .orderBy("Year", "MonthID") \
        .withColumn("prev_month_revenue", F.coalesce(F.lag("monthly_revenue").over(monthly_window), F.lit(0.0))) \
        .withColumn("monthly_revenue_diff", F.col("monthly_revenue") - F.col("prev_month_revenue")) \
        .withColumn(
            "monthly_revenue_diff_pct",
            F.when(F.col("prev_month_revenue") != 0, (F.col("monthly_revenue_diff") / F.col("prev_month_revenue")) * 100)
              .otherwise(F.lit(0.0)) #Clickhouse наругался на null(ибо сделал все новые колонки non-nullable)), так что для визуализации будет 0.
        )
    

    yearly_window = Window.orderBy("Year")

    yearly_trends = sales_with_date.groupBy("Year") \
        .agg(
            F.sum("sale_total_price").alias("yearly_revenue"),
            F.count("*").alias("sales_count")
        ) \
        .orderBy("Year") \
        .withColumn("prev_year_revenue", F.coalesce(F.lag("yearly_revenue").over(yearly_window), F.lit(0.0))) \
        .withColumn("yearly_revenue_diff", F.col("yearly_revenue") - F.col("prev_year_revenue")) \
        .withColumn(
            "yearly_revenue_diff_pct",
            F.when(F.col("prev_year_revenue") != 0, (F.col("yearly_revenue_diff") / F.col("prev_year_revenue") * 100))
              .otherwise(F.lit(0.0))
        )

    
    return {
        "monthly_trends": monthly_trends,
        "yearly_trends": yearly_trends
    }

def create_store_analytics(sales, stores, city, country):
    # Топ-5 магазинов по выручке
    top_stores = sales.groupBy("sales_store_id") \
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("*").alias("sales_count")
        ) \
        .join(stores, sales.sales_store_id == stores.store_id) \
        .join(city, stores.city_id == city.city_id) \
        .join(country, stores.country_id == country.country_id) \
        .select(
            "store_id",
            "store_location",
            "city_name",
            "country_name",
            "store_phone",
            "store_email",
            "total_revenue",
            "sales_count"
        ) \
        .orderBy(F.desc("total_revenue")) \
        .limit(5)
    
    # Распределение продаж по городам и странам
    sales_by_location = sales.join(stores, sales.sales_store_id == stores.store_id) \
        .join(city, stores.city_id == city.city_id) \
        .join(country, stores.country_id == country.country_id) \
        .groupBy("country_name", "city_name") \
        .agg(F.sum("sale_total_price").alias("location_revenue")) \
        .orderBy(F.desc("location_revenue"))
    # Средний чек для каждого магазина.
    avg_store_check = sales.groupBy("sales_store_id") \
    .agg(
            
            F.avg("sale_total_price").alias("avg_check")
        ) \
        .join(stores, sales.sales_store_id == stores.store_id) \
        .join(city, stores.city_id == city.city_id) \
        .join(country, stores.country_id == country.country_id) \
        .select(
            "store_id",
            "store_location",
            "city_name",
            "country_name",
            "store_phone",
            "store_email",
            "avg_check"
        ) \
        .orderBy("store_id")
    return {
        "store_top_5": top_stores,
        "sales_by_location": sales_by_location,
        "avg_store_check": avg_store_check
    }

def create_supplier_analytics(sales, suppliers, products, country):
    # Топ-5 поставщиков по выручке
    sales_with_products = sales.join(
        products, 
        sales.sale_product_id == products.product_id
    )


    top_suppliers = sales_with_products.groupBy("sales_supplier_id") \
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("*").alias("sales_count")
        ) \
        .join(suppliers, sales_with_products.sales_supplier_id == suppliers.supplier_id) \
        .join(country, suppliers.country_id == country.country_id) \
        .select(
            "supplier_id",
            "supplier_name",
            "country_name",
            "total_revenue",
            "sales_count"
        ) \
        .orderBy(F.desc("total_revenue")) \
        .limit(5)
    #Средняя цена товаров от каждого поставщика.
    avg_supplier_price =  sales_with_products.groupBy("sales_supplier_id") \
        .agg(
            F.avg("product_price").alias("avg_product_price")  
        ) \
        .join(suppliers, sales_with_products.sales_supplier_id == suppliers.supplier_id) \
        .join(country, suppliers.country_id == country.country_id) \
        .select(
            "supplier_id",
            "supplier_name",
            "country_name",
            "avg_product_price"
        ) \
        .orderBy("supplier_id")
    # Распределение продаж по странам поставщиков
    sales_by_supplier_country = sales.join(suppliers, sales.sales_supplier_id == suppliers.supplier_id) \
        .join(country, suppliers.country_id == country.country_id) \
        .groupBy("country_name") \
        .agg(F.sum("sale_total_price").alias("country_revenue")) \
        .orderBy(F.desc("country_revenue"))
    
    return {
        "supplier_top_5": top_suppliers,
        "avg_supplier_price":avg_supplier_price,
        "sales_by_supplier_country": sales_by_supplier_country
    }

def create_product_quality_analytics(products, sales):
    #Корреляция между рейтингом и объемом продаж.
    product_ratings = products.join(
        sales.groupBy("sale_product_id")
            .agg(F.sum("sale_quantity").alias("total_sold")),
        products.product_id == F.col("sale_product_id"),
        "left"
    ) \
    .select(
        "product_id",
        "product_name",
        "product_rating",
        "product_reviews",
        F.coalesce("total_sold", F.lit(0)).alias("total_sold")
    )
    # Продукты с наивысшим и наименьшим рейтингом
    top_rated = product_ratings.orderBy(F.desc("product_rating")).limit(10)
    low_rated = product_ratings.orderBy("product_rating").limit(10)
    #Продукты с наибольшим количеством отзывов.
    most_rated= product_ratings.orderBy(F.desc("product_reviews")).limit(10)
    return {
        "top_rated_products": top_rated,
        "low_rated_products": low_rated,
        "product_rating_sales_correlation": product_ratings,
        "most_rated": most_rated
    }

def save_to_clickhouse(dataframes):
    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
    clickhouse_properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": "custom_user",
        "password": "custom_password"
    }
    
    for name, df in dataframes.items():
        df.write \
            .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()") \
            .jdbc(
                url=clickhouse_url,
                table=name,
                mode="overwrite",
                properties=clickhouse_properties
            )
        print(f"Saved table {name} to ClickHouse")

def main():
    spark = create_spark_session()
    

    data = load_data_from_postgres(spark)
    

    analytics = {}
    
    # 1. Витрина продаж по продуктам
    product_analytics = create_product_analytics(
        data["sales"], 
        data["dm_products"], 
        data["dm_product_category"],
        data["dm_product_brand"]
    )
    analytics.update(product_analytics)
    
    # 2. Витрина продаж по клиентам
    customer_analytics = create_customer_analytics(
        data["sales"], 
        data["dm_customer"], 
        data["dm_country"]
    )
    analytics.update(customer_analytics)
    
    # 3. Витрина продаж по времени
    time_analytics = create_time_analytics(data["sales"], data["d_time"])
    analytics.update(time_analytics)
    
    # 4. Витрина продаж по магазинам
    store_analytics = create_store_analytics(
        data["sales"], 
        data["dm_stores"], 
        data["dm_city"], 
        data["dm_country"]
    )
    analytics.update(store_analytics)
    
    # 5. Витрина продаж по поставщикам
    supplier_analytics = create_supplier_analytics(
        data["sales"], 
        data["dm_supplier"], 
        data["dm_products"], 
        data["dm_country"]
    )
    analytics.update(supplier_analytics)
    
    # 6. Витрина качества продукции
    quality_analytics = create_product_quality_analytics(
        data["dm_products"], 
        data["sales"]
    )
    analytics.update(quality_analytics)
    
  
    save_to_clickhouse(analytics)
    
    spark.stop()

if __name__ == "__main__":
    main()


'''
docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/
jars/clickhouse-jdbc-0.4.6.jar /opt/spark-apps/clickhouse.py
'''