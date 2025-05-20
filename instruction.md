Поднимал все на винде, при необходимости исправьте postgress_url  в etl скрипте.

1. Клонируйте репозиторий и перейдите в каталог проекта:


2. Поднимите все сервисы:

   ```bash
   docker-compose up -d 
   ```
3. Загрузите начальные данные в PostgreSQL через механизм импорта cvg DBeaver.

  

4. Запустите ETL-скрипт Spark для загрузки данных в PostgreSQL и преобразования их в снежинку:

   ```bash
   docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6.jar /opt/spark-apps/etl_job.py
   ```
5. Запустите скрипт Spark для выгрузки данных из PostgreSQL в ClickHouse и последующей агрегации:

   ```bash
   docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6.jar /opt/spark-apps/clickhouse.py
   ```
6. Посмотрите таблицы в ClickHouse, например, зайдя через консоль:

   ```bash
	docker exec -it clickhouse bash
	clickhouse-client --user custom_user --password custom_password
   ```

