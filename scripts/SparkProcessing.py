from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract, col, split, lower, upper
from sqlalchemy import create_engine

spark = SparkSession.builder.appName("practice").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def extract_transaction_data():
    transaction_schema = StructType([
        StructField("DATE", IntegerType(), True),
        StructField("STORE_NBR", IntegerType(), True),
        StructField("LYLTY_CARD_NBR", IntegerType(), True),
        StructField("TXN_ID", IntegerType(), True),
        StructField("PROD_NBR", IntegerType(), True),
        StructField("PROD_NAME", StringType(), True),
        StructField("PROD_QTY", IntegerType(), True),
        StructField("TOT_SALES", FloatType(), True)
    ])

    transaction = spark.read.csv("file:///opt/airflow/data/QVI_Transaction_Data.csv", header=True,
                                 schema=transaction_schema)
    transaction = transaction.withColumn('DATE', F.expr("date_add('1899-12-30', DATE)").cast(DateType()))
    transaction = transaction.dropDuplicates()

    transaction = transaction.withColumn('size', regexp_extract(col('PROD_NAME'), r'(\d+)', 1).cast('int'))
    transaction = transaction.withColumn("Brand", split(col("PROD_NAME"), " ").getItem(0))

    brand_mapper = {
        'red': 'RRD',
        'ww': 'WOOLWORTHS',
        'ncc': 'NATURAL',
        'snbts': 'SUNBITES',
        'infzns': 'INFUZIONS',
        'smith': 'SMITHS',
        'dorito': 'DORITOS',
        'grain': 'GRNWVES'
    }

    def map_brand(brand):
        return brand_mapper.get(brand.lower(), brand).upper() if brand else brand

    map_brand_udf = F.udf(map_brand)
    transaction = transaction.withColumn("Brand", lower(col("Brand")))
    transaction = transaction.withColumn('Brand', map_brand_udf(F.col('Brand')))
    transaction = transaction.withColumn("Brand", upper(col("Brand")))

    # Clean product names
    transaction_final = (
        transaction.withColumn("PROD_NAME", F.lower(F.col("PROD_NAME")))
        .withColumn("PROD_NAME", F.regexp_replace(F.col("PROD_NAME"), r'\d+g', ''))
        .withColumn("PROD_NAME", F.regexp_replace(F.col("PROD_NAME"), r'\s+', ' '))
        .withColumn("PROD_NAME", F.trim(F.col("PROD_NAME")))
        .withColumn("PROD_NAME", F.initcap(F.col("PROD_NAME")))
    )

    replacement_mapper = {
        'Ww': 'Woolworths',
        'Ncc': 'Natural Chip Compny',
        'Snbts': 'Sunbites',
        'Infzns': 'Infuzions',
        'Smith': 'Smiths',
        'Dorito': 'Doritos',
        'Grain': 'Grnwves',
        'Rrd': 'Red Rock Deli'
    }

    def replace_brand(prod_name):
        for old, new in replacement_mapper.items():
            if old in prod_name:
                return prod_name.replace(old, new)
        return prod_name

    replace_brand_udf = F.udf(replace_brand)
    transaction_final = transaction_final.withColumn("PROD_NAME", replace_brand_udf(F.col("PROD_NAME")))
    transaction_final = transaction_final.filter(~F.col("PROD_NAME").contains('Salsa'))

    transaction_final.write.mode('overwrite').csv('/opt/airflow/data/processed_transaction_data', header=True)


def extract_behaviour_data():
    behaviour_schema = StructType([
        StructField("LYLTY_CARD_NBR", IntegerType(), True),
        StructField("LIFESTAGE", StringType(), True),
        StructField("PREMIUM_CUSTOMER", StringType(), True)
    ])

    # Load behaviour data
    behaviour = spark.read.csv("file:///opt/airflow/data/QVI_purchase_behaviour.csv", schema=behaviour_schema)
    behaviour.write.mode('overwrite').csv('/opt/airflow/data/processed_behaviour_data', header=True)


def combine_data():
    transaction_final = spark.read.csv("/opt/airflow/data/processed_transaction_data", header=True, inferSchema=True)

    behaviour = spark.read.csv("/opt/airflow/data/processed_behaviour_data", header=True, inferSchema=True)

    data = transaction_final.join(behaviour, on='LYLTY_CARD_NBR', how='inner')
    data.show(5)
    data.write.mode('overwrite').csv('/opt/airflow/data/data', header=True)


def load_data_to_postgres():
    df_spark = spark.read.csv("/opt/airflow/data/data", header=True, inferSchema=True)

    df_pandas = df_spark.toPandas()

    db_url = "postgresql+psycopg2://airflow:airflow@postgres:5432/Airflow"
    table_name = "combined_data"

    engine = create_engine(db_url)

    df_pandas.to_sql(table_name, engine, if_exists='replace', index=False)
