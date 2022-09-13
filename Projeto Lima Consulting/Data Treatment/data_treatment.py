

from pyspark.sql.functions import col, to_date, split
from pyspark.sql.types import StringType, TimestampType, StructType, StructField
from pyspark.sql import SparkSession
from google.cloud import bigquery

# Função para tratar o output do scrapy, e enviar o DF no formato de tabela pro bigquery

# Bloco de código executado no databricks Interactive.

def bq_load(key, value):
  
  project_name = 'webscraping-362301'
  dataset_name = 'webscraping-362301.economy_notices'
  table_name = key
  
  value.to_gbq(destination_table='{economy_notices}.{fox_news}'.format(dataset_name, table_name), project_id=project_name, if_exists='replace')

#Configurando Instancia Spark
spark  = SparkSession.builder\
                  .master("local")\
                  .enableHiveSupport()\
                  .getOrCreate()

#Configurações de executores no spark.conf
spark.conf.set("spark.executor.memory", '8g')
spark.conf.set('spark.executor.cores', '3')
spark.conf.set('spark.cores.max', '3')
spark.conf.set("spark.driver.memory",'8g')
sc = spark.sparkContext

#Schema da tabela
schema = StructType([
    StructField("title", StringType()),
    StructField("byline", StringType()),
    StructField("time", TimestampType()),
    StructField("content", StringType()),
])

#Lendo arquivo de output do webscraping
df = spark.read.json('output_scrapy.json', schema=schema)


#Tratamento dos dados, removendo sujeiras e linhas duplicadas
df_final = df.withColumn('topic_content', split(col('content'), ':', 2)[1])\
           .select(col('title').alias('notice_title'),
                   col('byline').alias('notice_author'),
                   to_date(col('time')).alias('notice_date'),
                   col('topic_content')).filter((col('topic_content') != 'null') & (col("topic_content").like("@%") == False )).toPandas()


#Inserção dados BigQuery
bq_load('economy_notices.fox', df_final)

