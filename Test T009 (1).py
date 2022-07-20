# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType,LongType,DoubleType
from pyspark.sql.functions import lit,current_timestamp,col, expr, when,regexp_replace,to_date,to_timestamp

# COMMAND ----------

table_name = 'T009'
read_format = 'json'
write_format = 'delta'
database_name = 'S42'
delimiter = '^'

read_path = '/mnt/uct-landing-gen-dev/'+table_name+'/'
archive_path = '/mnt/uct-archive-gen-dev/SAP/'+database_name+'/'+table_name+'/'
write_path = '/mnt/uct-transform-gen-dev/SAP/'+database_name+'/'+table_name+'/'

stage_view = 'stg_'+table_name

# COMMAND ----------

write_path

# COMMAND ----------

dbutils.fs.ls("/mnt/")

# COMMAND ----------

# MAGIC %fs ls /mnt/uct-transform-fin-dev/

# COMMAND ----------

#df = spark.read.format(read_format) \
#      .option("header", True) \
#      .option("delimiter",delimiter) \
#     .option("InferSchema",True) \
#      .load(read_path)

# COMMAND ----------

schema = StructType([ \
                     StructField('MANDT',StringType(),True),\
StructField('PERIV',StringType(),True),\
StructField('XKALE',StringType(),True),\
StructField('XJABH',StringType(),True),\
StructField('ANZBP',StringType(),True),\
StructField('ANZSP',StringType(),True),\
StructField('XWEEK',StringType(),True),\
StructField('FYOFB',StringType(),True),\
StructField('FYOFE',StringType(),True),\
StructField('XWEEKQUART',StringType(),True),\
StructField('LandingFileTimeStamp',StringType(),True) 
                    ])

# COMMAND ----------

schema

# COMMAND ----------

df = spark.read.format(read_format) \
      .option("header", True) \
      .option("delimiter",delimiter) \
      .schema(schema) \
      .load(read_path)

# COMMAND ----------

df_add_column = df.withColumn('UpdatedOn',lit(current_timestamp())).withColumn('DataSource',lit('SAP'))

# COMMAND ----------

df_transform = df_add_column.withColumn("LandingFileTimeStamp", regexp_replace(df_add_column.LandingFileTimeStamp,'-','')) 

# COMMAND ----------

write_format

# COMMAND ----------

#df_transform.write.format(write_format).mode("overwrite").save(write_path) 

# COMMAND ----------

spark.sql("Create table IF NOT EXISTS S42.T009 USING DELTA LOCATION '/mnt/uct-transform-gen-dev/SAP/S42/T009/'")

# COMMAND ----------


#spark.sql("CREATE TABLE IF NOT EXISTS " +database_name+"."+ table_name + " USING DELTA LOCATION '" + write_path + "'")

# COMMAND ----------

df_transform.createOrReplaceTempView(stage_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO S42.T009 as T
# MAGIC USING (select * from (select RANK() OVER (PARTITION BY MANDT,PERIV,XKALE ORDER BY LandingFileTimeStamp DESC) as rn,* from stg_T009)A where A.rn = 1 ) as S 
# MAGIC ON T.MANDT = S.MANDT and
# MAGIC T.PERIV = S.PERIV and
# MAGIC T.XKALE = S.XKALE 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC T.`MANDT` =  S.`MANDT`,
# MAGIC T.`PERIV` =  S.`PERIV`,
# MAGIC T.`XKALE` =  S.`XKALE`,
# MAGIC T.`XJABH` =  S.`XJABH`,
# MAGIC T.`ANZBP` =  S.`ANZBP`,
# MAGIC T.`ANZSP` =  S.`ANZSP`,
# MAGIC T.`XWEEK` =  S.`XWEEK`,
# MAGIC T.`FYOFB` =  S.`FYOFB`,
# MAGIC T.`FYOFE` =  S.`FYOFE`,
# MAGIC T.`XWEEKQUART` =  S.`XWEEKQUART`,
# MAGIC T.`LandingFileTimeStamp` =  S.`LandingFileTimeStamp`,
# MAGIC T.`UpdatedOn` = now()
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (
# MAGIC     `MANDT`,
# MAGIC `PERIV`,
# MAGIC `XKALE`,
# MAGIC `XJABH`,
# MAGIC `ANZBP`,
# MAGIC `ANZSP`,
# MAGIC `XWEEK`,
# MAGIC `FYOFB`,
# MAGIC `FYOFE`,
# MAGIC `XWEEKQUART`,
# MAGIC `LandingFileTimeStamp`,
# MAGIC `UpdatedOn`,
# MAGIC `DataSource`
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC S.`MANDT`,
# MAGIC S.`PERIV`,
# MAGIC S.`XKALE`,
# MAGIC S.`XJABH`,
# MAGIC S.`ANZBP`,
# MAGIC S.`ANZSP`,
# MAGIC S.`XWEEK`,
# MAGIC S.`FYOFB`,
# MAGIC S.`FYOFE`,
# MAGIC S.`XWEEKQUART`,
# MAGIC S.`LandingFileTimeStamp`,
# MAGIC now(),
# MAGIC 'SAP'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM S42.T009

# COMMAND ----------


