from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_CLIENTES")
df_divisao = spark.sql ("SELECT * FROM DESAFIO_CURSO.TBL_DIVISAO")
df_endereco = spark.sql ("SELECT * FROM DESAFIO_CURSO.TBL_ENDERECO")
df_regiao = spark.sql ("SELECT * FROM DESAFIO_CURSO.TBL_REGIAO")
df_vendas = spark.sql ("SELECT * FROM DESAFIO_CURSO.TBL_VENDAS")

# Espaço para tratar e juntar os campos e a criação do modelo dimensional

#Criando um dataframe_clientes, juntando df_clientes, df_divisao, df_endereco
dataframe_clientes = df_clientes.join (df_divisao, df_clientes.division == df_divisao.division, "inner") \
.join(df_regiao, df_clientes.region_code == df_regiao.region_code, "left") \
.join(df_endereco, df_clientes.address_number == df_endereco.address_number, "left") \
.drop(df_clientes.address_number).drop(df_divisao.division).drop(df_regiao.region_code)

+---------------+-------------+--------------------+-----------+-------------+--------+----------------+------------+-----------+------------------+-----------+-------------+-------------+--------------+--------------------+-------+--------------------+--------------------+--------------------+--------------------+-----+--------+
|business_family|business_unit|            costumer|costumerkey|costumer_type|division|line_of_business|       phone|region_code|regional_sales_mgr|search_type|division_name|  region_name|address_number|                city|country|  costumer_address_1|  costumer_address_2|  costumer_address_3|  costumer_address_4|state|zip_code|
+---------------+-------------+--------------------+-----------+-------------+--------+----------------+------------+-----------+------------------+-----------+-------------+-------------+--------------+--------------------+-------+--------------------+--------------------+--------------------+--------------------+-----+--------+
|             R3|            1|    City Supermarket|   10000000|           G2|       2|                |816-455-8733|          4|               S16|          C|     Domestic|      Central|      10000000|               Akron|     US|         PO Box 6258|                 ...|                 ...|                 ...|   OH|   44312|
|             R3|            1|       A Supermarket|   10000453|           G1|       1|                |816-455-8733|          5|               S19|          C|International|International|      10000453|                 ...|     UK|                 ...|                 ...|                 ...|                 ...|     |    null|
|             R3|            1|Caribian Supermarket|   10000455|           G2|       2|                |816-455-8733|          1|               S16|          C|     Domestic|      Western|      10000455|    Huntington Beach|     US|   7392 Count Circle|                 ...|                 ...|                 ...|   CA|   92647|
|             R1|            1|            A&B Shop|   10000456|           G3|       1|                |816-455-8733|          0|                S2|          C|International|       Canada|      10000456|            Edmonton|     CA|    8151 Wagner Road|                 ...|                 ...|                 ...|   AB|    null|
|             O2|            1|            A&G Shop|   10000457|           G1|       1|                |816-455-8733|          5|                S1|          C|International|International|          null|                null|   null|                null|                null|                null|                null| null|    null|
+---------------+-------------+--------------------+-----------+-------------+--------+----------------+------------+-----------+------------------+-----------+-------------+-------------+--------------+--------------------+-------+--------------------+--------------------+--------------------+--------------------+-----+--------+

#Preenchendo os campos Strings vazios com "Não informado" e campos numéricos vazios/null com 0
dataframe_clientes.fillna(value=0).na.fill("Não informado")

#Removendo duplicatas
dataframe_clientes.dropDuplicates

#Criando uma PrimaryKey para dataframe_clientes
dataframe_clientes = dataframe_clientes.withColumn ('PK_CLIENTE', sha2(concat_ws("", dataframe_clientes.address_number, dataframe_clientes.business_family, dataframe_clientes.business_unit, dataframe_clientes.costumer, dataframe_clientes.costumerkey, dataframe_clientes.costumer_type, dataframe_clientes.division, dataframe_clientes.line_of_business, dataframe_clientes.phone, dataframe_clientes.region_code, dataframe_clientes.regional_sales_mgr, dataframe_clientes.search_type, dataframe_clientes.division_name, dataframe_clientes.region_name, dataframe_clientes.country, dataframe_clientes.costumer_address_1, dataframe_clientes.costumer_address_2, dataframe_clientes.costumer_address_3, dataframe_clientes.costumer_address_4, dataframe_clientes.state, dataframe_clientes.zip_code), 256))


#Criando e tratando df_regiao
df_regiao = spark.sql ("SELECT * FROM DESAFIO_CURSO.TBL_REGIAO")

#Criando uma PrimaryKey para df_regiao
df_regiao = df_regiao.withColumn ('PK_REGIAO', sha2(concat_ws("", df_regiao.region_code, df_regiao.region_name), 256))

#Preenchendo campos numéricos null com 0
df_regiao = df_regiao.na.fill(value=0)

+-----------+-------------+--------------------+
|region_code|  region_name|           PK_REGIAO|
+-----------+-------------+--------------------+
|          0|  Region Name|b2655f9aa88540353...|
|          0|       Canada|b9e460e31d4f439da...|
|          1|      Western|e0d84587c4c454fe4...|
|          2|     Southern|8ac48183a39d6e7c2...|
|          3|    Northeast|4a17dcda47e8af256...|
|          4|      Central|43fae111968e55a0e...|
|          5|International|3a7a9f765776de9a6...|
+-----------+-------------+--------------------+

#Criando df_calendario
df_calendario = spark.sql("Select datekey, invoice_date from DESAFIO_CURSO.TBL_VENDAS")

#Criando PrimaryKey para df_calendario
df_calendario = df_calendario.withColumn ('PK_CALENDARIO', sha2(concat_ws("", df_calendario.datekey, df_calendario.invoice_date),256))

#Dropando duplicatas da df_calendario
df_calendario.dropDuplicates


+----------+------------+--------------------+
|   datekey|invoice_date|       PK_CALENDARIO|
+----------+------------+--------------------+
|   DateKey|Invoice Date|26da7270375a02fcf...|
|28/04/2018|  30/04/2018|ddbeb96d62492820d...|
|12/07/2018|  14/07/2018|22605565cc45d2f67...|
|15/10/2018|  17/10/2018|ef6ee0179819040a5...|
|01/06/2018|  03/06/2018|2650d859054c5c511...|
+----------+------------+--------------------+

# criando o fato
dataframe_vendas = dataframe_clientes.join (df_vendas, dataframe_clientes.costumerkey == df_vendas.costumerkey, "inner").drop(df_vendas.costumerkey)

#Criando PrimaryKey para dataframe_vendas
dataframe_vendas = dataframe_vendas.withColumn ('PK_VENDAS', sha2(concat_ws("", dataframe_vendas.address_number, dataframe_vendas.business_family, dataframe_vendas.business_unit, dataframe_vendas.costumer, dataframe_vendas.costumerkey, dataframe_vendas.costumer_type, dataframe_vendas.division, dataframe_vendas.line_of_business, dataframe_vendas.phone, dataframe_vendas.region_code, dataframe_vendas.regional_sales_mgr, dataframe_vendas.search_type, dataframe_vendas.division_name, dataframe_vendas.region_name, dataframe_vendas.country, dataframe_vendas.costumer_address_1, dataframe_vendas.costumer_address_2, dataframe_vendas.costumer_address_3, dataframe_vendas.costumer_address_4, dataframe_vendas.state, dataframe_vendas.zip_code, dataframe_vendas.actual_delivery_date, dataframe_vendas.datekey, dataframe_vendas.discount_amount, dataframe_vendas.invoice_date, dataframe_vendas.invoice_number, dataframe_vendas.item_class, dataframe_vendas.item_number, dataframe_vendas.item, dataframe_vendas.line_number, dataframe_vendas.list_price, dataframe_vendas.order_number, dataframe_vendas.promised_delivery_date, dataframe_vendas.sales_amount, dataframe_vendas.sales_amount_based_on_list_price, dataframe_vendas.sales_cost_amount, dataframe_vendas.sales_margin_amount, dataframe_vendas.sales_price, dataframe_vendas.sales_quantity, dataframe_vendas.sales_rep, dataframe_vendas.unit_of_measure), 256))

#Removendo as duplicatas
dataframe_vendas.dropDuplicates

#Preenchendo os campos Strings vazios com "Não informado" e campos numéricos vazios/null com 0
dataframe_vendas.na.fill(value=0).na.fill("Não informado")

#criando as dimensões
salvar_df(dataframe_clientes, 'dim_clientes')
salvar_df(df_regiao, 'dim_localidade')
salvar_df(df_calendario, 'dim_tempo')
salvar_df(dataframe_vendas, 'ft_vendas')

# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_hive/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_hive/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_clientes, 'dimclientes')