CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_ENDERECO ( 
Address_Number int,
City string,
Country string,
Costumer_Address_1 string,
Costumer_Address_2 string,
Costumer_Address_3 string,
Costumer_Address_4 string,
State string,
Zip_Code int
)
COMMENT 'Tabela de Enderecos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';, \t\r\n'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/datalake/raw/ENDERECO/'
TBLPROPERTIES ("skip.header.line.count"="1");


+--------------+--------------------+-------+--------------------+--------------------+--------------------+--------------------+-----+--------+
|address_number|                city|country|  costumer_address_1|  costumer_address_2|  costumer_address_3|  costumer_address_4|state|zip_code|
+--------------+--------------------+-------+--------------------+--------------------+--------------------+--------------------+-----+--------+
|             0|                City|Country|  Customer Address 1|  Customer Address 2|  Customer Address 3|  Customer Address 4|State|       0|
|      10000000|               Akron|     US|         PO Box 6258|                 ...|                 ...|                 ...|   OH|   44312|
|      10000453|                 ...|     UK|                 ...|                 ...|                 ...|                 ...|     |       0|
|      10000455|    Huntington Beach|     US|   7392 Count Circle|                 ...|                 ...|                 ...|   CA|   92647|
|      10000456|            Edmonton|     CA|    8151 Wagner Road|                 ...|                 ...|                 ...|   AB|       0|
+--------------+--------------------+-------+--------------------+--------------------+--------------------+--------------------+-----+--------+