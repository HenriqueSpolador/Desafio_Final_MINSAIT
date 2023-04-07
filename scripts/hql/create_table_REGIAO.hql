CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_REGIAO (
Region_Code int,
Region_Name string
)
COMMENT 'Tabela de Regiao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/REGIAO/'
TBLPROPERTIES ("skip.header.line.count"="1");


+-----------+-----------+
|region_code|region_name|
+-----------+-----------+
|       null|Region Name|
|          0|     Canada|
|          1|    Western|
|          2|   Southern|
|          3|  Northeast|
+-----------+-----------+