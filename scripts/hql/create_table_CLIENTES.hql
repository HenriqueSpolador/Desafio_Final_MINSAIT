    CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_CLIENTES ( 
    Address_Number int,
    Business_Family string,
    Business_Unit int,
    Costumer string,
    CostumerKey int,
    Costumer_Type string,
    Division int,
    Line_of_Business string,
    Phone string,
    Region_Code int,
    Regional_Sales_Mgr string,
    Search_Type string
    )
    COMMENT 'Tabela de Clientes'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ';'
    STORED AS TEXTFILE
    location '/datalake/raw/CLIENTES/'
    TBLPROPERTIES ("skip.header.line.count"="1");

+---------------+-------------+----------------+-----------+-------------+--------+----------------+------------+-----------+------------------+-----------+-------------+-----------+--------------+-----+-------+------------------+--------------------+--------------------+--------------------+-----+--------+
|business_family|business_unit|        costumer|costumerkey|costumer_type|division|line_of_business|       phone|region_code|regional_sales_mgr|search_type|division_name|region_name|address_number| city|country|costumer_address_1|  costumer_address_2|  costumer_address_3|  costumer_address_4|state|zip_code|
+---------------+-------------+----------------+-----------+-------------+--------+----------------+------------+-----------+------------------+-----------+-------------+-----------+--------------+-----+-------+------------------+--------------------+--------------------+--------------------+-----+--------+
|             R3|            1|City Supermarket|   10000000|           G2|       2|                |816-455-8733|          4|               S16|          C|     Domestic|    Central|      10000000|Akron|     US|       PO Box 6258|                 ...|                 ...|                 ...|   OH|   44312|
+---------------+-------------+----------------+-----------+-------------+--------+----------------+------------+-----------+------------------+-----------+-------------+-----------+--------------+-----+-------+------------------+--------------------+--------------------+--------------------+-----+--------+

