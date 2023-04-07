CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_VENDAS ( 
Actual_Delivery_Date string,
CostumerKey int,
DateKey string,
Discount_Amount string,
Invoice_Date string,
Invoice_Number int,
Item_Class string,
Item_Number int,
Item string,
Line_Number int,
List_Price string,
Order_Number int,
Promised_Delivery_Date string,
Sales_Amount string,
Sales_Amount_Based_on_List_Price string,
Sales_Cost_Amount string,
Sales_Margin_Amount string,
Sales_Price string,
Sales_Quantity int,
Sales_Rep int,
Unit_of_Measure string
)
COMMENT 'Tabela de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';, \t\r\n'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/datalake/raw/VENDAS/'
TBLPROPERTIES ("skip.header.line.count"="1");


+--------------------+-----------+----------+---------------+------------+--------------+----------+-----------+--------------------+-----------+----------+------------+----------------------+------------+--------------------------------+-----------------+-------------------+-----------+--------------+---------+---------------+
|actual_delivery_date|costumerkey|   datekey|discount_amount|invoice_date|invoice_number|item_class|item_number|                item|line_number|list_price|order_number|promised_delivery_date|sales_amount|sales_amount_based_on_list_price|sales_cost_amount|sales_margin_amount|sales_price|sales_quantity|sales_rep|unit_of_measure|
+--------------------+-----------+----------+---------------+------------+--------------+----------+-----------+--------------------+-----------+----------+------------+----------------------+------------+--------------------------------+-----------------+-------------------+-----------+--------------+---------+---------------+
|Actual Delivery Date|       null|   DateKey|Discount Amount|Invoice Date|Invoice Number|Item Class|       null|                Item|Line Number|List Price|Order Number|  Promised Delivery...|Sales Amount|            Sales Amount Base...|Sales Cost Amount|Sales Margin Amount|Sales Price|Sales Quantity|Sales Rep|            U/M|
|          28/04/2019|   10000481|28/04/2018|        -237,91|  30/04/2018|        100012|          |       null|    Urban Large Eggs|       2000|         0|      200015|            28/04/2019|      237,91|                               0|                0|             237,91|     237,91|             1|      184|             EA|
|          12/07/2019|   10002220|12/07/2018|         368,79|  14/07/2018|        100233|       P01|      20910|  Moms Sliced Turkey|       1000|    824,96|      200245|            12/07/2019|      456,17|                          824,96|                0|             456,17|     456,17|             1|      127|             EA|
|          14/10/2019|   10002220|15/10/2018|         109,73|  17/10/2018|        116165|       P01|      38076|Cutting Edge Foot...|       1000|    548,66|      213157|            14/10/2019|      438,93|                          548,66|                0|             438,93|     438,93|             1|      127|             EA|
|          01/06/2019|   10002489|01/06/2018|        -211,75|  03/06/2018|        100096|          |       null|            Kiwi Lox|       1000|         0|      200107|            01/06/2019|      211,75|                               0|                0|             211,75|     211,75|             1|      160|             EA|
+--------------------+-----------+----------+---------------+------------+--------------+----------+-----------+--------------------+-----------+----------+------------+----------------------+------------+--------------------------------+-----------------+-------------------+-----------+--------------+---------+---------------+