#!/bin/bash

# Criação das pastas

DADOS=("VENDAS" "CLIENTES" "ENDERECO" "REGIAO" "DIVISAO")

for i in "${DADOS[@]}"
do
	echo "$i"
    cd ../../raw/
    mkdir ../../raw/$i
    chmod 777 ../../raw/$i
    cd ../../raw/$i
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal /input/Desafio_Final_MINSAIT/raw/$i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000/desafio_curso -f /input/Desafio_Final_MINSAIT/desafio_curso/scripts/hql/create_table_$i.hql

done