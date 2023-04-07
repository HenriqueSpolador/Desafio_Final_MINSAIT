O Presente Repositório tem como objetivo criar e prover dados para ser consumido por um relatório em PowerBI.

LINGUAGENS UTILIZADAS: HiveHQL, Python, Shell Script, SQL

Foi criado um Shell Script para criar as pastas /datalake/raw e para enviar os arquivos para o HDFS
Com um script de repetição, foram criados arquivos HQL, onde dentro destes, foram criadas estruturas para a criação de tabelas.
Estas tabelas foram feitas dentro do Hive.
Em seguida, os dados foram processados e tratados no Spark, sendo criados dataframes e salvos como .csv na pasta gold.
Logo após, os dados foram trabalhados no PowerBI, sendo criadas tabelas e gráficos de vendas.
Ao final os dados são utilizados por empresas para ajudar a tomarem as melhores decisões e auxiliam a desenvolver estratégias eficientes para melhorar a performance da empresa.