# insert_hbase-spark
Insert no hbase2.3 usando spark2 e spark3


# Compilar para spark2 

Para realizar o build no spark2, deve copiar o conteúdo do arquivo build.sbt_scala2.11 para o arquivo build.sbt, e executar o comando sbt assembly, dentro do diretório onde se encontra o arquivo build.sbt. Após a execução irá gerar um jar em, target/scala-2.11/carrega_banco-assembly-1.1.jar. 

# Na chamada do submit é necessário o parâmetro 

--master local ou local[*] 

# Os parâmetros que devem ser enviados para a aplicação são: 

--pathHFile=(caminho onde irá gerar o arquivo temporário dentro do hdfs. Exemplo:  /data/stage/solar/customerdefaultscore) 

--numPartitions=(número de partes, que o processo de montagem do arquivo será dividido. Exemplo: 2)  

--zkQuorum=(host do zookeeper. Exemplo: brux000.claro.com.br)  

--zkClientPort=(porta do zookeeper. Exemplo: 2181) 

--nameTable=(nome da tabela do hbase, para o spark2 deve colocar o namespace e o nome da tabela separado pelo ‘:’. Exemplo:  ns_bus_solar:customerdefaultscore) 

--nameSpace=(nome do namespace da tabela. Exemplo: ns_bus_solar) 

--nameRowkey=(nome do key da tabela. Exemplo: rowkey)  

--colunmFamily=(nome da família da tabela. Exemplo: d) 

--use_api=(identifica se irá usar a api spark ou mapreduce. Exemplo: true) 

--ingestion=(identifica a forma que irá realizar a insert, se será truncate, append ou expurgo. Exemplo:  truncate) 

--selectOrigin=(o select da tabela de origem, teve conter o campo rowkey.  Exemplo: select campo1 as rowkey, campo2 as nome from tabela) 

 

#!/bin/bash 

 
echo "Inicio Default" 

JARFILE=`pwd`/carrega_banco-assembly-2.2.jar  

echo $JARFILE 

/opt/spark/bin/spark-submit \ 

        --name ads_bulkload_hbase_customerdefaultscore \ 

        --class Main \ 

    	--master local \ 

    	--deploy-mode client \ 

    	--conf spark.driver.cores=1 \ 

	--driver-memory 4G \ 

	--conf spark.driver.memory=2G \ 

    	--conf spark.dynamicAllocation.enabled=true \ 

    	--conf spark.dynamicAllocation.minExecutors=1 \ 

    	--conf spark.dynamicAllocation.maxExecutors=248 \ 

    	--conf spark.shuffle.service.enabled=true \ 

    	--executor-memory 2G \ 

    	--executor-cores 2 \ 

    	--conf spark.executor.memoryOverhead=2048 \ 

    	--conf spark.driver.memoryOverhead=2048 \ 

    	--conf spark.sql.shuflle.partitions=200 \ 

    	--conf spark.network.timeout=3600 \ 

    	--conf spark.executor.heartbeatInterval=1800 \ 

        --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \ 

        --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \ 

        --files `pwd`/log4j.properties \ 

	$JARFILE --pathHFile=/data/stage/solar/customerdefaultscore \ 

		--numPartitions=1 \ 

		--zkQuorum=localhost \ 

		--zkClientPort=2181 \ 

		--nameTable=ns_bus_solar:customerdefaultscore \ 

		--nameSpace=ns_bus_solar \ 

		--nameRowkey=rowkey \ 

		--colunmFamily=d \ 

		--use_api=true \ 

		--ingestion=truncate \ 

		--selectOrigin="select concat(cod_sistema_origem, '|', num_cpf_cnpj_contrato, '|', cod_operadora, '|', cod_conta_contrato_dl) as rowkey, num_ntc as cellPhone, cod_segmentacao_im as codDefault,dsc_nome_segmentacao_im as dscDefault, cod_conta_contrato_dl as contractNumber,  dat_reference as datReferenc, num_cpf_cnpj_contrato as numCpf, cod_operadora as operatorCode, cod_sistema_origem as origin, cod_cluster_im as codCluster, dsc_cluster_im as descCluster, dsc_cluster_complementar_im as descClusterCompl from db_bus_360.dl_cec_segmentacao where cod_segmentacao_im = '2'" 


# Compilar para spark3 

Para realizar o build no spark3, deve ter o diretório libs com os jars hbase-spark-protocol-shaded-1.0.1_spark-3.2.0-hbase-2.3.7-cern1_1.jar e hbase-spark-1.0.1_spark-3.2.0-hbase-2.3.7-cern1_1.jar e copiar o conteúdo do arquivo build.sbt_scala2.12 para o arquivo build.sbt, e executar o comando sbt assembly, dentro do diretório onde se encontra o arquivo build.sbt. Após a execução irá gerar um jar em, target/scala-2.12/carrega_banco-assembly-2.2.jar. 

# Na chamada do submit é necessário o parâmetro  

--packages org.apache.hbase:hbase-shaded-mapreduce:2.3.7, que irá formar o spark3 a ser executado com a lib, compatível com o hbase versão 2. 

--master local ou local[*] 

# Os parâmetros que devem ser enviados para a aplicação são: 

--pathHFile=(caminho onde irá gerar o arquivo temporário dentro do hdfs. Exemplo:  /data/stage/solar/customerdefaultscore) 

--numPartitions=(número de partes, que o processo de montagem do arquivo será dividido. Exemplo: 2)  

--zkQuorum=(host do zookeeper. Exemplo: brux000.claro.com.br)  

--zkClientPort=(porta do zookeeper. Exemplo: 2181) 

--nameTable=(nome da tabela do hbase. Exemplo:  customerdefaultscore) 

--nameSpace=(nome do namespace da tabela. Exemplo: ns_bus_solar) 

--nameRowkey=(nome do key da tabela. Exemplo: rowkey)  

--colunmFamily=(nome da família da tabela. Exemplo: d) 

--use_api=(identifica se irá usar a api spark ou mapreduce, para o spark3 tem que ser usado, logo é necessário ter o valor true. Exemplo: true) 

--ingestion=(identifica a forma que irá realizar a insert, se será truncate, append ou expurgo. Exemplo:  truncate) 

--selectOrigin=(o select da tabela de origem, teve conter o campo rowkey.  Exemplo: select campo1 as rowkey, campo2 as nome from tabela) 

 

#!/bin/bash 

 
echo "Inicio Default" 

JARFILE=`pwd`/carrega_banco-assembly-2.2.jar 
 
echo $JARFILE 

/opt/spark/bin/spark-submit \ 

        --name ads_bulkload_hbase_customerdefaultscore \ 

        --class Main \ 

    	--master local \ 

    	--deploy-mode client \ 

    	--conf spark.driver.cores=1 \ 

	--driver-memory 4G \ 

	--conf spark.driver.memory=2G \ 

    	--conf spark.dynamicAllocation.enabled=true \ 

    	--conf spark.dynamicAllocation.minExecutors=1 \ 

    	--conf spark.dynamicAllocation.maxExecutors=248 \ 

    	--conf spark.shuffle.service.enabled=true \ 

    	--executor-memory 2G \ 

    	--executor-cores 2 \ 

    	--conf spark.executor.memoryOverhead=2048 \ 

    	--conf spark.driver.memoryOverhead=2048 \ 

    	--conf spark.sql.shuflle.partitions=200 \ 

    	--conf spark.network.timeout=3600 \ 

    	--conf spark.executor.heartbeatInterval=1800 \ 

        --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \ 

        --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \ 

	--packages org.apache.hbase:hbase-shaded-mapreduce:2.3.7 \ 

        --files `pwd`/log4j.properties \ 

	$JARFILE --pathHFile=/data/stage/solar/customerdefaultscore \ 

		--numPartitions=1 \ 

		--zkQuorum=localhost \ 

		--zkClientPort=2181 \ 

		--nameTable=customerdefaultscore \ 

		--nameSpace=ns_bus_solar \ 

		--nameRowkey=rowkey \ 

		--colunmFamily=d \ 

		--use_api=true \ 

		--ingestion=truncate \ 

		--selectOrigin="select concat(cod_sistema_origem, '|', num_cpf_cnpj_contrato, '|', cod_operadora, '|', cod_conta_contrato_dl) as rowkey, num_ntc as cellPhone, cod_segmentacao_im as codDefault,dsc_nome_segmentacao_im as dscDefault, cod_conta_contrato_dl as contractNumber,  dat_reference as datReferenc, num_cpf_cnpj_contrato as numCpf, cod_operadora as operatorCode, cod_sistema_origem as origin, cod_cluster_im as codCluster, dsc_cluster_im as descCluster, dsc_cluster_complementar_im as descClusterCompl from db_bus_360.dl_cec_segmentacao where cod_segmentacao_im = '2'" 
