import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration


import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Connection, Delete, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.tool.BulkLoadHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.ConnectionFactory



object Main {

  var connection: Connection = null
  var ugi: UserGroupInformation = null

  var pathHFile: String = "/tmp/HFile"
  var numPartitions: Int = 2
  var zkQuorum: String = null
  var zkClientPort: String = null
  var ingestion: String = "append"
  var dayPurge: Int = 0
  var nameTable: String = null
  var nameSpace: String = "defaul"
  var colunmFamily:String = "d"
  var nameRowkey: String = "rowkey"
  var select: String = null
  var use_api: Boolean = false
  var isKerberos: Boolean = false
  var kerberosMasterPrincipal: String = null
  var kerberosRegionserverPrincipal:String = null
  var kerberosRpcProtection:String = null
  var keyTabFile:String = null
  var keyTabUser: String = null
  var isHbaseClusterDistributed: Boolean  = false
  var isKerberosDebug: Boolean  = false
  var fileHbaseXml: String = null
  var fileHdfsXml: String = null

  val today = Calendar.getInstance.getTime
  val _YYYYMMDD_ = new SimpleDateFormat("YYYYMMdd")
  val dirDados: String = "dados"

  val confHbase = HBaseConfiguration.create()

  confHbase.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
  confHbase.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
  confHbase.set("mapreduce.job.output.key.class", "org.apache.hadoop.hbase.io.ImmutableBytesWritable")
  confHbase.set("mapreduce.job.output.value.class", "org.apache.hadoop.hbase.KeyValue")
  confHbase.set("zookeeper.session.timeout", "10000")
  confHbase.set("zookeeper.recovery.retry", "3")
  confHbase.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "2000")
  confHbase.set("fs.permissions.umask-mode", "000")

  val sparkConf = new SparkConf()
  sparkConf.registerKryoClasses(
    Array(
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2],
      classOf[org.apache.hadoop.hbase.KeyValue],
      classOf[org.apache.hadoop.hbase.spark.DefaultSource],
      classOf[org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog],
      classOf[org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage],
      classOf[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema],
      classOf[org.apache.spark.sql.catalyst.InternalRow],
      classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
      Class.forName("scala.collection.immutable.Set$EmptySet$"),
      Class.forName("scala.reflect.ClassTag$$anon$1"),
      Class.forName("java.lang.Class")
    )
  )

  val spark:SparkSession = SparkSession.builder()
    //.appName("BulkLoad")
    //.master("local")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrationRequired", "true")
    .config("spark.kryoserializer.buffer", "1024k")
    .config("spark.kryoserializer.buffer.max", "1024m")
    .config(sparkConf)
    .getOrCreate()

  @transient lazy val logger: Logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {

    val tpInicioMain = System.currentTimeMillis

    msg(Level.INFO,"Iniciando o processo")
    msg(Level.INFO,"São necessário arquivos de parâmetros {--nameTable}, {--selectOrigin}, " +
      "{--pathHFile}, {--use_api}, {--numPartitions}, {--zkQuorum},{--zkClientport}, {--hbaseTimeout},{--zkZnodeparent},{--hbaseClientScannerCaching} , o parâmetro {--colunmFamily} é opcional padrão d,o parâmetro {--ingestion} é opcional, padrão truncate, o parâmetro {--nameRowkey} é opcional, padrão rowkey,")


    try {

      if (args.length != 0) {

        for (i <- 0 until args.length) yield {
          if (args(i).contains("=")) {
            val conf = args(i).split("=")

            if ("--pathHFile".equals(conf(0))) {
              pathHFile = conf(1)
            } else if ("--numPartitions".equals(conf(0))) {
              numPartitions = conf(1).toInt
            } else if ("--zkQuorum".equals(conf(0))) {
              zkQuorum = conf(1)
            } else if ("--zkClientPort".equals(conf(0))) {
              zkClientPort = conf(1)
            } else if ("--fileHdfsXml".equals(conf(0))) {
              fileHdfsXml = conf(1)
            } else if ("--fileHbaseXml".equals(conf(0))) {
              fileHbaseXml = conf(1)
            } else if ("--use_api".equals(conf(0))) {
               use_api = conf(1).toBoolean
            }else if ("--nameTable".equals(conf(0))) {
              nameTable = conf(1)
            }else if ("--nameSpace".equals(conf(0))) {
              nameSpace = conf(1)
            } else if ("--nameRowkey".equals(conf(0))) {
              nameRowkey = conf(1)
            } else if ("--colunmFamily".equals(conf(0))) {
              colunmFamily = conf(1)
            } else if ("--isKerberos".equals(conf(0))) {
              isKerberos = conf(1).toBoolean
            } else if ("--kerberosMasterPrincipal".equals(conf(0))) {
              kerberosMasterPrincipal = conf(1)
            } else if ("--kerberosRegionserverPrincipal".equals(conf(0))) {
              kerberosRegionserverPrincipal = conf(1)
            } else if ("--kerberosRpcProtection".equals(conf(0))) {
              kerberosRpcProtection = conf(1)
            } else if ("--keyTabFile".equals(conf(0))) {
              keyTabFile = conf(1)
            } else if ("--keyTabUser".equals(conf(0))) {
              keyTabUser = conf(1)
            } else if ("--isHbaseClusterDistributed".equals(conf(0))) {
              isHbaseClusterDistributed = conf(1).toBoolean
            } else if ("--isKerberosDebug".equals(conf(0))) {
              isKerberosDebug = conf(1).toBoolean
            } else if ("--selectOrigin".equals(conf(0))) {
              var dados = ""
              for (i <- 1 until conf.length) yield {
                dados = dados + conf(i) + "="
              }
              select = dados.substring(0, dados.length - 1)
            } else if ("--ingestion".equals(conf(0))) {

              if (conf(1).toLowerCase.startsWith("expurgo")) {
                val purge = conf(1).split("\\|")
                ingestion = purge(0)
                dayPurge = Integer.valueOf(purge(1))
              } else {
                ingestion = conf(1)
              }
            }
          }
        }
      }

      if (zkQuorum != null && zkClientPort != null ) {
        confHbase.set("hbase.zookeeper.quorum", zkQuorum)
        confHbase.set("hbase.zookeeper.property.clientport", zkClientPort)
      } else {
        msg(Level.ERROR, "Parâmetro zkQuorum e zkClientPort são obrigatórios exemplo {--zkQuorum=localhost:2181}")
        return
      }

      if (nameTable != null) {
        confHbase.set(TableOutputFormat.OUTPUT_TABLE, nameTable)
        confHbase.set("hbase.mapreduce.hfileoutputformat.table.name", nameTable)
      } else {
        msg(Level.ERROR, "Parâmetro nameTable e obrigatório exemplo {--nameTable=namespace:nametable}")
        return
      }

      if (isKerberos) {

        confHbase.set("hadoop.security.authentication", "kerberos")
        confHbase.set("hbase.security.authentication", "kerberos")
        confHbase.set("hbase.cluster.distributed", String.valueOf(isHbaseClusterDistributed))
        if(kerberosRpcProtection != null && !("" == kerberosRpcProtection.trim)) {
          confHbase.set("hbase.rpc.protection", kerberosRpcProtection) //verifique esta configuração no lado do HBase
        }
        if (kerberosMasterPrincipal != null && !("" == kerberosMasterPrincipal.trim)) {
          confHbase.set("hbase.regionserver.kerberos.principal", kerberosMasterPrincipal)//qual o principal do mestre / região. uso de servidores
        }
        if (kerberosRegionserverPrincipal != null && !("" == kerberosRegionserverPrincipal.trim)){
          confHbase.set("hbase.master.kerberos.principal", kerberosRegionserverPrincipal) // isso é necessário mesmo se você se conectar através de rpc / zookeeper
        }
        if (keyTabFile != null && !("" == keyTabFile.trim)) {
          confHbase.set("hbase.regionserver.keytab.file", keyTabFile)
          confHbase.set("hbase.master.keytab.file", keyTabFile)
        }
        System.setProperty("sun.security.krb5.debug", String.valueOf(isKerberosDebug))

        UserGroupInformation.setConfiguration(confHbase)

      }

      if(fileHdfsXml != null){
        confHbase.addResource(fileHdfsXml)
      }
      if(fileHbaseXml != null){
        confHbase.addResource(fileHbaseXml)
      }

      if (ingestion == null || (!"truncate".equalsIgnoreCase(ingestion) && !"append".equalsIgnoreCase(ingestion) && !"expurgo".equalsIgnoreCase(ingestion))) {
        msg(Level.INFO, "Parametro ingestion deve ser truncate ou append exemplo {--ingestion=truncate}, {--ingestion=append} ou {--ingestion=expurgo|1}")
        return
      }

      msg(Level.INFO, "Parâmetros")
      msg(Level.INFO, "Tabela do Hbase: " + nameTable)
      msg(Level.INFO, "NameSpace do Hbase: " + nameSpace)
      msg(Level.INFO, "Familia do Hbase: " + colunmFamily)
      msg(Level.INFO, "Chave do Hbase: " + nameRowkey)
      msg(Level.INFO, "Tipo de ingestão: " + ingestion)
      if ("expurgo".equalsIgnoreCase(ingestion)) {
        msg(Level.INFO, "Dia(s) de expurgo: " + dayPurge)
      }
      msg(Level.INFO, "Diretório do HFile: " + pathHFile)
      msg(Level.INFO, "Número de particionamento: " + numPartitions)
      msg(Level.INFO, "Parâmetro para hbase zookeeper servidor: " + zkQuorum)
      msg(Level.INFO, "Parâmetro para hbase zookeeper porta: " + zkClientPort)
      msg(Level.INFO, "Parâmetro usando api: " + use_api)
      msg(Level.INFO, "Parâmetro usar kerberos: " + isKerberos)
      msg(Level.INFO, "Parâmetro kerberos master principal: " + kerberosMasterPrincipal)
      msg(Level.INFO, "Parâmetro kerberos regionserver principal: " + kerberosRegionserverPrincipal)
      msg(Level.INFO, "Parâmetro kerberos rpc protection: " + kerberosRpcProtection)
      msg(Level.INFO, "Parâmetro kerberos keyTabFile: " + keyTabFile)
      msg(Level.INFO, "Parâmetro kerberos keyTabUser: " + keyTabUser)
      msg(Level.INFO, "Parâmetro kerberos é Hbase cluster distributed: " + isHbaseClusterDistributed)
      msg(Level.INFO, "Parâmetro kerberos debug: " + isKerberosDebug)
      msg(Level.INFO,"security: " + UserGroupInformation.isSecurityEnabled)

      //val datasDataFrame = getDadosFake(select)
      val datasDataFrame = getDados(select)

      if ("truncate".equalsIgnoreCase(ingestion)) {
        truncate(nameTable)
      } else if ("expurgo".equalsIgnoreCase(ingestion) && dayPurge != null) {
        expurgo(nameTable, dayPurge)
      }

      if(use_api){
        api_write(datasDataFrame)
      }else {
        val datasRDD = mountDatas(datasDataFrame, colunmFamily, nameRowkey, numPartitions)
        val pathHFileDados = criaEstrutura(pathHFile)
        bulkLoad(datasRDD, nameTable, pathHFileDados)
      }

    msg(Level.INFO, "Fim do processo em " + formatData(System.currentTimeMillis - tpInicioMain))

    } catch {
      case e: Exception =>
        msg(Level.ERROR,e.getMessage)
        throw e
    }finally{
      if( connection != null ){
        connection.close()
      }
      if( spark != null){
        spark.close()
      }
    }
  }

  def criaEstrutura(path: String): String ={

    msg(Level.INFO,"Criando estrutura de pastas")
    val tpInicioGetDados = System.currentTimeMillis

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val pathHFileDados = new Path(path +"/"+dirDados )

    if (!fs.exists( pathHFileDados ) )
      fs.create(pathHFileDados)

    msg(Level.INFO,"Fim do processo de criaçao da estrutura de pasta {"+pathHFileDados.toString+"} " + formatData(System.currentTimeMillis - tpInicioGetDados))

    return pathHFileDados.toString
  }

  def get_catalog(cols: Array[String]): String ={

    msg(Level.INFO,"Gerando o catalog")
    val tpInicioGetDados = System.currentTimeMillis

    var resultFinal = ""
    for (i <- 0 to cols.length - 1) yield {
      if( nameRowkey.equals(cols(i)) ){
        resultFinal = resultFinal + "\"" +nameRowkey + "\":{\"cf\":\""+ nameRowkey +"\", \"col\":\"" + nameRowkey + "\", \"type\":\"string\"},"
      }else {
        resultFinal = resultFinal + "\"" + cols(i) + "\":{\"cf\":\""+ colunmFamily +"\", \"col\":\"" + cols(i) + "\", \"type\":\"string\"},"
      }
    }

    val catalog =
      s"""{"table":{"namespace":"""" + nameSpace + """", "name":"""" + nameTable +
        """"},
          |"rowkey": """".stripMargin + nameRowkey +
        """",
          |"columns":{ """.stripMargin +resultFinal.dropRight(1) + """}}""".stripMargin

    msg(Level.INFO,"Catalog: " + catalog)

    msg(Level.INFO,"Fim da geraçao do catalog " + formatData(System.currentTimeMillis - tpInicioGetDados))

    return catalog
  }

  def api_write(dados: DataFrame): Unit ={

    msg(Level.INFO,"Escrevendo hbase pela API")
    val tpInicioGetDados = System.currentTimeMillis

    val dadosLimpos = dados
      .repartition(numPartitions)
      .sortWithinPartitions(nameRowkey)

    val catalog = get_catalog(dadosLimpos.columns)

    new HBaseContext(spark.sparkContext, confHbase)

    dadosLimpos.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      //.option("hbase.spark.use.hbasecontext", false)
      .save()

    msg(Level.INFO,"Fim do processo escrita do hbase pela API " + formatData(System.currentTimeMillis - tpInicioGetDados))

  }

  def getDados(select:String): DataFrame ={

    msg(Level.INFO,"Iniciando o recuperação dos dados ")
    val tpInicioGetDados = System.currentTimeMillis

    val result = spark.sql(select)

    msg(Level.INFO, "Quantidade de registros recuperados do banco {"+result.count()+"}")

    result.show(10)

    msg(Level.INFO,"Fim do processo de recuperação dos dados " + formatData(System.currentTimeMillis - tpInicioGetDados))

    return result
  }

  def getDadosFake(select:String): DataFrame ={

    val newDF = spark.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter", ",")
      .load("file:///tmp/yellowpill_header_mais.csv")

    newDF.show()

    return newDF
  }

  def connectHbase(): Connection = {

    if (connection == null || connection.isClosed) {

      if (isKerberos) {
        if (ugi == null || UserGroupInformation.isInitialized()) {

          import org.apache.hadoop.security.UserGroupInformation
          ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keyTabUser, keyTabFile)

        } else {
          ugi.reloginFromKeytab()
        }

        connection = ugi.doAs(new PrivilegedExceptionAction[Connection]() {
          @throws[Exception]
          override def run: Connection = {
            return ConnectionFactory.createConnection(confHbase)
          }
        })

      } else {
        connection = ConnectionFactory.createConnection(confHbase)
      }
    }

    return connection
  }


  def expurgo(tableName: String, dayExpurgo: Int): Unit ={

    msg(Level.INFO,"Iniciando processo de expurgo da tabela "+tableName + " quantidade de dias " + dayExpurgo)
    val dtExpurgo = System.currentTimeMillis

    val table = connectHbase().getTable(TableName.valueOf(tableName))

    //val table = connectHbase().getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

    try {

      val calEnd = Calendar.getInstance
      calEnd.add(Calendar.DATE, -dayExpurgo)
      calEnd.set(Calendar.HOUR_OF_DAY, 23)
      calEnd.set(Calendar.MINUTE, 59)
      calEnd.set(Calendar.SECOND, 59)

      val STOP_TS = calEnd.getTimeInMillis

      val scan = new Scan()
      scan.setTimeRange(0, STOP_TS)

      val resultScanner = table.getScanner(scan)
      val result = resultScanner.iterator()

      var listDeletes: java.util.List[Delete] = new util.ArrayList[Delete]()

      while (result.hasNext) {
        listDeletes.add(new Delete(result.next().getRow))
      }
      table.delete(listDeletes)
    }finally {
      table.close()
      //connection.close()
    }

    msg(Level.INFO,"Fim do processo de expurgo da tabela " +tableName +" " + formatData(System.currentTimeMillis - dtExpurgo))
  }

  def truncate(table: String): Unit ={

    msg(Level.INFO,"Iniciando processo de truncate da tabela "+table)
    val dtTruncate = System.currentTimeMillis

    val tableName = TableName.valueOf(table)

    val admin = connectHbase().getAdmin()

    try {
      if (!admin.isTableDisabled(tableName)) {
        admin.disableTable(tableName)
      }

      admin.truncateTable(tableName, true)

      if (admin.isTableDisabled(tableName)) {
        admin.enableTable(tableName)
      }
    }finally {
      //connection.close()
    }

    msg(Level.INFO,"Fim do processo de truncate da tabela " +tableName +" " + formatData(System.currentTimeMillis - dtTruncate))
  }

  def msg(level: Level, msg:String): Unit ={
    println(msg)
    if(Level.INFO.equals(level)){
      logger.info(msg)
    }else if( Level.ERROR.equals(level) ) {
      logger.error(msg)
    }
  }

  def getYearMonthDay():String ={
    return _YYYYMMDD_.format(today)
  }

  def getYearMonth( _YYYYMMDD_ : String ): String ={
    return _YYYYMMDD_.substring(0,6)
  }

  def formatData(timeMillis: Long): String ={

    val segundo = java.util.concurrent.TimeUnit.MILLISECONDS.toSeconds(timeMillis)
    val minuto = java.util.concurrent.TimeUnit.MILLISECONDS.toMinutes(timeMillis)
    val hora = java.util.concurrent.TimeUnit.MILLISECONDS.toHours(timeMillis)
    return "hora:" + hora + ":minuto:"+minuto+":segundo:"+segundo
  }

  def mountDatas(dados: DataFrame, colunmFamily: String, nameRowkey:String, numPartitions: Int): RDD[(ImmutableBytesWritable, KeyValue)] = {

    msg(Level.INFO,"Iniciando da montagem dos dados")
    val tpInicioMountDatas = System.currentTimeMillis

    val dadosLimpos = dados.dropDuplicates(Array(nameRowkey))
      .repartition(numPartitions)
      .sortWithinPartitions(nameRowkey)

    val cols = dados.columns.sorted
    val family: Array[Byte] = colunmFamily.getBytes

    val mountDatas = dadosLimpos.rdd
      .flatMap(row => {
        val rowkey = Bytes.toBytes(row.getAs[String](nameRowkey) )
        val immutableRowKey = new ImmutableBytesWritable(rowkey)
        for (i <- 0 to cols.length - 1) yield {
          var value: Array[Byte] = null
          try {
            if(cols(i) == null){
              value = Bytes.toBytes("")
              msg(Level.ERROR,"Campo null")
            }else {
              value = Bytes.toBytes(row.getAs[String](cols(i)))
            }
          } catch {
            case e: ClassCastException =>
              value = Bytes.toBytes(row.getAs[BigInt](cols(i)) + "")
            case e: Exception =>
              msg(Level.ERROR,e.getMessage)
          }

          val qualifier = cols(i).getBytes
          val kv = new KeyValue(rowkey, family, qualifier, value)
          (immutableRowKey,kv)
        }
      })

    msg(Level.INFO,"Fim da montagem dos dados " + formatData(System.currentTimeMillis - tpInicioMountDatas))

    return mountDatas
  }

  def deleteHdfs( outPutPath: Path ): Unit = {

    msg(Level.INFO,"Inicio do processo de delete diretorio. Diretorio: " + outPutPath)
    val tpInicioDelete = System.currentTimeMillis

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)

    msg(Level.INFO,"Fim do processo de delete diretorio " + formatData(System.currentTimeMillis - tpInicioDelete))

  }

  def saveHDFS(datasRDD: RDD[(ImmutableBytesWritable, KeyValue)], pathString:String): Unit ={

    msg(Level.INFO,"Inicio do processo de salvar os dados no HDFS. Diretorio: "+ pathString)
    val tpSaveHDFS = System.currentTimeMillis

    datasRDD.saveAsNewAPIHadoopFile(pathString,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      confHbase)

    msg(Level.INFO,"Fim do processo de salvar os dados no HDFS " + formatData(System.currentTimeMillis - tpSaveHDFS))
  }

  def bulkLoad(datasRDD: RDD[(ImmutableBytesWritable, KeyValue)], tableName: String, pathString:String): Unit ={

    msg(Level.INFO,"Inicio do bulkLoad")
    val tpBulkLoad = System.currentTimeMillis

    val path = new Path(pathString)
    deleteHdfs(path)

    saveHDFS(datasRDD,pathString)

    val loader = BulkLoadHFiles.create(confHbase)
    loader.bulkLoad(TableName.valueOf(tableName), path)

    msg(Level.INFO,"Fim do bulkLoad " + formatData(System.currentTimeMillis - tpBulkLoad))

  }
}
