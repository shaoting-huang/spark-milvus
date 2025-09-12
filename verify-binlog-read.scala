import org.apache.spark.sql.SparkSession
import com.zilliz.spark.connector.{MilvusDataReader, MilvusDataReaderConfig}

object MilvusBinlogVerification {
  def main(args: Array[String]): Unit = {
    println("🚀 开始验证Milvus Binlog读取功能")
    
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("MilvusBinlogVerification")
      .master("local[*]")
      .config("spark.sql.extensions", "com.zilliz.spark.connector.extensions.VectorSearchExtensions")
      .getOrCreate()

    try {
      // 配置Milvus连接参数
      val config = MilvusDataReaderConfig(
        uri = sys.env.getOrElse("MILVUS_URI", "http://localhost:19530"),
        token = sys.env.getOrElse("MILVUS_TOKEN", "root:Milvus"),
        collectionName = sys.env.getOrElse("MILVUS_COLLECTION", "hello_spark_milvus"),
        options = Map(
          "milvus.database.name" -> "default",
          // 可以配置S3或其他存储相关参数
          "fs.s3a.access.key" -> sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""),
          "fs.s3a.secret.key" -> sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""),
          "fs.s3a.endpoint" -> sys.env.getOrElse("S3_ENDPOINT", "")
        ).filter(_._2.nonEmpty)
      )
      
      println(s"📊 连接配置:")
      println(s"  - Milvus URI: ${config.uri}")
      println(s"  - Collection: ${config.collectionName}")
      println(s"  - Database: ${config.options.getOrElse("milvus.database.name", "default")}")
      
      // 方法1: 直接使用MilvusDataReader
      println("\n📖 方法1: 使用MilvusDataReader读取binlog数据")
      try {
        val df = MilvusDataReader.read(spark, config)
        println(s"✅ 成功创建DataFrame")
        println(s"📋 Schema:")
        df.printSchema()
        
        val count = df.count()
        println(s"📊 总记录数: $count")
        
        if (count > 0) {
          println("📄 前5条记录:")
          df.show(5, truncate = false)
        }
        
      } catch {
        case e: Exception =>
          println(s"❌ MilvusDataReader失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 方法2: 直接使用format("milvusbinlog")
      println("\n📖 方法2: 直接使用milvusbinlog format")
      try {
        val binlogDF = spark.read
          .format("milvusbinlog")
          .option("milvus.uri", config.uri)
          .option("milvus.token", config.token)
          .option("milvus.collection.name", config.collectionName)
          .option("milvus.database.name", "default")
          .option("reader.type", "delete")
          .load()
          
        println(s"✅ 成功创建Binlog DataFrame")
        println(s"📋 Binlog Schema:")
        binlogDF.printSchema()
        
        val binlogCount = binlogDF.count()
        println(s"📊 Binlog记录数: $binlogCount")
        
        if (binlogCount > 0) {
          println("📄 Binlog前5条记录:")
          binlogDF.show(5, truncate = false)
        }
        
      } catch {
        case e: Exception =>
          println(s"❌ Binlog读取失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 方法3: 使用format("milvus")读取insert数据
      println("\n📖 方法3: 使用milvus format读取insert数据")
      try {
        val insertDF = spark.read
          .format("milvus")
          .option("milvus.uri", config.uri)
          .option("milvus.token", config.token)
          .option("milvus.collection.name", config.collectionName)
          .option("milvus.database.name", "default")
          .option("reader.type", "insert")
          .load()
          
        println(s"✅ 成功创建Insert DataFrame")
        println(s"📋 Insert Schema:")
        insertDF.printSchema()
        
        val insertCount = insertDF.count()
        println(s"📊 Insert记录数: $insertCount")
        
        if (insertCount > 0) {
          println("📄 Insert前5条记录:")
          insertDF.show(5, truncate = false)
        }
        
      } catch {
        case e: Exception =>
          println(s"❌ Insert读取失败: ${e.getMessage}")
          e.printStackTrace()
      }
      
      // 测试连接
      println("\n🔗 测试Milvus连接")
      try {
        import com.zilliz.spark.connector.{MilvusClient, MilvusConnectionParams}
        
        val client = new MilvusClient(
          MilvusConnectionParams(
            uri = config.uri,
            token = config.token,
            databaseName = "default"
          )
        )
        
        println("✅ Milvus客户端连接成功")
        client.close()
        
      } catch {
        case e: Exception =>
          println(s"❌ Milvus连接失败: ${e.getMessage}")
      }
      
      println("\n🎉 验证完成")
      
    } catch {
      case e: Exception =>
        println(s"💥 总体验证失败: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}