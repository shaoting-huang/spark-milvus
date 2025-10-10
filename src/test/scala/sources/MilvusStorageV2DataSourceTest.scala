package com.zilliz.spark.connector.sources

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import scala.util.Random

import com.zilliz.spark.connector.{MilvusClient, MilvusConnectionParams, MilvusFieldData, MilvusOption}
import io.milvus.grpc.schema.DataType

/**
 * Integration test for MilvusStorageV2DataSource
 *
 * Prerequisites:
 * - Milvus 2.6+ running at localhost:19530
 * - Minio running at localhost:9000
 * - Native library libmilvus-storage.so loaded via LD_PRELOAD
 */
class MilvusStorageV2DataSourceTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var milvusClient: MilvusClient = _

  val collectionName = s"test_storagev2_collection_${System.currentTimeMillis()}"
  val dim = 128
  val batchSize = 10
  val batchCount = 3

  override def beforeAll(): Unit = {
    // Initialize Spark
    spark = SparkSession.builder()
      .appName("MilvusStorageV2DataSourceTest")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Initialize Milvus client
    milvusClient = MilvusClient(
      MilvusConnectionParams(
        uri = "http://localhost:19530",
        token = "root:Milvus",
        databaseName = "default"
      )
    )

    // Prepare test data
    prepareTestCollection()
  }

  override def afterAll(): Unit = {
    try {
      // Clean up
      milvusClient.dropCollection("", collectionName)
    } finally {
      if (milvusClient != null) milvusClient.close()
      if (spark != null) spark.stop()
    }
  }

  test("Read data using Storage V2 DataSource") {
    val df = spark.read
      .format("storagev2")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option("fs.endpoint", "localhost:9000")
      .option("fs.bucket_name", "a-bucket")
      .option("fs.root_path", "files")
      .option("fs.access_key_id", "minioadmin")
      .option("fs.access_key_value", "minioadmin")
      .option("fs.use_ssl", "false")
      .load()

    // Show schema
    df.printSchema()

    // Show data sample
    df.show()

    // Verify row count
    val actualCount = df.count()
    val expectedCount = batchSize * batchCount
    assert(actualCount == expectedCount,
      s"Expected $expectedCount rows but got $actualCount")
  }

  test("Read specific fields using ReaderFieldIDs") {
    val df = spark.read
      .format("storagev2")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(MilvusOption.ReaderFieldIDs, "100,102")  // read only id and float fields
      .option("fs.endpoint", "localhost:9000")
      .option("fs.bucket_name", "a-bucket")
      .option("fs.root_path", "files")
      .option("fs.access_key_id", "minioadmin")
      .option("fs.access_key_value", "minioadmin")
      .option("fs.use_ssl", "false")
      .load()

    println("\n=== Schema with Field IDs Filter ===")
    df.printSchema()

    println("\n=== Filtered Data ===")
    df.show(10, truncate = false)

    // Verify we only have the requested fields
    val fieldNames = df.schema.fieldNames.toSet
    assert(fieldNames.contains("id"), "id field should be present")
    assert(fieldNames.contains("float"), "float field should be present")
  }

  test("Query data with Spark SQL") {
    val df = spark.read
      .format("storagev2")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option("fs.endpoint", "localhost:9000")
      .option("fs.bucket_name", "a-bucket")
      .option("fs.root_path", "files")
      .option("fs.access_key_id", "minioadmin")
      .option("fs.access_key_value", "minioadmin")
      .option("fs.use_ssl", "false")
      .load()

    // Register as temp view
    df.createOrReplaceTempView("milvus_data")

    // Run SQL queries
    println("\n=== SQL Query: Filter by int64 < 5 ===")
    val filtered = spark.sql("SELECT id, int64, float FROM milvus_data WHERE int64 < 5")
    filtered.show()

    assert(filtered.count() > 0, "Filtered query should return results")

    println("\n=== SQL Query: Aggregation ===")
    val agg = spark.sql("SELECT COUNT(*) as total, AVG(float) as avg_float FROM milvus_data")
    agg.show()

    println("\nSuccessfully executed Spark SQL queries")
  }

  // Helper method to prepare test collection
  private def prepareTestCollection(): Unit = {
    // Drop and recreate collection
    milvusClient.dropCollection("", collectionName)

    val fields = List(
      milvusClient.createCollectionField("id", isPrimary = true, dataType = DataType.Int64, autoID = false),
      milvusClient.createCollectionField("int64", dataType = DataType.Int64, isClusteringKey = true),
      milvusClient.createCollectionField("float", dataType = DataType.Float),
      milvusClient.createCollectionField("varchar", dataType = DataType.VarChar, typeParams = Map("max_length" -> "1024")),
      milvusClient.createCollectionField("vector", dataType = DataType.FloatVector, typeParams = Map("dim" -> dim.toString))
    )

    val schema = milvusClient.createCollectionSchema(
      name = collectionName,
      fields = fields,
      description = "Test collection for Storage V2 DataSource",
      enableAutoID = false,
      enableDynamicSchema = false
    )

    milvusClient.createCollection("", collectionName, schema, shardsNum = 1)

    // Generate and insert test data
    val random = new Random(42)
    for (i <- 0 until batchCount) {
      val idData = (0 until batchSize).map(j => (i * batchSize + j).toLong)
      val int64Data = (0 until batchSize).map(j => j.toLong)
      val floatData = (0 until batchSize).map(_ => random.nextFloat())
      val varcharData = (0 until batchSize).map(j =>
        s"test_string_${i * batchSize + j}")
      val vectorData = (0 until batchSize).map(_ =>
        (0 until dim).map(_ => random.nextFloat()).toSeq)

      val fieldsData = Seq(
        MilvusFieldData.packInt64FieldData("id", idData),
        MilvusFieldData.packInt64FieldData("int64", int64Data),
        MilvusFieldData.packFloatFieldData("float", floatData),
        MilvusFieldData.packStringFieldData("varchar", varcharData),
        MilvusFieldData.packFloatVectorFieldData("vector", vectorData, dim)
      )

      milvusClient.insert("", collectionName, fieldsData = fieldsData, numRows = batchSize)
    }

    // Flush to ensure data is persisted
    milvusClient.flush("", Seq(collectionName))
  }
}
