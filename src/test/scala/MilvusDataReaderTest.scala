package com.zilliz.spark.connector

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import scala.util.Random
import io.milvus.grpc.schema.DataType
import com.zilliz.spark.connector.PKProcessor._

/**
 * Integration test for MilvusDataReader with Storage V2 segments
 *
 * Prerequisites:
 * - Milvus 2.6+ running at localhost:19530
 * - Minio running at localhost:9000
 * - Native library libmilvus-storage.so loaded via LD_PRELOAD
 */
class MilvusDataReaderTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var milvusClient: MilvusClient = _

  val collectionName = s"test_datareader_v2_${System.currentTimeMillis()}"
  val dim = 128
  val batchSize = 10
  val batchCount = 3

  override def beforeAll(): Unit = {
    // Initialize Spark
    spark = SparkSession.builder()
      .appName("MilvusDataReaderTest")
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

  test("Read Storage V2 collection using MilvusDataReader") {
    val config = MilvusDataReaderConfig(
      uri = "http://localhost:19530",
      token = "root:Milvus",
      collectionName = collectionName,
      options = Map(
        MilvusOption.MilvusDatabaseName -> "default",
        "fs.endpoint" -> "localhost:9000",
        "fs.bucket_name" -> "a-bucket",
        "fs.root_path" -> "files",
        "fs.access_key_id" -> "minioadmin",
        "fs.access_key_value" -> "minioadmin",
        "fs.use_ssl" -> "false"
      )
    )

    val df = MilvusDataReader.read(spark, config)

    println("\n=== Schema ===")
    df.printSchema()

    println("\n=== Data Sample ===")
    df.show(10, truncate = false)

    // Verify row count (30 inserted - 6 deleted = 24 remaining)
    val actualCount = df.count()
    val deletedCount = 6
    val expectedCount = batchSize * batchCount - deletedCount
    assert(actualCount == expectedCount,
      s"Expected $expectedCount rows but got $actualCount (inserted ${batchSize * batchCount}, deleted $deletedCount)")

    // Verify schema - should not have row_id and timestamp columns (they are dropped)
    val fieldNames = df.schema.fieldNames.toSet
    assert(!fieldNames.contains("row_id"), "row_id should be dropped")
    assert(!fieldNames.contains("timestamp"), "timestamp should be dropped")
    assert(fieldNames.contains("id"), "id field should be present")
    assert(fieldNames.contains("int64"), "int64 field should be present")
    assert(fieldNames.contains("float"), "float field should be present")
    assert(fieldNames.contains("varchar"), "varchar field should be present")
    assert(fieldNames.contains("vector"), "vector field should be present")
  }

  test("Query Storage V2 data with Spark SQL") {
    val config = MilvusDataReaderConfig(
      uri = "http://localhost:19530",
      token = "root:Milvus",
      collectionName = collectionName,
      options = Map(
        MilvusOption.MilvusDatabaseName -> "default",
        "fs.endpoint" -> "localhost:9000",
        "fs.bucket_name" -> "a-bucket",
        "fs.root_path" -> "files",
        "fs.access_key_id" -> "minioadmin",
        "fs.access_key_value" -> "minioadmin",
        "fs.use_ssl" -> "false"
      )
    )

    val df = MilvusDataReader.read(spark, config)

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

  test("Verify delete log merging for V2 storage") {
    val config = MilvusDataReaderConfig(
      uri = "http://localhost:19530",
      token = "root:Milvus",
      collectionName = collectionName,
      options = Map(
        MilvusOption.MilvusDatabaseName -> "default",
        "fs.endpoint" -> "localhost:9000",
        "fs.bucket_name" -> "a-bucket",
        "fs.root_path" -> "files",
        "fs.access_key_id" -> "minioadmin",
        "fs.access_key_value" -> "minioadmin",
        "fs.use_ssl" -> "false"
      )
    )

    val df = MilvusDataReader.read(spark, config)

    // Deleted IDs: 0, 5, 10, 15, 20, 25
    val deletedIds = Set(0L, 5L, 10L, 15L, 20L, 25L)

    println("\n=== Verifying Delete Log Merging ===")
    println(s"Deleted IDs: ${deletedIds.mkString(", ")}")

    // Collect all IDs from the result
    val resultIds = df.select("id").collect().map(_.getLong(0)).toSet

    println(s"Total rows in result: ${resultIds.size}")
    println(s"Expected rows: ${batchSize * batchCount - deletedIds.size}")

    // Verify deleted records are not in the result
    val foundDeletedIds = deletedIds.intersect(resultIds)
    assert(foundDeletedIds.isEmpty,
      s"Found deleted IDs in result: ${foundDeletedIds.mkString(", ")}")

    // Verify all non-deleted records are present
    val allInsertedIds = (0 until batchSize * batchCount).map(_.toLong).toSet
    val expectedIds = allInsertedIds -- deletedIds
    val missingIds = expectedIds -- resultIds
    assert(missingIds.isEmpty,
      s"Missing non-deleted IDs from result: ${missingIds.mkString(", ")}")

    // Verify exact count
    assert(resultIds.size == expectedIds.size,
      s"Expected ${expectedIds.size} rows but got ${resultIds.size}")

    println(s"✓ Delete log merging works correctly")
    println(s"✓ All ${deletedIds.size} deleted records were filtered out")
    println(s"✓ All ${expectedIds.size} non-deleted records are present")
  }

  test("Verify automatic V2 format detection") {
    // Create a new client for this test to avoid gRPC timeout issues
    val testClient = MilvusClient(
      MilvusConnectionParams(
        uri = "http://localhost:19530",
        token = "root:Milvus",
        databaseName = "default"
      )
    )

    try {
      // Verify segments are V2
      val segments = testClient.getSegments("default", collectionName).get
      assert(segments.nonEmpty, "Collection should have segments")
      assert(segments.forall(_.storageVersion >= 2),
        "All segments should be V2 (storageVersion >= 2)")

      val config = MilvusDataReaderConfig(
        uri = "http://localhost:19530",
        token = "root:Milvus",
        collectionName = collectionName,
        options = Map(
          MilvusOption.MilvusDatabaseName -> "default",
          "fs.endpoint" -> "localhost:9000",
          "fs.bucket_name" -> "a-bucket",
          "fs.root_path" -> "files",
          "fs.access_key_id" -> "minioadmin",
          "fs.access_key_value" -> "minioadmin",
          "fs.use_ssl" -> "false"
        )
      )

      // Read data - should automatically detect and use storagev2 format
      val df = MilvusDataReader.read(spark, config)

      // Verify we can read the data successfully (30 inserted - 6 deleted = 24)
      val count = df.count()
      val deletedCount = 6
      val expectedCount = batchSize * batchCount - deletedCount
      assert(count == expectedCount,
        s"Should read $expectedCount rows (${batchSize * batchCount} inserted - $deletedCount deleted)")
    } finally {
      testClient.close()
    }
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
      description = "Test collection for MilvusDataReader with Storage V2",
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

    // Delete some records (delete ids: 0, 5, 10, 15, 20, 25)
    // This will test the delete log merging functionality
    val idsToDelete = Seq(0L, 5L, 10L, 15L, 20L, 25L)
    println(s"\nDeleting ${idsToDelete.size} records with ids: ${idsToDelete.mkString(", ")}")
    val deleteResult = milvusClient.delete[Long]("", collectionName, pks = idsToDelete)
    println(s"Delete result: $deleteResult")

    // Flush again to persist delete logs
    val flushResult = milvusClient.flush("", Seq(collectionName))
    println(s"Flush after delete result: $flushResult")

    // Wait a bit for segments to be sealed and storage version to be set
    Thread.sleep(2000)
  }
}
