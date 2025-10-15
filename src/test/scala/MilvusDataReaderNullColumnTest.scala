package com.zilliz.spark.connector

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import scala.util.Random
import io.milvus.grpc.schema.DataType
import com.zilliz.spark.connector.PKProcessor._

/**
 * Integration test for MilvusDataReader with null column values
 *
 * This test specifically verifies that the connector can handle collections where
 * one or more columns have all null values in their binlog files.
 *
 * Prerequisites:
 * - Milvus 2.6+ running at localhost:19530
 * - Minio running at localhost:9000
 * - Native library libmilvus-storage.so loaded via LD_PRELOAD
 */
class MilvusDataReaderNullColumnTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var milvusClient: MilvusClient = _

  val collectionName = s"hello_milvus_float16"
  val dim = 128
  val batchSize = 20
  val batchCount = 2

  override def beforeAll(): Unit = {
    // Initialize Spark
    spark = SparkSession.builder()
      .appName("MilvusDataReaderNullColumnTest")
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

    // Prepare test data with null column
    // prepareTestCollectionWithNullColumn()
  }

  override def afterAll(): Unit = {
    try {
      // Clean up
      // milvusClient.dropCollection("", collectionName)
    } finally {
      if (milvusClient != null) milvusClient.close()
      if (spark != null) spark.stop()
    }
  }

  test("Read collection with nullable column") {
    val config = MilvusDataReaderConfig(
      uri = "http://localhost:19530",
      token = "root:Milvus",
      collectionName = collectionName,
      options = Map(
        MilvusOption.MilvusDatabaseName -> "default",
      )
    )

    val df = MilvusDataReader.read(spark, config)

    println("\n=== Schema with Nullable Column ===")
    df.printSchema()

    println("\n=== Data Sample (showing null column) ===")
    df.show(10, truncate = false)

    // Verify row count
    val actualCount = df.count()
    val expectedCount = batchSize * batchCount
    assert(actualCount == expectedCount,
      s"Expected $expectedCount rows but got $actualCount")

    // Verify schema contains the nullable column
    val fieldNames = df.schema.fieldNames.toSet
    assert(fieldNames.contains("id"), "id field should be present")
    assert(fieldNames.contains("int64"), "int64 field should be present")
    assert(fieldNames.contains("some_null"), "nullable_int field should be present")
    assert(fieldNames.contains("all_null"), "float field should be present")
    assert(fieldNames.contains("vector"), "vector field should be present")

    // Verify all values in nullable_int column are null
    println("\n=== Verifying Null Column ===")
    val nullableIntColumn = df.select("nullable_int")
    val nullCount = nullableIntColumn.filter(nullableIntColumn("nullable_int").isNull).count()

    assert(nullCount == expectedCount,
      s"All $expectedCount rows should have null in nullable_int, but only $nullCount are null")

    println(s"✓ Successfully read $expectedCount rows")
    println(s"✓ All rows have null value in 'nullable_int' column")
    println(s"✓ No record count mismatch error occurred")
  }

  // Helper method to prepare test collection with a null column
  private def prepareTestCollectionWithNullColumn(): Unit = {
    // Drop and recreate collection
    milvusClient.dropCollection("", collectionName)

    val fields = List(
      milvusClient.createCollectionField("id", isPrimary = true, dataType = DataType.Int64, autoID = false),
      milvusClient.createCollectionField("int64", dataType = DataType.Int64),
      milvusClient.createCollectionField("all_null_int", dataType = DataType.Int64, nullable = true),
      milvusClient.createCollectionField("some_null_float", dataType = DataType.Float, nullable = true),
      milvusClient.createCollectionField("vector", dataType = DataType.FloatVector, typeParams = Map("dim" -> dim.toString))
    )

    val schema = milvusClient.createCollectionSchema(
      name = collectionName,
      fields = fields,
      description = "Test collection with null column for MilvusDataReader",
      enableAutoID = false,
      enableDynamicSchema = false
    )

    milvusClient.createCollection("", collectionName, schema, shardsNum = 1)

    // Generate and insert test data with null column
    val random = new Random(42)
    for (i <- 0 until batchCount) {
      val idData = (0 until batchSize).map(j => (i * batchSize + j).toLong)
      val int64Data = (0 until batchSize).map(j => j.toLong)

      // For nullable Int64 column - all values are None (null)
      val nullableIntData = (0 until batchSize).map(_ => None: Option[Long])

      // For nullable Float column - alternate between Some(value) and None (null)
      val floatData = (0 until batchSize).map(j =>
        if (j % 2 == 0) Some(random.nextFloat()) else None: Option[Float]
      )

      val vectorData = (0 until batchSize).map(_ =>
        (0 until dim).map(_ => random.nextFloat()).toSeq)

      val fieldsData = Seq(
        MilvusFieldData.packInt64FieldData("id", idData),
        MilvusFieldData.packInt64FieldData("int64", int64Data),
        MilvusFieldData.packNullableInt64FieldData("all_null_int", nullableIntData),
        MilvusFieldData.packNullableFloatFieldData("some_null_float", floatData),
        MilvusFieldData.packFloatVectorFieldData("vector", vectorData, dim)
      )

      println(s"\nInserting batch $i with $batchSize rows (nullable_int has ${nullableIntData.size} values - all null)")
      milvusClient.insert("", collectionName, fieldsData = fieldsData, numRows = batchSize)
    }

    // Flush to ensure data is persisted and binlogs are created
    println(s"\nFlushing collection $collectionName...")
    milvusClient.flush("", Seq(collectionName))

    println(s"\n✓ Test collection prepared with ${batchSize * batchCount} rows")
    println("✓ Column 'nullable_int' has all null values in its binlog files")

    // Verify segments were created
    println(s"Checking segments for collection $collectionName...")
    val segmentsTry = milvusClient.getSegments("", collectionName)
    segmentsTry match {
      case scala.util.Success(segments) =>
        println(s"Found ${segments.length} segments")
        segments.foreach { seg =>
          println(s"  Segment ${seg.segmentID}: state=${seg.state}, numRows=${seg.numRows}")
        }
      case scala.util.Failure(ex) =>
        println(s"Failed to get segments: ${ex.getMessage}")
    }
  }
}
