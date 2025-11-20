package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import io.milvus.grpc.schema.DataType
import scala.util.{Success, Failure}

/**
 * Test suite for MilvusSnapshotReader
 */
class MilvusSnapshotReaderTest extends AnyFunSuite with Matchers {

  val sampleSnapshotJson: String = """{
  "snapshot-info": {
    "name": "backfill_snapshot",
    "id": 462324574599774209,
    "description": "add field backfill snapshot",
    "collection_id": 462324574592960519,
    "partition_ids": [
      462324574592960520
    ],
    "create_ts": 462324677975474190
  },
  "collection": {
    "schema": {
      "name": "backfilltestcollection",
      "description": "Test collection for MilvusBackfill",
      "fields": [
        {
          "fieldID": 100,
          "name": "id",
          "is_primary_key": true,
          "data_type": 5
        },
        {
          "fieldID": 101,
          "name": "int64",
          "data_type": 5,
          "is_clustering_key": true
        },
        {
          "fieldID": 102,
          "name": "float",
          "data_type": 10
        },
        {
          "fieldID": 103,
          "name": "varchar",
          "data_type": 21,
          "type_params": [
            {
              "key": "max_length",
              "value": "1024"
            }
          ]
        },
        {
          "fieldID": 104,
          "name": "vector",
          "data_type": 101,
          "type_params": [
            {
              "key": "dim",
              "value": "128"
            }
          ]
        },
        {
          "name": "RowID",
          "description": "row id",
          "data_type": 5
        },
        {
          "fieldID": 1,
          "name": "Timestamp",
          "description": "time stamp",
          "data_type": 5
        }
      ],
      "properties": [
        {
          "key": "timezone",
          "value": "UTC"
        }
      ]
    },
    "num_partitions": 1,
    "num_shards": 1,
    "properties": [
      {
        "key": "timezone",
        "value": "UTC"
      }
    ]
  },
  "indexes": [],
  "manifest-list": [
    "snapshots/462324574592960519/manifests/data-file-manifest-95c6ab19-749e-4088-990b-b4613ff2df2f.avro"
  ]
}"""

  test("Parse complete snapshot metadata successfully") {
    val result = MilvusSnapshotReader.parseSnapshotMetadata(sampleSnapshotJson)

    result shouldBe a[Success[_]]
    val metadata = result.get

    // Verify snapshot info
    metadata.snapshotInfo.name shouldBe "backfill_snapshot"
    metadata.snapshotInfo.id shouldBe 462324574599774209L
    metadata.snapshotInfo.description shouldBe Some("add field backfill snapshot")
    metadata.snapshotInfo.collectionId shouldBe 462324574592960519L
    metadata.snapshotInfo.partitionIds should contain(462324574592960520L)
    metadata.snapshotInfo.createTs shouldBe 462324677975474190L

    // Verify collection
    metadata.collection.numPartitions shouldBe Some(1)
    metadata.collection.numShards shouldBe Some(1)

    // Verify manifest list
    metadata.manifestList should have size 1
    metadata.manifestList.head should include("data-file-manifest")
  }

  test("Parse collection schema successfully") {
    val result = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson)

    result shouldBe a[Success[_]]
    val schema = result.get

    schema.name shouldBe "backfilltestcollection"
    schema.description shouldBe Some("Test collection for MilvusBackfill")
    schema.fields should have size 7
  }

  test("Extract primary key field correctly") {
    val schema = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson).get

    val pkField = schema.getPrimaryKeyField
    pkField shouldBe defined
    pkField.get.name shouldBe "id"
    pkField.get.getFieldIDAsLong shouldBe 100L
    pkField.get.dataType shouldBe 5
    pkField.get.isPrimaryKey shouldBe Some(true)
  }

  test("Extract clustering key field correctly") {
    val schema = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson).get

    val ckField = schema.getClusteringKeyField
    ckField shouldBe defined
    ckField.get.name shouldBe "int64"
    ckField.get.getFieldIDAsLong shouldBe 101L
    ckField.get.isClusteringKey shouldBe Some(true)
  }

  test("Extract field type parameters correctly") {
    val schema = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson).get

    // Test varchar field with max_length param
    val varcharField = schema.getFieldByName("varchar")
    varcharField shouldBe defined
    varcharField.get.typeParams shouldBe defined
    varcharField.get.getTypeParam("max_length") shouldBe Some("1024")

    // Test vector field with dim param
    val vectorField = schema.getFieldByName("vector")
    vectorField shouldBe defined
    vectorField.get.typeParams shouldBe defined
    vectorField.get.getTypeParam("dim") shouldBe Some("128")
  }

  test("Get field by name") {
    val schema = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson).get

    val floatField = schema.getFieldByName("float")
    floatField shouldBe defined
    floatField.get.getFieldIDAsLong shouldBe 102L
    floatField.get.dataType shouldBe 10

    val nonExistentField = schema.getFieldByName("nonexistent")
    nonExistentField shouldBe None
  }

  test("Get field by ID") {
    val schema = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson).get

    val field104 = schema.getFieldById(104)
    field104 shouldBe defined
    field104.get.name shouldBe "vector"
    field104.get.dataType shouldBe 101

    val nonExistentField = schema.getFieldById(999)
    nonExistentField shouldBe None
  }

  test("Handle fields without fieldID (system fields)") {
    val schema = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson).get

    val rowIdField = schema.getFieldByName("RowID")
    rowIdField shouldBe defined
    rowIdField.get.getFieldIDAsLong shouldBe 0L  // Default value for missing fieldID
    rowIdField.get.description shouldBe Some("row id")
  }

  test("Parse schema properties") {
    val schema = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson).get

    schema.properties shouldBe defined
    schema.properties.get should have size 1
    schema.properties.get.head.key shouldBe "timezone"
    schema.properties.get.head.value shouldBe "UTC"
  }

  test("Handle invalid JSON gracefully") {
    val invalidJson = """{"invalid": "json structure"}"""
    val result = MilvusSnapshotReader.parseSnapshotMetadata(invalidJson)

    result shouldBe a[Failure[_]]
  }

  test("Handle empty JSON gracefully") {
    val emptyJson = ""
    val result = MilvusSnapshotReader.parseSnapshotMetadata(emptyJson)

    result shouldBe a[Failure[_]]
  }

  test("Parse collection schema with minimal required fields") {
    val minimalSchemaJson = """{
      "name": "test_collection",
      "fields": [
        {
          "name": "id",
          "data_type": 5
        }
      ]
    }"""

    val result = MilvusSnapshotReader.parseCollectionSchema(minimalSchemaJson)

    result shouldBe a[Success[_]]
    val schema = result.get
    println(result.get)
    schema.name shouldBe "test_collection"
    schema.fields should have size 1
    schema.fields.head.name shouldBe "id"
    schema.fields.head.isPrimaryKey shouldBe None
    schema.fields.head.typeParams shouldBe None
  }

  test("All fields are correctly parsed with proper types") {
    val schema = MilvusSnapshotReader.extractSchemaFromSnapshot(sampleSnapshotJson).get

    // Verify all 7 fields
    val fieldNames = schema.fields.map(_.name)
    fieldNames should contain allOf("id", "int64", "float", "varchar", "vector", "RowID", "Timestamp")

    // Verify data types
    schema.getFieldByName("id").get.dataType shouldBe 5
    schema.getFieldByName("float").get.dataType shouldBe 10
    schema.getFieldByName("varchar").get.dataType shouldBe 21
    schema.getFieldByName("vector").get.dataType shouldBe 101
  }

  test("Convert JSON schema to proto CollectionSchema (compatible with MilvusClient)") {
    val result = MilvusSnapshotReader.extractProtoSchemaFromSnapshot(sampleSnapshotJson)

    result shouldBe a[Success[_]]
    val protoSchema = result.get

    // Verify proto schema basic properties
    println(protoSchema)
    protoSchema.name shouldBe "backfilltestcollection"
    protoSchema.description shouldBe "Test collection for MilvusBackfill"
    protoSchema.fields should have size 7

    // Verify primary key field
    val pkField = protoSchema.fields.find(_.isPrimaryKey)
    pkField shouldBe defined
    pkField.get.name shouldBe "id"
    pkField.get.fieldID shouldBe 100L
    pkField.get.dataType shouldBe DataType.Int64

    // Verify clustering key field
    val ckField = protoSchema.fields.find(_.isClusteringKey)
    ckField shouldBe defined
    ckField.get.name shouldBe "int64"
    ckField.get.fieldID shouldBe 101L
    ckField.get.isClusteringKey shouldBe true

    // Verify field with type params (varchar)
    val varcharField = protoSchema.fields.find(_.name == "varchar")
    varcharField shouldBe defined
    varcharField.get.dataType shouldBe DataType.VarChar
    varcharField.get.typeParams should have size 1
    varcharField.get.typeParams.head.key shouldBe "max_length"
    varcharField.get.typeParams.head.value shouldBe "1024"

    // Verify vector field
    val vectorField = protoSchema.fields.find(_.name == "vector")
    vectorField shouldBe defined
    vectorField.get.dataType shouldBe DataType.FloatVector
    vectorField.get.typeParams should have size 1
    vectorField.get.typeParams.head.key shouldBe "dim"
    vectorField.get.typeParams.head.value shouldBe "128"

    // Verify properties
    protoSchema.properties should have size 1
    protoSchema.properties.head.key shouldBe "timezone"
    protoSchema.properties.head.value shouldBe "UTC"
  }

  test("Proto schema is compatible with MilvusClient return type") {
    // This test verifies that the returned type is exactly the same as MilvusClient.getCollectionSchema()
    val protoSchema: io.milvus.grpc.schema.CollectionSchema =
      MilvusSnapshotReader.extractProtoSchemaFromSnapshot(sampleSnapshotJson).get

    // If this compiles, it proves type compatibility
    protoSchema shouldBe a[io.milvus.grpc.schema.CollectionSchema]

    // Verify we can use it the same way as MilvusClient schema
    val fieldNames = protoSchema.fields.map(_.name)
    fieldNames should contain("id")

    // Verify DataType enum values match
    val idField = protoSchema.fields.find(_.name == "id").get
    idField.dataType shouldBe DataType.Int64
  }

  test("Convert minimal schema to proto format") {
    val minimalSchemaJson = """{
      "name": "test_collection",
      "fields": [
        {
          "name": "id",
          "data_type": 5
        }
      ]
    }"""

    val result = MilvusSnapshotReader.parseProtoCollectionSchema(minimalSchemaJson)

    result shouldBe a[Success[_]]
    val protoSchema = result.get

    protoSchema.name shouldBe "test_collection"
    protoSchema.fields should have size 1
    protoSchema.fields.head.name shouldBe "id"
    protoSchema.fields.head.dataType shouldBe DataType.Int64
    protoSchema.fields.head.isPrimaryKey shouldBe false
  }
}
