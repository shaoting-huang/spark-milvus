package com.zilliz.spark.connector.read

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import io.milvus.grpc.schema.{DataType, FieldSchema => ProtoFieldSchema, CollectionSchema => ProtoCollectionSchema}
import io.milvus.grpc.common.KeyValuePair
import scala.util.{Try, Success, Failure}

/**
 * Type parameter for Milvus field
 */
case class TypeParam(
    @JsonProperty("key") key: String,
    @JsonProperty("value") value: String
)

/**
 * Field schema definition
 */
case class Field(
    @JsonProperty("fieldID") fieldID: Option[Any],  // Can be Int or Long from JSON
    @JsonProperty("name") name: String,
    @JsonProperty("description") description: Option[String],
    @JsonProperty("data_type") dataType: Int,
    @JsonProperty("is_primary_key") isPrimaryKey: Option[Boolean],
    @JsonProperty("is_clustering_key") isClusteringKey: Option[Boolean],
    @JsonProperty("type_params") typeParams: Option[Seq[TypeParam]]
) {
  def getTypeParam(key: String): Option[String] = {
    typeParams.flatMap(_.find(_.key == key).map(_.value))
  }

  def getFieldIDAsLong: Long = {
    fieldID match {
      case Some(l: Long) => l
      case Some(i: Int) => i.toLong
      case Some(n: Number) => n.longValue()
      case _ => 0L
    }
  }
}

/**
 * Property key-value pair
 */
case class Property(
    @JsonProperty("key") key: String,
    @JsonProperty("value") value: String
)

/**
 * Collection schema definition
 */
case class CollectionSchema(
    @JsonProperty("name") name: String,
    @JsonProperty("description") description: Option[String],
    @JsonProperty("fields") fields: Seq[Field],
    @JsonProperty("properties") properties: Option[Seq[Property]]
) {
  def getPrimaryKeyField: Option[Field] = {
    fields.find(_.isPrimaryKey.contains(true))
  }

  def getClusteringKeyField: Option[Field] = {
    fields.find(_.isClusteringKey.contains(true))
  }

  def getFieldByName(name: String): Option[Field] = {
    fields.find(_.name == name)
  }

  def getFieldById(id: Long): Option[Field] = {
    fields.find(_.getFieldIDAsLong == id)
  }
}

/**
 * Collection metadata
 */
case class Collection(
    @JsonProperty("schema") schema: CollectionSchema,
    @JsonProperty("num_partitions") numPartitions: Option[Int],
    @JsonProperty("num_shards") numShards: Option[Int],
    @JsonProperty("properties") properties: Option[Seq[Property]]
)

/**
 * Snapshot information
 */
case class SnapshotInfo(
    @JsonProperty("name") name: String,
    @JsonProperty("id") id: Long,
    @JsonProperty("description") description: Option[String],
    @JsonProperty("collection_id") collectionId: Long,
    @JsonProperty("partition_ids") partitionIds: Seq[Long],
    @JsonProperty("create_ts") createTs: Long
)

/**
 * Complete snapshot metadata
 */
case class SnapshotMetadata(
    @JsonProperty("snapshot-info") snapshotInfo: SnapshotInfo,
    @JsonProperty("collection") collection: Collection,
    @JsonProperty("indexes") indexes: Seq[Any],
    @JsonProperty("manifest-list") manifestList: Seq[String]
)

/**
 * Reader for Milvus snapshot metadata JSON files
 */
object MilvusSnapshotReader {

  private val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m
  }

  /**
   * Convert JSON Field to proto FieldSchema
   */
  private def convertFieldToProto(field: Field): ProtoFieldSchema = {
    ProtoFieldSchema(
      fieldID = field.getFieldIDAsLong,
      name = field.name,
      isPrimaryKey = field.isPrimaryKey.getOrElse(false),
      description = field.description.getOrElse(""),
      dataType = DataType.fromValue(field.dataType),
      typeParams = field.typeParams.getOrElse(Seq.empty).map(tp =>
        KeyValuePair(key = tp.key, value = tp.value)
      ),
      isClusteringKey = field.isClusteringKey.getOrElse(false)
    )
  }

  /**
   * Convert JSON CollectionSchema to proto CollectionSchema
   */
  private def convertSchemaToProto(schema: CollectionSchema): ProtoCollectionSchema = {
    ProtoCollectionSchema(
      name = schema.name,
      description = schema.description.getOrElse(""),
      fields = schema.fields.map(convertFieldToProto),
      properties = schema.properties.getOrElse(Seq.empty).map(p =>
        KeyValuePair(key = p.key, value = p.value)
      )
    )
  }

  /**
   * Parse snapshot metadata from JSON string
   *
   * @param json JSON string containing snapshot metadata
   * @return Try containing parsed SnapshotMetadata or exception
   */
  def parseSnapshotMetadata(json: String): Try[SnapshotMetadata] = {
    Try {
      mapper.readValue[SnapshotMetadata](json)
    }
  }

  /**
   * Parse collection schema from JSON string
   *
   * @param json JSON string containing collection schema
   * @return Try containing parsed CollectionSchema or exception
   */
  def parseCollectionSchema(json: String): Try[CollectionSchema] = {
    Try {
      mapper.readValue[CollectionSchema](json)
    }
  }

  /**
   * Extract collection schema from snapshot metadata JSON
   *
   * @param json JSON string containing snapshot metadata
   * @return Try containing parsed CollectionSchema or exception
   */
  def extractSchemaFromSnapshot(json: String): Try[CollectionSchema] = {
    parseSnapshotMetadata(json).map(_.collection.schema)
  }

  /**
   * Extract proto CollectionSchema from snapshot metadata JSON
   * This returns the same type as MilvusClient.getCollectionSchema()
   *
   * @param json JSON string containing snapshot metadata
   * @return Try containing proto CollectionSchema compatible with MilvusClient
   */
  def extractProtoSchemaFromSnapshot(json: String): Try[ProtoCollectionSchema] = {
    extractSchemaFromSnapshot(json).map(convertSchemaToProto)
  }

  /**
   * Parse collection schema and convert to proto format
   * This returns the same type as MilvusClient.getCollectionSchema()
   *
   * @param json JSON string containing collection schema
   * @return Try containing proto CollectionSchema compatible with MilvusClient
   */
  def parseProtoCollectionSchema(json: String): Try[ProtoCollectionSchema] = {
    parseCollectionSchema(json).map(convertSchemaToProto)
  }

  /**
   * Read snapshot metadata from file
   *
   * @param path Path to the snapshot metadata JSON file
   * @return Try containing parsed SnapshotMetadata or exception
   */
  def readSnapshotMetadataFromFile(path: String): Try[SnapshotMetadata] = {
    Try {
      val source = scala.io.Source.fromFile(path)
      try {
        val json = source.mkString
        parseSnapshotMetadata(json).get
      } finally {
        source.close()
      }
    }
  }
}
