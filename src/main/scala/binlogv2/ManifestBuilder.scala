package com.zilliz.spark.connector.binlogv2

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import io.milvus.grpc.schema.CollectionSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import scala.util.{Try, Success, Failure}

/**
 * Manifest Builder for Milvus Storage V2 binlog files
 *
 * Background on Milvus Storage V2 binlog structure:
 * - Field ID 0 (binlog): Contains system fields + first few user fields
 *   - Typically: RowID (field 0), Timestamp (field 1), and user primary key + clustering key field
 * - Field ID 1 (binlog): Contains remaining scalar fields (if any)
 * - Field ID > 100 (binlog): Each vector field gets its own binlog field ID
 *
 */
object ManifestBuilder {

  private val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m
  }

  /**
   * Build manifest using provided global field ID mapping
   *
   * @param schema Collection schema
   * @param binlogFilesMap Binlog field ID to file paths mapping
   * @param version Manifest version
   * @param groupFieldIdList The semicolon-separated field ID mapping string (e.g., "100,101,0,1;102,103;104")
   * @return Manifest object
   */
  def buildManifest(
      schema: CollectionSchema,
      binlogFilesMap: Map[String, Seq[String]],
      version: Int,
      groupFieldIdList: String
  ): Manifest = {
    val globalMapping = parseGroupFieldIdList(groupFieldIdList)

    val columnGroups = binlogFilesMap.map { case (binlogFieldID, files) =>
      val fieldIDs = globalMapping.getOrElse(binlogFieldID, {
        Seq(binlogFieldID.toLong)
      })

      // Convert field IDs to field names
      val columns = fieldIDs.map(fieldID => getFieldNameByID(schema, fieldID))

      ColumnGroup(
        columns = columns,
        format = "parquet",
        paths = files
      )
    }.toSeq

    Manifest(
      columnGroups = columnGroups,
      version = version
    )
  }

  /**
   * Parse group_field_id_list string into a mapping
   *
   * @param groupFieldIdList The semicolon-separated field ID mapping string (e.g., "100,101,0,1;102,103;104")
   * @return Map of binlog field ID to list of actual Milvus field IDs
   */
  def parseGroupFieldIdList(groupFieldIdList: String): Map[String, Seq[Long]] = {
    val groups = groupFieldIdList.split(";")
    groups.zipWithIndex.flatMap { case (group, idx) =>
      val fieldIDs = group.split(",").map(_.trim.toLong).toSeq
      if (fieldIDs.length == 1) {
        // Single field: use the field ID as binlog field ID
        Seq(fieldIDs.head.toString -> fieldIDs)
      } else {
        // Multiple fields: use the group index as binlog field ID
        Seq(idx.toString -> fieldIDs)
      }
    }.toMap
  }

  /**
   * Get field name by field ID from schema
   *
   * Special handling:
   * - Field ID 0 -> "RowID" (system field)
   * - Field ID 1 -> "Timestamp" (system field)
   * - Others -> find from schema or use default name
   */
  private def getFieldNameByID(schema: CollectionSchema, fieldID: Long): String = {
    fieldID match {
      case 0 => "RowID"
      case 1 => "Timestamp"
      case _ =>
        schema.fields
          .find(_.fieldID == fieldID)
          .map(_.name)
          .getOrElse(s"field_$fieldID")
    }
  }

  /**
   * Convert Manifest to JSON string
   */
  def toJson(manifest: Manifest): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest)
  }

  /**
   * Parse Manifest from JSON string
   */
  def fromJson(json: String): Manifest = {
    mapper.readValue(json, classOf[Manifest])
  }
}

/**
 * Manifest data structure
 */
case class Manifest(
    @JsonProperty("column_groups") columnGroups: Seq[ColumnGroup],
    @JsonProperty("version") version: Int
)

/**
 * Column Group data structure
 */
case class ColumnGroup(
    @JsonProperty("columns") columns: Seq[String],
    @JsonProperty("format") format: String,
    @JsonProperty("paths") paths: Seq[String]
)
