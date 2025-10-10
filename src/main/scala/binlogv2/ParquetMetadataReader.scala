package com.zilliz.spark.connector.binlogv2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import scala.util.{Try, Success, Failure}

/**
 * Utility for reading metadata from Parquet files
 */
object ParquetMetadataReader {

  /**
   * Read group_field_id_list from field 0 parquet file's metadata
   *
   * @param filePath Path to field 0 parquet file (can be s3a:// or local)
   * @param s3Config Optional S3 configuration
   * @return The group_field_id_list string (e.g., "100,101,0,1;102,103;104")
   */
  def readGroupFieldIdList(
      filePath: String,
      s3Config: Option[S3Config] = None
  ): String = {
    val conf = new Configuration()

    // Configure S3 if provided
    s3Config.foreach { config =>
      conf.set("fs.s3a.access.key", config.accessKey)
      conf.set("fs.s3a.secret.key", config.secretKey)
      conf.set("fs.s3a.endpoint", config.endpoint)
      conf.set("fs.s3a.path.style.access", "true")
      conf.set("fs.s3a.connection.ssl.enabled", config.useSsl.toString)
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    }

    val path = new Path(filePath)
    val inputFile = HadoopInputFile.fromPath(path, conf)

    Try {
      val reader = ParquetFileReader.open(inputFile)
      try {
        val fileMetaData = reader.getFooter.getFileMetaData
        val keyValueMetaData = fileMetaData.getKeyValueMetaData

        // Read group_field_id_list from metadata
        val groupFieldIdList = keyValueMetaData.get("group_field_id_list")
        if (groupFieldIdList == null) {
          throw new IllegalStateException(
            s"Missing 'group_field_id_list' in file metadata: $filePath"
          )
        }

        groupFieldIdList
      } finally {
        reader.close()
      }
    } match {
      case Success(result) => result
      case Failure(e) =>
        throw new RuntimeException(
          s"Failed to read group_field_id_list from $filePath: ${e.getMessage}",
          e
        )
    }
  }
}

/**
 * S3 configuration (kept for backward compatibility with tests)
 */
case class S3Config(
    accessKey: String,
    secretKey: String,
    endpoint: String,
    useSsl: Boolean = false
)
