package com.zilliz.spark.connector.sources

import java.{util => ju}
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.{HashMap, Map => JMap}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema, Data}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan,
  ScanBuilder
}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  DataTypes => SparkDataTypes,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  ShortType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.{
  DataTypeUtil,
  MilvusClient,
  MilvusCollectionInfo,
  MilvusOption
}
import com.zilliz.spark.connector.binlogv2.{
  ManifestBuilder,
  ParquetMetadataReader,
  S3Config
}
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType => MilvusDataType
}
import io.milvus.storage.{
  ArrowUtils,
  MilvusStorageProperties,
  MilvusStorageReader,
  NativeLibraryLoader
}

// 1. DataSourceRegister and TableProvider
case class MilvusStorageV2DataSource() extends TableProvider with DataSourceRegister with Logging {

  override def shortName(): String = "storagev2"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOption = MilvusOption(options)

    if (milvusOption.uri.isEmpty) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.MilvusUri}' is required for storagev2 format."
      )
    }
    if (milvusOption.collectionName.isEmpty) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.MilvusCollectionName}' is required for storagev2 format."
      )
    }

    val client = MilvusClient(milvusOption)
    try {
      val collectionInfo = client.getCollectionInfo(
        milvusOption.databaseName,
        milvusOption.collectionName
      ).getOrElse(
        throw new Exception(
          s"Collection ${milvusOption.collectionName} not found"
        )
      )

      // Convert Milvus schema to Spark schema
      convertMilvusSchemaToSparkSchema(collectionInfo.schema, options)
    } finally {
      client.close()
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val milvusOption = MilvusOption(options)
    MilvusStorageV2Table(milvusOption, Some(schema))
  }

  override def supportsExternalMetadata = true

  private def convertMilvusSchemaToSparkSchema(
      milvusSchema: CollectionSchema,
      options: CaseInsensitiveStringMap
  ): StructType = {
    var fields = Seq[StructField]()

    // Get field IDs to read (if specified)
    val readerFieldIDs = Option(options.get(MilvusOption.ReaderFieldIDs))
      .map(_.split(",").map(_.trim).toSet)
      .getOrElse(Set.empty[String])

    // Add system fields only if explicitly requested
    // Note: Storage V2 reader doesn't return system fields by default
    if (readerFieldIDs.contains("0")) {
      fields = fields :+ StructField("RowID", LongType, false)
    }
    if (readerFieldIDs.contains("1")) {
      fields = fields :+ StructField("Timestamp", LongType, false)
    }

    // Add user fields
    milvusSchema.fields.foreach { field =>
      val fieldIDStr = field.fieldID.toString
      if (readerFieldIDs.isEmpty || readerFieldIDs.contains(fieldIDStr)) {
        fields = fields :+ StructField(
          field.name,
          DataTypeUtil.toDataType(field),
          field.nullable
        )
      }
    }

    StructType(fields)
  }
}

// 2. Table
case class MilvusStorageV2Table(
    milvusOption: MilvusOption,
    sparkSchema: Option[StructType]
) extends Table with SupportsRead with Logging {

  var milvusCollection: MilvusCollectionInfo = _
  var partitionID: Long = 0L
  initInfo()

  def initInfo(): Unit = {
    val client = MilvusClient(milvusOption)
    try {
      milvusCollection = client
        .getCollectionInfo(
          milvusOption.databaseName,
          milvusOption.collectionName
        )
        .getOrElse(
          throw new Exception(
            s"Collection ${milvusOption.collectionName} not found"
          )
        )
      if (milvusOption.partitionName.nonEmpty) {
        partitionID = client
          .getPartitionID(
            milvusOption.databaseName,
            milvusOption.collectionName,
            milvusOption.partitionName
          )
          .getOrElse(
            throw new Exception(
              s"Partition ${milvusOption.partitionName} not found"
            )
          )
      }
    } finally {
      client.close()
    }
  }

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    // Merge table properties with scan options
    val mergedOptions: JMap[String, String] = new HashMap[String, String]()
    mergedOptions.put(MilvusOption.MilvusUri, milvusOption.uri)
    mergedOptions.put(MilvusOption.MilvusToken, milvusOption.token)
    mergedOptions.put(MilvusOption.MilvusCollectionName, milvusOption.collectionName)
    mergedOptions.put(MilvusOption.MilvusDatabaseName, milvusOption.databaseName)
    if (milvusOption.partitionName.nonEmpty) {
      mergedOptions.put(MilvusOption.MilvusPartitionName, milvusOption.partitionName)
    }
    mergedOptions.put(
      MilvusOption.MilvusCollectionID,
      milvusCollection.collectionID.toString
    )
    if (partitionID != 0L) {
      mergedOptions.put(
        MilvusOption.MilvusPartitionID,
        partitionID.toString
      )
    }
    mergedOptions.putAll(options)

    val allOptions = new CaseInsensitiveStringMap(mergedOptions)
    new MilvusStorageV2ScanBuilder(schema(), allOptions)
  }

  override def name(): String = s"MilvusStorageV2[${milvusOption.collectionName}]"

  override def schema(): StructType = {
    sparkSchema.getOrElse {
      // Fallback: convert from milvus schema
      // Note: System fields (RowID, Timestamp) are not included by default
      // as Storage V2 reader doesn't return them unless explicitly requested
      val fields = milvusCollection.schema.fields.map(field =>
        StructField(
          field.name,
          DataTypeUtil.toDataType(field),
          field.nullable
        )
      )
      StructType(fields)
    }
  }

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_READ
    ).asJava
  }
}

// 3. ScanBuilder
class MilvusStorageV2ScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends ScanBuilder with Logging {

  override def build(): Scan =
    new MilvusStorageV2Scan(schema, options)
}

// 4. Scan (Batch Scan)
class MilvusStorageV2Scan(
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends Scan with Batch with Logging {

  private val milvusOption = MilvusOption(options)

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val client = MilvusClient(milvusOption)
    try {
      val collectionInfo = client.getCollectionInfo(
        milvusOption.databaseName,
        milvusOption.collectionName
      ).getOrElse(
        throw new Exception(
          s"Collection ${milvusOption.collectionName} not found"
        )
      )

      val segments = client.getSegments(
        milvusOption.databaseName,
        milvusOption.collectionName
      ).getOrElse(
        throw new Exception("Failed to get segments")
      )

      if (segments.isEmpty) {
        logWarning(s"No segments found for collection ${milvusOption.collectionName}")
        return Array.empty[InputPartition]
      }

      // Filter by partition if specified
      val filteredSegments = if (milvusOption.partitionName.nonEmpty) {
        // Get partition ID from client
        val targetPartitionID = client.getPartitionID(
          milvusOption.databaseName,
          milvusOption.collectionName,
          milvusOption.partitionName
        ).getOrElse(
          throw new Exception(s"Partition ${milvusOption.partitionName} not found")
        )
        segments.filter(_.partitionID == targetPartitionID)
      } else {
        segments
      }

      logInfo(s"Found ${filteredSegments.size} segments to process")

      // Get S3/Minio config from options
      val s3Config = getS3Config(options)

      // Create input partition for each segment
      val inputPartitions = filteredSegments.map { segment =>
        val collectionID = segment.collectionID.toString
        val partitionID = segment.partitionID.toString
        val segmentID = segment.segmentID.toString

        try {
          val manifestJson = buildManifestForSegment(
            collectionInfo.schema,
            collectionID,
            partitionID,
            segmentID,
            s3Config
          )

          MilvusStorageV2InputPartition(
            manifestJson,
            collectionInfo.schema.toByteArray,
            partitionID,
            s3Config
          )
        } catch {
          case e: Exception =>
            logError(s"Failed to build manifest for segment $segmentID", e)
            throw e
        }
      }

      inputPartitions.toArray

    } finally {
      client.close()
    }
  }

  private def buildManifestForSegment(
      schema: CollectionSchema,
      collectionID: String,
      partitionID: String,
      segmentID: String,
      s3Config: S3ConfigWrapper
  ): String = {
    logInfo(s"Building manifest for segment $segmentID")

    // Get segment info from Milvus which contains insertLogIDs
    val client = MilvusClient(milvusOption)
    val segmentInfo = client.getSegmentInfo(collectionID.toLong, segmentID.toLong)
      .getOrElse(throw new IllegalStateException(
        s"Failed to get segment info for segment $segmentID"
      ))

    if (segmentInfo.insertLogIDs.isEmpty) {
      throw new IllegalStateException(
        s"No insert logs found for segment $segmentID"
      )
    }

    logInfo(s"Found ${segmentInfo.insertLogIDs.size} insert logs for segment $segmentID")

    val binlogFilesMapFull = segmentInfo.insertLogIDs
      .groupBy(_.split("/")(0))  // Group by fieldID
      .map { case (fieldID, logIDs) =>
        val files = logIDs.map { logID =>
          s"s3a://${s3Config.bucket}/${s3Config.rootPath}/insert_log/$collectionID/$partitionID/$segmentID/$logID"
        }
        fieldID -> files
      }

    // Get field 0 path for reading group_field_id_list metadata
    val field0FullPaths = binlogFilesMapFull.get("0")
      .orElse(binlogFilesMapFull.values.headOption)
      .getOrElse(throw new IllegalStateException(
        s"No binlog files found in segment $segmentID"
      ))

    if (field0FullPaths.isEmpty) {
      throw new IllegalStateException(
        s"Field 0 has no binlog files in segment $segmentID"
      )
    }

    val s3Conf = S3Config(
      s3Config.accessKey,
      s3Config.secretKey,
      s3Config.endpoint,
      s3Config.useSsl
    )

    // Read group_field_id_list from field 0 parquet metadata
    val groupFieldIdList = ParquetMetadataReader.readGroupFieldIdList(
      field0FullPaths.head,
      Some(s3Conf)
    )

    // Convert full paths to relative paths for manifest
    val binlogFilesMap = binlogFilesMapFull.map { case (fieldID, files) =>
      val relativePaths = files.map(_.replaceFirst("s3a://", ""))
      fieldID -> relativePaths
    }

    // Build manifest
    val manifest = ManifestBuilder.buildManifest(
      schema,
      binlogFilesMap,
      version = 0,
      groupFieldIdList
    )

    val manifestJson = ManifestBuilder.toJson(manifest)

    manifestJson
  }

  private def getS3Config(options: CaseInsensitiveStringMap): S3ConfigWrapper = {
    val endpoint = Option(options.get("fs.endpoint"))
      .orElse(Option(options.get("fs.address")))
      .getOrElse("localhost:9000")
    val bucket = Option(options.get("fs.bucket_name"))
      .getOrElse("a-bucket")
    val rootPath = Option(options.get("fs.root_path"))
      .getOrElse("files")
    val accessKey = Option(options.get("fs.access_key_id"))
      .getOrElse("minioadmin")
    val secretKey = Option(options.get("fs.access_key_value"))
      .getOrElse("minioadmin")
    val useSsl = Option(options.get("fs.use_ssl"))
      .map(_.toBoolean)
      .getOrElse(false)

    S3ConfigWrapper(endpoint, bucket, rootPath, accessKey, secretKey, useSsl)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Convert CaseInsensitiveStringMap to Scala Map for serialization
    val optionsMap = options.asScala.toMap
    new MilvusStorageV2ReaderFactory(schema, optionsMap)
  }
}

// Internal wrapper for S3 config
case class S3ConfigWrapper(
    endpoint: String,
    bucket: String,
    rootPath: String,
    accessKey: String,
    secretKey: String,
    useSsl: Boolean
) extends Serializable

// 5. InputPartition
case class MilvusStorageV2InputPartition(
    manifestJson: String,
    milvusSchemaBytes: Array[Byte],  // Serialized protobuf
    partitionName: String,
    s3Config: S3ConfigWrapper
) extends InputPartition

// 6. PartitionReaderFactory
class MilvusStorageV2ReaderFactory(
    schema: StructType,
    optionsMap: Map[String, String]  // Use Scala Map instead of CaseInsensitiveStringMap
) extends PartitionReaderFactory {

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[MilvusStorageV2InputPartition]

    // Deserialize the protobuf schema
    val milvusSchema = CollectionSchema.parseFrom(p.milvusSchemaBytes)

    new MilvusStorageV2PartitionReader(
      schema,
      p.manifestJson,
      milvusSchema,
      p.s3Config,
      optionsMap
    )
  }
}

// 7. PartitionReader
class MilvusStorageV2PartitionReader(
    schema: StructType,
    manifestJson: String,
    milvusSchema: CollectionSchema,
    s3Config: S3ConfigWrapper,
    optionsMap: Map[String, String]
) extends PartitionReader[InternalRow] with Logging {

  // Load native library
  NativeLibraryLoader.loadLibrary()

  private val allocator = ArrowUtils.getAllocator

  // Create Arrow schema from Milvus schema
  private val arrowSchemaC = createArrowSchema()

  // Create reader properties
  private val readerProperties = createReaderProperties()

  // Determine which columns to read
  private val columnNames = getColumnNames()

  // Create Storage V2 reader
  private val reader = new MilvusStorageReader()
  reader.create(manifestJson, arrowSchemaC, columnNames, readerProperties)

  if (!reader.isValid) {
    throw new IllegalStateException("Failed to create MilvusStorageReader")
  }

  // Get Arrow stream
  private val recordBatchReaderPtr = reader.getRecordBatchReaderScala(null, 1024, 8 * 1024 * 1024)
  private val arrowArrayStream = ArrowArrayStream.wrap(recordBatchReaderPtr)
  private val arrowReader = Data.importArrayStream(allocator, arrowArrayStream)

  private var currentBatch: VectorSchemaRoot = _
  private var currentRowIndex: Int = 0

  override def next(): Boolean = {
    // Check if current batch has more rows
    if (currentBatch != null && currentRowIndex < currentBatch.getRowCount) {
      return true
    }

    // Try to load next batch
    if (arrowReader.loadNextBatch()) {
      currentBatch = arrowReader.getVectorSchemaRoot
      currentRowIndex = 0
      return currentBatch.getRowCount > 0
    }

    false
  }

  override def get(): InternalRow = {
    if (currentBatch == null) {
      throw new IllegalStateException("No batch loaded")
    }

    val row = convertArrowRowToInternalRow(currentBatch, currentRowIndex, schema)
    currentRowIndex += 1
    row
  }

  override def close(): Unit = {
    try {
      if (arrowReader != null) arrowReader.close()
      if (arrowArrayStream != null) arrowArrayStream.close()
      if (reader != null) reader.destroy()
      ArrowUtils.releaseArrowSchema(arrowSchemaC)
      readerProperties.free()
    } catch {
      case e: Exception =>
        logWarning("Error closing Storage V2 reader", e)
    }
  }

  private def createArrowSchema(): Long = {
    // Convert Milvus schema to Arrow schema
    val arrowSchemaObj = com.zilliz.spark.connector.MilvusSchemaUtil.convertToArrowSchema(milvusSchema)
    val arrowSchemaC = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchemaObj, null, arrowSchemaC)
    arrowSchemaC.memoryAddress()
  }

  private def createReaderProperties(): MilvusStorageProperties = {
    val props = new MilvusStorageProperties()
    val readerProps = new ju.HashMap[String, String]()

    readerProps.put("fs.storage_type", "remote")
    readerProps.put("fs.access_key_id", s3Config.accessKey)
    readerProps.put("fs.access_key_value", s3Config.secretKey)
    readerProps.put("fs.bucket_name", s3Config.bucket)
    readerProps.put("fs.use_ssl", s3Config.useSsl.toString)
    readerProps.put("fs.address", s3Config.endpoint)
    readerProps.put("fs.region", "us-west-2")

    props.create(readerProps)
    if (!props.isValid) {
      throw new IllegalStateException("Failed to create MilvusStorageProperties")
    }
    props
  }

  private def getColumnNames(): Array[String] = {
    // Get column names from Milvus schema (only user-defined fields)
    // MilvusStorageReader does not support reading system fields (RowID, Timestamp) directly
    milvusSchema.fields.map(_.name).toArray
  }

  private def convertArrowRowToInternalRow(
      root: VectorSchemaRoot,
      rowIndex: Int,
      sparkSchema: StructType
  ): InternalRow = {
    val values = new Array[Any](sparkSchema.fields.length)

    sparkSchema.fields.zipWithIndex.foreach { case (field, index) =>
      val vector = root.getVector(field.name)

      if (vector == null) {
        logWarning(s"Vector not found for field: ${field.name}")
        values(index) = null
      } else if (vector.isNull(rowIndex)) {
        values(index) = null
      } else {
        values(index) = convertArrowValue(vector, rowIndex, field.dataType)
      }
    }

    InternalRow.fromSeq(values)
  }

  private def convertArrowValue(
      vector: FieldVector,
      rowIndex: Int,
      sparkType: org.apache.spark.sql.types.DataType
  ): Any = {
    sparkType match {
      case LongType =>
        vector.asInstanceOf[BigIntVector].get(rowIndex)

      case IntegerType =>
        vector.asInstanceOf[IntVector].get(rowIndex)

      case ShortType =>
        vector.asInstanceOf[SmallIntVector].get(rowIndex)

      case FloatType =>
        vector.asInstanceOf[Float4Vector].get(rowIndex)

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].get(rowIndex)

      case BooleanType =>
        vector.asInstanceOf[BitVector].get(rowIndex) != 0

      case StringType =>
        val bytes = vector.asInstanceOf[VarCharVector].get(rowIndex)
        UTF8String.fromBytes(bytes)

      case ArrayType(FloatType, _) =>
        // FloatVector stored as FixedSizeBinaryVector
        val bytes = vector.asInstanceOf[FixedSizeBinaryVector].get(rowIndex)
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        val floats = (0 until (bytes.length / 4)).map(_ => buffer.getFloat()).toArray
        ArrayData.toArrayData(floats)

      case ArrayType(elementType, _) =>
        // Generic array handling
        val listVector = vector.asInstanceOf[ListVector]
        val dataVector = listVector.getDataVector
        val startIndex = listVector.getElementStartIndex(rowIndex)
        val endIndex = listVector.getElementEndIndex(rowIndex)
        val length = endIndex - startIndex

        val arrayElements = (0 until length).map { i =>
          val elemIndex = startIndex + i
          if (dataVector.isNull(elemIndex)) {
            null
          } else {
            convertArrowValue(dataVector, elemIndex, elementType)
          }
        }.toArray

        ArrayData.toArrayData(arrayElements)

      case BinaryType =>
        val bytes = vector.asInstanceOf[VarBinaryVector].get(rowIndex)
        bytes

      case MapType(keyType, valueType, _) =>
        val mapVector = vector.asInstanceOf[MapVector]
        val dataVector = mapVector.getDataVector.asInstanceOf[StructVector]
        val startIndex = mapVector.getElementStartIndex(rowIndex)
        val endIndex = mapVector.getElementEndIndex(rowIndex)
        val length = endIndex - startIndex

        val keys = new Array[Any](length)
        val values = new Array[Any](length)

        (0 until length).foreach { i =>
          val elemIndex = startIndex + i
          keys(i) = convertArrowValue(dataVector.getChild("key"), elemIndex, keyType)
          values(i) = convertArrowValue(dataVector.getChild("value"), elemIndex, valueType)
        }

        ArrayBasedMapData(keys, values)

      case _ =>
        logWarning(s"Unsupported Spark type: $sparkType, returning null")
        null
    }
  }
}
