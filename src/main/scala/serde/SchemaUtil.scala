package com.zilliz.spark.connector

import io.milvus.grpc.schema.{
  ArrayArray,
  BoolArray,
  BytesArray,
  DataType,
  DoubleArray,
  FieldData,
  FieldSchema,
  FloatArray,
  GeometryArray,
  IntArray,
  JSONArray,
  LongArray,
  ScalarField,
  SparseFloatArray,
  StringArray,
  VectorField
}

/**
 * MilvusSchemaUtil provides utilities for converting between Milvus and Arrow schemas
 */
object MilvusSchemaUtil {
  def getDim(fieldSchema: FieldSchema): Int = {
    for (param <- fieldSchema.typeParams) {
      if (param.key == "dim") {
        return param.value.toInt
      }
    }
    throw new DataParseException(
      s"Field ${fieldSchema.name} has no dim parameter"
    )
  }

  /**
   * Convert Milvus FieldSchema to Arrow Field
   */
  def convertToArrowField(
      field: FieldSchema,
      arrowType: org.apache.arrow.vector.types.pojo.ArrowType
  ): org.apache.arrow.vector.types.pojo.Field = {
    import scala.collection.JavaConverters._

    val metadata = Map(
      "PARQUET:field_id" -> field.fieldID.toString
    ).asJava

    // Create FieldType with metadata included
    val fieldType = new org.apache.arrow.vector.types.pojo.FieldType(
      true, // nullable
      arrowType,
      null, // dictionary encoding
      metadata
    )

    new org.apache.arrow.vector.types.pojo.Field(
      field.name,
      fieldType,
      null // children - null for simple types
    )
  }

  /**
   * Convert Milvus CollectionSchema to Arrow Schema
   * This function converts a Milvus collection schema to an Arrow schema format.
   * Now uses the serdeMap for consistent type conversion.
   *
   * @param collectionSchema The Milvus collection schema
   * @return Arrow Schema
   */
  def convertToArrowSchema(
      collectionSchema: io.milvus.grpc.schema.CollectionSchema
  ): org.apache.arrow.vector.types.pojo.Schema = {
    import scala.collection.JavaConverters._
    import org.apache.arrow.vector.types.pojo.{Field, FieldType}

    val arrowFields = scala.collection.mutable.ArrayBuffer[Field]()

    // Helper function to append a field
    def appendArrowField(field: FieldSchema): Unit = {
      // Get dimension for vector types
      val dim = field.dataType match {
        case DataType.BinaryVector | DataType.Float16Vector |
             DataType.BFloat16Vector | DataType.Int8Vector |
             DataType.FloatVector | DataType.ArrayOfVector =>
          try {
            getDim(field)
          } catch {
            case e: DataParseException =>
              throw new DataParseException(
                s"dim not found in field [${field.name}] params: ${e.getMessage}"
              )
          }
        case _ => 0
      }

      // Get element type for ArrayOfVector
      val elementType = if (field.dataType == DataType.ArrayOfVector) {
        field.elementType
      } else {
        DataType.None
      }

      val serdeEntry = MilvusSerdeUtil.serdeMap.get(field.dataType)
      if (serdeEntry.isEmpty) {
        throw new DataParseException(
          s"No serde entry found for data type [${field.dataType}] for field [${field.name}]"
        )
      }

      val arrowType = serdeEntry.get.arrowType(dim, elementType)

      // Create Arrow field
      val arrowField = if (field.dataType == DataType.ArrayOfVector) {
        // Add extra metadata for ArrayOfVector
        val metadata = Map(
          "PARQUET:field_id" -> field.fieldID.toString,
          "elementType" -> elementType.value.toString,
          "dim" -> dim.toString
        ).asJava

        val fieldType = new FieldType(
          true, // nullable
          arrowType,
          null, // dictionary encoding
          metadata
        )

        new Field(
          field.name,
          fieldType,
          null // children
        )
      } else {
        convertToArrowField(field, arrowType)
      }

      arrowFields += arrowField
    }

    // Process all fields in the collection schema
    collectionSchema.fields.foreach { field =>
      appendArrowField(field)
    }

    // Create and return Arrow Schema
    new org.apache.arrow.vector.types.pojo.Schema(arrowFields.asJava)
  }
}