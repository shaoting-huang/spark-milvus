package com.zilliz.spark.connector

import io.milvus.grpc.schema.{
  DataType,
  ScalarField,
  VectorField
}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.ArrowType

/**
 * SerdeEntry encapsulates serialization/deserialization logic for a Milvus data type
 *
 * @param arrowType Function to get Arrow type given dimension and element type
 * @param deserialize Function to deserialize from Arrow array to Scala value
 * @param serialize Function to serialize from Scala value to Arrow builder
 */
case class SerdeEntry(
  arrowType: (Int, DataType) => ArrowType,
  deserialize: (org.apache.arrow.vector.FieldVector, Int, DataType, Int, Boolean) => Option[Any],
  serialize: (org.apache.arrow.vector.FieldVector, Any, DataType) => Boolean
)

/**
 * MilvusSerdeUtil provides serialization/deserialization utilities for Milvus data types
 */
object MilvusSerdeUtil {
  /**
   * SerdeMap provides serialization/deserialization entries for all Milvus data types
   */
  lazy val serdeMap: Map[DataType, SerdeEntry] = {
    import org.apache.arrow.vector.types.FloatingPointPrecision
    import java.nio.ByteBuffer
    import java.nio.ByteOrder

    val m = scala.collection.mutable.Map[DataType, SerdeEntry]()

    // Boolean type
    m(DataType.Bool) = SerdeEntry(
      arrowType = (_, _) => new ArrowType.Bool(),
      deserialize = (vec, i, _, _, _) => {
        if (vec.isNull(i)) None
        else vec.asInstanceOf[BitVector].get(i) match {
          case 0 => Some(false)
          case 1 => Some(true)
        }
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[BitVector].setNull(vec.getValueCount); true
          case b: Boolean => vec.asInstanceOf[BitVector].setSafe(vec.getValueCount, if (b) 1 else 0); true
          case _ => false
        }
      }
    )

    // Int8 type
    m(DataType.Int8) = SerdeEntry(
      arrowType = (_, _) => new ArrowType.Int(8, true),
      deserialize = (vec, i, _, _, _) => {
        if (vec.isNull(i)) None
        else Some(vec.asInstanceOf[TinyIntVector].get(i))
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[TinyIntVector].setNull(vec.getValueCount); true
          case b: Byte => vec.asInstanceOf[TinyIntVector].setSafe(vec.getValueCount, b); true
          case _ => false
        }
      }
    )

    // Int16 type
    m(DataType.Int16) = SerdeEntry(
      arrowType = (_, _) => new ArrowType.Int(16, true),
      deserialize = (vec, i, _, _, _) => {
        if (vec.isNull(i)) None
        else Some(vec.asInstanceOf[SmallIntVector].get(i))
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[SmallIntVector].setNull(vec.getValueCount); true
          case s: Short => vec.asInstanceOf[SmallIntVector].setSafe(vec.getValueCount, s); true
          case _ => false
        }
      }
    )

    // Int32 type
    m(DataType.Int32) = SerdeEntry(
      arrowType = (_, _) => new ArrowType.Int(32, true),
      deserialize = (vec, i, _, _, _) => {
        if (vec.isNull(i)) None
        else Some(vec.asInstanceOf[IntVector].get(i))
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[IntVector].setNull(vec.getValueCount); true
          case int: Int => vec.asInstanceOf[IntVector].setSafe(vec.getValueCount, int); true
          case _ => false
        }
      }
    )

    // Int64 type
    m(DataType.Int64) = SerdeEntry(
      arrowType = (_, _) => new ArrowType.Int(64, true),
      deserialize = (vec, i, _, _, _) => {
        if (vec.isNull(i)) None
        else Some(vec.asInstanceOf[BigIntVector].get(i))
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[BigIntVector].setNull(vec.getValueCount); true
          case l: Long => vec.asInstanceOf[BigIntVector].setSafe(vec.getValueCount, l); true
          case _ => false
        }
      }
    )

    // Float type
    m(DataType.Float) = SerdeEntry(
      arrowType = (_, _) => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
      deserialize = (vec, i, _, _, _) => {
        if (vec.isNull(i)) None
        else Some(vec.asInstanceOf[Float4Vector].get(i))
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[Float4Vector].setNull(vec.getValueCount); true
          case f: Float => vec.asInstanceOf[Float4Vector].setSafe(vec.getValueCount, f); true
          case _ => false
        }
      }
    )

    // Double type
    m(DataType.Double) = SerdeEntry(
      arrowType = (_, _) => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
      deserialize = (vec, i, _, _, _) => {
        if (vec.isNull(i)) None
        else Some(vec.asInstanceOf[Float8Vector].get(i))
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[Float8Vector].setNull(vec.getValueCount); true
          case d: Double => vec.asInstanceOf[Float8Vector].setSafe(vec.getValueCount, d); true
          case _ => false
        }
      }
    )

    // Timestamptz type
    m(DataType.Timestamptz) = SerdeEntry(
      arrowType = (_, _) => new ArrowType.Int(64, true),
      deserialize = (vec, i, _, _, _) => {
        if (vec.isNull(i)) None
        else Some(vec.asInstanceOf[BigIntVector].get(i))
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[BigIntVector].setNull(vec.getValueCount); true
          case l: Long => vec.asInstanceOf[BigIntVector].setSafe(vec.getValueCount, l); true
          case _ => false
        }
      }
    )

    // String types (VarChar, String, Text)
    val stringEntry = SerdeEntry(
      arrowType = (_, _) => new ArrowType.Utf8(),
      deserialize = (vec, i, _, _, shouldCopy) => {
        if (vec.isNull(i)) None
        else {
          val value = new String(vec.asInstanceOf[VarCharVector].get(i), "UTF-8")
          Some(if (shouldCopy) value else value)
        }
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[VarCharVector].setNull(vec.getValueCount); true
          case s: String => vec.asInstanceOf[VarCharVector].setSafe(vec.getValueCount, s.getBytes("UTF-8")); true
          case _ => false
        }
      }
    )

    m(DataType.VarChar) = stringEntry
    m(DataType.String) = stringEntry
    m(DataType.Text) = stringEntry

    // Binary/Byte entry for JSON, Geometry, and raw bytes
    val byteEntry = SerdeEntry(
      arrowType = (_, _) => new ArrowType.Binary(),
      deserialize = (vec, i, _, _, shouldCopy) => {
        if (vec.isNull(i)) None
        else {
          val value = vec.asInstanceOf[VarBinaryVector].get(i)
          Some(if (shouldCopy) {
            val copy = new Array[Byte](value.length)
            System.arraycopy(value, 0, copy, 0, value.length)
            copy
          } else value)
        }
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[VarBinaryVector].setNull(vec.getValueCount); true
          case bytes: Array[Byte] => vec.asInstanceOf[VarBinaryVector].setSafe(vec.getValueCount, bytes); true
          case sf: ScalarField =>
            val bytes = sf.toByteArray
            vec.asInstanceOf[VarBinaryVector].setSafe(vec.getValueCount, bytes)
            true
          case vf: VectorField =>
            val bytes = vf.toByteArray
            vec.asInstanceOf[VarBinaryVector].setSafe(vec.getValueCount, bytes)
            true
          case _ => false
        }
      }
    )

    m(DataType.Array) = byteEntry
    m(DataType.JSON) = byteEntry
    m(DataType.Geometry) = byteEntry

    // Fixed-size binary deserializer (for vectors)
    def fixedSizeDeserializer(vec: FieldVector, i: Int, dt: DataType, dim: Int, shouldCopy: Boolean): Option[Any] = {
      if (vec.isNull(i)) None
      else {
        val value = vec.asInstanceOf[FixedSizeBinaryVector].get(i)
        Some(if (shouldCopy) {
          val copy = new Array[Byte](value.length)
          System.arraycopy(value, 0, copy, 0, value.length)
          copy
        } else value)
      }
    }

    def fixedSizeSerializer(vec: FieldVector, v: Any, dt: DataType): Boolean = {
      v match {
        case null => vec.asInstanceOf[FixedSizeBinaryVector].setNull(vec.getValueCount); true
        case bytes: Array[Byte] => vec.asInstanceOf[FixedSizeBinaryVector].setSafe(vec.getValueCount, bytes); true
        case _ => false
      }
    }

    // BinaryVector
    m(DataType.BinaryVector) = SerdeEntry(
      arrowType = (dim, _) => new ArrowType.FixedSizeBinary((dim + 7) / 8),
      deserialize = fixedSizeDeserializer,
      serialize = fixedSizeSerializer
    )

    // Float16Vector
    m(DataType.Float16Vector) = SerdeEntry(
      arrowType = (dim, _) => new ArrowType.FixedSizeBinary(dim * 2),
      deserialize = fixedSizeDeserializer,
      serialize = fixedSizeSerializer
    )

    // BFloat16Vector
    m(DataType.BFloat16Vector) = SerdeEntry(
      arrowType = (dim, _) => new ArrowType.FixedSizeBinary(dim * 2),
      deserialize = fixedSizeDeserializer,
      serialize = fixedSizeSerializer
    )

    // Int8Vector
    m(DataType.Int8Vector) = SerdeEntry(
      arrowType = (dim, _) => new ArrowType.FixedSizeBinary(dim),
      deserialize = (vec, i, _, _, shouldCopy) => {
        if (vec.isNull(i)) None
        else {
          val bytes = vec.asInstanceOf[FixedSizeBinaryVector].get(i)
          val int8s = bytes.map(_.toByte)
          Some(if (shouldCopy) int8s.clone() else int8s)
        }
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[FixedSizeBinaryVector].setNull(vec.getValueCount); true
          case bytes: Array[Byte] => vec.asInstanceOf[FixedSizeBinaryVector].setSafe(vec.getValueCount, bytes); true
          case _ => false
        }
      }
    )

    // FloatVector
    m(DataType.FloatVector) = SerdeEntry(
      arrowType = (dim, _) => new ArrowType.FixedSizeBinary(dim * 4),
      deserialize = (vec, i, _, _, shouldCopy) => {
        if (vec.isNull(i)) None
        else {
          val bytes = vec.asInstanceOf[FixedSizeBinaryVector].get(i)
          val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
          val floats = new Array[Float](bytes.length / 4)
          for (j <- floats.indices) {
            floats(j) = buffer.getFloat()
          }
          Some(if (shouldCopy) floats.clone() else floats)
        }
      },
      serialize = (vec, v, _) => {
        v match {
          case null => vec.asInstanceOf[FixedSizeBinaryVector].setNull(vec.getValueCount); true
          case floats: Array[Float] =>
            val buffer = ByteBuffer.allocate(floats.length * 4).order(ByteOrder.LITTLE_ENDIAN)
            floats.foreach(buffer.putFloat)
            vec.asInstanceOf[FixedSizeBinaryVector].setSafe(vec.getValueCount, buffer.array())
            true
          case _ => false
        }
      }
    )

    // SparseFloatVector
    m(DataType.SparseFloatVector) = byteEntry

    // ArrayOfVector (placeholder - needs more complex implementation)
    m(DataType.ArrayOfVector) = SerdeEntry(
      arrowType = (_, elementType) => {
        // Returns List type - actual element type depends on elementType parameter
        new ArrowType.List()
      },
      deserialize = (vec, i, elementType, dim, shouldCopy) => {
        // TODO: Implement full ArrayOfVector deserialization based on elementType
        if (vec.isNull(i)) None
        else {
          // This is a placeholder - full implementation would handle different element types
          Some(vec.asInstanceOf[org.apache.arrow.vector.complex.ListVector].getObject(i))
        }
      },
      serialize = (vec, v, elementType) => {
        v match {
          case null =>
            vec.asInstanceOf[org.apache.arrow.vector.complex.ListVector].setNull(vec.getValueCount)
            true
          case vf: VectorField =>
            // TODO: Implement full ArrayOfVector serialization based on elementType
            // This is a placeholder
            elementType match {
              case DataType.FloatVector =>
                // Serialize float vector array
                true
              case _ =>
                throw new UnsupportedOperationException(s"ArrayOfVector with element type $elementType not yet implemented")
            }
          case _ => false
        }
      }
    )

    m.toMap
  }
}
