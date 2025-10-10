package binlogv2

import org.scalatest.funsuite.AnyFunSuite
import com.zilliz.spark.connector.binlogv2.ManifestBuilder
import scala.collection.JavaConverters._

/**
 * Test ManifestBuilder parsing logic and integration with Milvus binlog files
 */
class ManifestBuilderTest extends AnyFunSuite {

  test("Parse group_field_id_list with typical Milvus 2.6 format") {
    val input = "100,101,0,1;102,103;104"

    val mapping = ManifestBuilder.parseGroupFieldIdList(input)

    assert(mapping("0") == Seq(100, 101, 0, 1))
    assert(mapping("1") == Seq(102, 103))
    assert(mapping("104") == Seq(104))
    assert(mapping.size == 3)
  }

  test("Build manifest with group_field_id_list parameter") {
    import io.milvus.grpc.schema.{CollectionSchema, FieldSchema, DataType}

    // Create a mock schema with the fields we need (ScalaPB case classes)
    val field100 = FieldSchema(
      fieldID = 100,
      name = "id",
      dataType = DataType.Int64
    )

    val field101 = FieldSchema(
      fieldID = 101,
      name = "int64",
      dataType = DataType.Int64
    )

    val field102 = FieldSchema(
      fieldID = 102,
      name = "float",
      dataType = DataType.Float
    )

    val field103 = FieldSchema(
      fieldID = 103,
      name = "varchar",
      dataType = DataType.VarChar
    )

    val field104 = FieldSchema(
      fieldID = 104,
      name = "vector",
      dataType = DataType.FloatVector
    )

    val schema = CollectionSchema(
      name = "test_collection",
      fields = Seq(field100, field101, field102, field103, field104)
    )

    // Create binlog files map
    val binlogFilesMap = Map(
      "0" -> Seq("a-bucket/files/insert_log/collection/partition/segment/0/file1.parquet"),
      "1" -> Seq("a-bucket/files/insert_log/collection/partition/segment/1/file2.parquet"),
      "104" -> Seq("a-bucket/files/insert_log/collection/partition/segment/104/file3.parquet")
    )

    // Group field id list: field 0 contains [100,101,0,1], field 1 contains [102,103], field 104 contains [104]
    val groupFieldIdList = "100,101,0,1;102,103;104"

    // Build manifest
    val manifest = ManifestBuilder.buildManifest(schema, binlogFilesMap, 0, groupFieldIdList)

    // Verify manifest structure
    assert(manifest.version == 0, "Version should be 0")
    assert(manifest.columnGroups.size == 3, s"Should have 3 column groups, got ${manifest.columnGroups.size}")

    // Verify binlog field 0 (contains id, int64, RowID, Timestamp)
    val group0 = manifest.columnGroups.find(_.paths.head.contains("/0/"))
    assert(group0.isDefined, "Should have group for binlog field 0")
    assert(group0.get.columns == Seq("id", "int64", "RowID", "Timestamp"),
      s"Group 0 columns mismatch: ${group0.get.columns}")

    // Verify binlog field 1 (contains float, varchar)
    val group1 = manifest.columnGroups.find(_.paths.head.contains("/1/"))
    assert(group1.isDefined, "Should have group for binlog field 1")
    assert(group1.get.columns == Seq("float", "varchar"),
      s"Group 1 columns mismatch: ${group1.get.columns}")

    // Verify binlog field 104 (contains vector)
    val group104 = manifest.columnGroups.find(_.paths.head.contains("/104/"))
    assert(group104.isDefined, "Should have group for binlog field 104")
    assert(group104.get.columns == Seq("vector"),
      s"Group 104 columns mismatch: ${group104.get.columns}")
  }
}
