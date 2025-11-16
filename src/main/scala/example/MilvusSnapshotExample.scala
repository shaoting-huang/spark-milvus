package example

import com.zilliz.spark.connector.{MilvusClient, MilvusConnectionParams}
import io.milvus.grpc.schema.DataType
import scala.util.{Success, Failure}

/** Example demonstrating Milvus Snapshot APIs
  *
  * This example shows how to:
  * 1. Create a snapshot for a collection
  * 2. List all snapshots
  * 3. Describe a snapshot
  * 4. Restore a snapshot to a new collection
  * 5. Monitor restore job state
  * 6. List restore jobs
  * 7. Drop a snapshot
  */
object MilvusSnapshotExample {

  def main(args: Array[String]): Unit = {
    // Initialize Milvus client
    val params = MilvusConnectionParams(
      uri = "http://localhost:19530",
      token = "root:Milvus",
      databaseName = "default"
    )

    val client = MilvusClient(params)

    try {
      val collectionName = "test_snapshot_collection"
      val dbName = "default"

      println("=== Milvus Snapshot API Examples ===\n")

      // 1. Create a snapshot
      println("1. Creating a snapshot...")
      val snapshotName = s"snapshot_${System.currentTimeMillis()}"
      client.createSnapshot(
        dbName = dbName,
        collectionName = collectionName,
        name = snapshotName,
        description = "Example snapshot created from Scala connector"
      ) match {
        case Success(status) =>
          println(s"✓ Snapshot '$snapshotName' created successfully")
        case Failure(exception) =>
          println(s"✗ Failed to create snapshot: ${exception.getMessage}")
      }

      // 2. List all snapshots for a collection
      println("\n2. Listing snapshots for collection...")
      client.listSnapshots(
        dbName = dbName,
        collectionName = collectionName
      ) match {
        case Success(snapshots) =>
          println(s"✓ Found ${snapshots.size} snapshot(s):")
          snapshots.foreach(name => println(s"  - $name"))
        case Failure(exception) =>
          println(s"✗ Failed to list snapshots: ${exception.getMessage}")
      }

      // 3. Describe a snapshot
      println("\n3. Describing the snapshot...")
      client.describeSnapshot(name = snapshotName) match {
        case Success(response) =>
          println(s"✓ Snapshot details:")
          println(s"  Name: ${response.name}")
          println(s"  Description: ${response.description}")
          println(s"  Collection: ${response.collectionName}")
          println(s"  Create Timestamp: ${response.createTs}")
          println(s"  Partitions: ${response.partitionNames.mkString(", ")}")
          if (response.s3Location.nonEmpty) {
            println(s"  S3 Location: ${response.s3Location}")
          }
        case Failure(exception) =>
          println(s"✗ Failed to describe snapshot: ${exception.getMessage}")
      }

      // 4. Restore a snapshot to a new collection
      println("\n4. Restoring snapshot to a new collection...")
      val newCollectionName = s"${collectionName}_restored"
      client.restoreSnapshot(
        name = snapshotName,
        dbName = dbName,
        collectionName = newCollectionName,
        rewriteData = false
      ) match {
        case Success(jobId) =>
          println(s"✓ Restore job started with ID: $jobId")

          // 5. Monitor restore job state
          println("\n5. Monitoring restore job state...")
          var completed = false
          var attempts = 0
          val maxAttempts = 10

          while (!completed && attempts < maxAttempts) {
            client.getRestoreSnapshotState(jobId = jobId) match {
              case Success(info) =>
                println(s"  Job state: ${info.state}")
                info.state.name match {
                  case "RestoreSnapshotCompleted" =>
                    println(s"✓ Restore completed successfully!")
                    println(s"  Collection: ${info.collectionName}")
                    println(s"  Time cost: ${info.timeCost} ms")
                    completed = true
                  case "RestoreSnapshotFailed" =>
                    println(s"✗ Restore failed: ${info.reason}")
                    completed = true
                  case "RestoreSnapshotPending" =>
                    println(s"  Restore job pending...")
                  case "RestoreSnapshotExecuting" =>
                    println(s"  Restore job executing... (${info.progress}%)")
                  case _ =>
                    println(s"  Unknown state: ${info.state}")
                }
              case Failure(exception) =>
                println(s"✗ Failed to get restore state: ${exception.getMessage}")
                completed = true
            }

            if (!completed) {
              Thread.sleep(2000) // Wait 2 seconds before checking again
              attempts += 1
            }
          }

        case Failure(exception) =>
          println(s"✗ Failed to restore snapshot: ${exception.getMessage}")
      }

      // 6. List all restore jobs
      println("\n6. Listing all restore jobs...")
      client.listRestoreSnapshotJobs(collectionName = collectionName) match {
        case Success(jobs) =>
          println(s"✓ Found ${jobs.size} restore job(s):")
          jobs.foreach { job =>
            println(s"  - Job ID: ${job.jobId}")
            println(s"    Snapshot: ${job.snapshotName}")
            println(s"    Collection: ${job.collectionName}")
            println(s"    State: ${job.state}")
          }
        case Failure(exception) =>
          println(s"✗ Failed to list restore jobs: ${exception.getMessage}")
      }

      // 7. Drop the snapshot
      println("\n7. Dropping the snapshot...")
      client.dropSnapshot(name = snapshotName) match {
        case Success(status) =>
          println(s"✓ Snapshot '$snapshotName' dropped successfully")
        case Failure(exception) =>
          println(s"✗ Failed to drop snapshot: ${exception.getMessage}")
      }

      println("\n=== Example completed ===")

    } finally {
      // Always close the client
      client.close()
    }
  }
}
