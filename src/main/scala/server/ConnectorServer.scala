package com.zilliz.spark.connector.server

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.net.InetSocketAddress
import java.io.{IOException, OutputStream}
import java.util.concurrent.Executors
import scala.util.{Try, Success, Failure}
import com.zilliz.spark.connector._

case class HealthResponse(status: String, timestamp: Long, version: String)
case class ReadyResponse(ready: Boolean, services: Map[String, Boolean])

/**
 * Simple HTTP server for Spark Milvus Connector
 * Provides health check, readiness probe and basic API endpoints
 */
class ConnectorServer(port: Int = 8080) {
  
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  
  private var server: HttpServer = _
  private var milvusClient: Option[MilvusClient] = None
  
  def start(): Unit = {
    try {
      server = HttpServer.create(new InetSocketAddress(port), 0)
      
      // Health check endpoint
      server.createContext("/health", new HealthHandler())
      
      // Readiness probe endpoint  
      server.createContext("/ready", new ReadinessHandler())
      
      // Metrics endpoint
      server.createContext("/metrics", new MetricsHandler())
      
      // Test connectivity endpoint
      server.createContext("/test-connection", new TestConnectionHandler())
      
      server.setExecutor(Executors.newCachedThreadPool())
      server.start()
      
      println(s"Spark Milvus Connector server started on port $port")
      
      // Initialize Milvus client
      initializeMilvusClient()
      
    } catch {
      case e: Exception =>
        println(s"Failed to start server: ${e.getMessage}")
        throw e
    }
  }
  
  def stop(): Unit = {
    if (server != null) {
      server.stop(0)
      println("Server stopped")
    }
    milvusClient.foreach(_.close())
  }
  
  private def initializeMilvusClient(): Unit = {
    try {
      val milvusUri = sys.env.getOrElse("MILVUS_URI", "http://milvus-standalone.milvus-system.svc.cluster.local:19530")
      val milvusToken = sys.env.getOrElse("MILVUS_TOKEN", "")
      val databaseName = sys.env.getOrElse("MILVUS_DATABASE_NAME", "default")
      
      val params = MilvusConnectionParams(
        uri = milvusUri,
        token = milvusToken,
        databaseName = databaseName
      )
      
      milvusClient = Some(MilvusClient(params))
      println(s"Milvus client initialized with URI: $milvusUri")
      
    } catch {
      case e: Exception =>
        println(s"Failed to initialize Milvus client: ${e.getMessage}")
        // Don't fail the server startup, just log the error
    }
  }
  
  class HealthHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val response = HealthResponse(
        status = "UP",
        timestamp = System.currentTimeMillis(),
        version = "0.1.14"
      )
      
      sendJsonResponse(exchange, 200, response)
    }
  }
  
  class ReadinessHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val milvusReady = milvusClient.isDefined
      
      val response = ReadyResponse(
        ready = milvusReady,
        services = Map(
          "milvus" -> milvusReady,
          "spark" -> true // For now, always true
        )
      )
      
      val status = if (response.ready) 200 else 503
      sendJsonResponse(exchange, status, response)
    }
  }
  
  class MetricsHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val metrics = 
        """# HELP spark_milvus_connector_health Health status of the connector
          |# TYPE spark_milvus_connector_health gauge
          |spark_milvus_connector_health{service="connector"} 1
          |# HELP spark_milvus_connector_milvus_connection Milvus connection status
          |# TYPE spark_milvus_connector_milvus_connection gauge
          |spark_milvus_connector_milvus_connection{service="milvus"} %d
          |""".stripMargin.format(if (milvusClient.isDefined) 1 else 0)
      
      sendTextResponse(exchange, 200, metrics)
    }
  }
  
  class TestConnectionHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      exchange.getRequestMethod match {
        case "GET" =>
          testMilvusConnection() match {
            case Success(result) =>
              sendJsonResponse(exchange, 200, Map(
                "status" -> "success",
                "message" -> "Milvus connection test successful",
                "result" -> result
              ))
            case Failure(e) =>
              sendJsonResponse(exchange, 500, Map(
                "status" -> "error", 
                "message" -> s"Milvus connection test failed: ${e.getMessage}",
                "error" -> e.getClass.getSimpleName
              ))
          }
        case _ =>
          sendTextResponse(exchange, 405, "Method Not Allowed")
      }
    }
  }
  
  private def testMilvusConnection(): Try[String] = {
    Try {
      milvusClient match {
        case Some(client) =>
          // Try to create a test database
          val testResult = client.createDatabase("test_connection_db")
          testResult match {
            case Success(_) => "Successfully connected to Milvus and created test database"
            case Failure(e) => throw new RuntimeException(s"Failed to create test database: ${e.getMessage}")
          }
        case None =>
          throw new RuntimeException("Milvus client not initialized")
      }
    }
  }
  
  private def sendJsonResponse(exchange: HttpExchange, statusCode: Int, data: Any): Unit = {
    try {
      val jsonResponse = mapper.writeValueAsString(data)
      val responseBytes = jsonResponse.getBytes("UTF-8")
      
      exchange.getResponseHeaders.set("Content-Type", "application/json")
      exchange.sendResponseHeaders(statusCode, responseBytes.length)
      
      val os: OutputStream = exchange.getResponseBody
      os.write(responseBytes)
      os.close()
    } catch {
      case e: Exception =>
        println(s"Error sending JSON response: ${e.getMessage}")
    }
  }
  
  private def sendTextResponse(exchange: HttpExchange, statusCode: Int, text: String): Unit = {
    try {
      val responseBytes = text.getBytes("UTF-8")
      
      exchange.getResponseHeaders.set("Content-Type", "text/plain")
      exchange.sendResponseHeaders(statusCode, responseBytes.length)
      
      val os: OutputStream = exchange.getResponseBody
      os.write(responseBytes)
      os.close()
    } catch {
      case e: Exception =>
        println(s"Error sending text response: ${e.getMessage}")
    }
  }
}

object ConnectorServerApp {
  def main(args: Array[String]): Unit = {
    val port = sys.env.getOrElse("SERVER_PORT", "8080").toInt
    val server = new ConnectorServer(port)
    
    // Add shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println("Shutting down server...")
      server.stop()
    }))
    
    try {
      server.start()
      
      // Keep the server running
      Thread.currentThread().join()
      
    } catch {
      case e: Exception =>
        println(s"Server failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }
}