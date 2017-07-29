package com.samelamin.spark.bigquery.streaming

import com.samelamin.spark.bigquery._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * A simple Structured Streaming sink which writes the data frame to Google Bigquery.
  *
  * @param options options passed from the upper level down to the dataframe writer.
  */
class BigQuerySink(sparkSession: SparkSession, path: String, options: Map[String, String]) extends Sink {
  private val logger = LoggerFactory.getLogger(classOf[BigQuerySink])
  private val basePath = new Path(path)
  private val logPath = new Path(basePath, new Path(BigQuerySink.metadataDir,"transaction.json"))

  private val fileLog = new BigQuerySinkLog(sparkSession, logPath.toUri.toString)
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val fullyQualifiedOutputTableId = options.get("tableReferenceSink").get
    val isPartitionByDay =  options.getOrElse("partitionByDay","true").toBoolean
    val useStreamingInserts = options.getOrElse("useStreamingInserts", "false").toBoolean
    if (batchId <= fileLog.getLatest().getOrElse(-1L)) {
      logger.info(s"Skipping already committed batch $batchId")
    } else if (useStreamingInserts) {
      val bqClient = BigQueryClient.getInstance(data.sqlContext)
      bqClient.streamToBQTable(fullyQualifiedOutputTableId, data, batchId)
    } else {
      data.saveAsBigQueryTable(fullyQualifiedOutputTableId, isPartitionByDay)
    }
    fileLog.writeBatch(batchId)
  }
}

object BigQuerySink {
  // The name of the subdirectory that is used to store metadata about which files are valid.
  val metadataDir = "_spark_metadata"
}

