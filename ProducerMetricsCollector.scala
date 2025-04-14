package kafka.metrics

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import kafka.server.KafkaConfig

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class ProducerMetricsCollector(kafkaConfig: KafkaConfig) extends Logging {

  private val metrics = new KafkaMetricsGroup(classOf[ProducerMetricsCollector])
  private val producerMetrics = new ConcurrentHashMap[String, ProducerMetrics]()

  case class ProducerMetrics(
    clientId: String,
    topic: String,
    partition: Int,
    ip: String,
    lastOffset: Long,
    totalRequestBytes: Long,
    totalRecords: Long,  
    timestamp: Long
  )

  def initialize(): Unit = {
    info("Initializing ProducerMetricsCollector")
  }

  private def registerMetric(metric: ProducerMetrics): Unit = {
    val labels = Map(
      "clientId" -> metric.clientId,
      "topic" -> metric.topic,
      "partition" -> metric.partition.toString,
      "ip" -> metric.ip
    ).asJava

    metrics.newGauge("producer_last_offset", () => {
      val key = buildKey(metric.clientId, metric.ip, metric.topic, metric.partition)
      Option(producerMetrics.get(key)).map(_.lastOffset).getOrElse(0L)
    }, labels)

    metrics.newGauge("producer_request_bytes_total", () => {
      val key = buildKey(metric.clientId, metric.ip, metric.topic, metric.partition)
      Option(producerMetrics.get(key)).map(_.totalRequestBytes).getOrElse(0L)
    }, labels)

    metrics.newGauge("producer_records_total", () => {
      val key = buildKey(metric.clientId, metric.ip, metric.topic, metric.partition)
      Option(producerMetrics.get(key)).map(_.totalRecords).getOrElse(0L)
    }, labels)
  }

  def interceptProduceRequest(clientId: String, ip: String, produceRequest: ProduceRequest): Unit = {
    produceRequest.data().topicData().forEach { topicData =>
      val topic = topicData.name()
      topicData.partitionData().forEach { partitionData =>
        val partition = partitionData.index()
        val memoryRecords = partitionData.records().asInstanceOf[MemoryRecords]
        val sizeInBytes = memoryRecords.sizeInBytes()
        
        val recordCount = memoryRecords.records().asScala.size

        val key = buildKey(clientId, ip, topic, partition)
        val currentTime = System.currentTimeMillis()
        
        val existing = Option(producerMetrics.get(key))
        val updated = existing match {
          case Some(metrics) => 
            metrics.copy(timestamp = currentTime, 
                          totalRequestBytes = metrics.totalRequestBytes + sizeInBytes,
                          totalRecords = metrics.totalRecords + recordCount)
          case None =>
            val metric = ProducerMetrics(clientId, topic, partition, ip, -1, sizeInBytes, recordCount,currentTime)
            registerMetric(metric)
            metric
        }
        
        producerMetrics.put(key, updated)
      }
    }
  }

  def interceptProduceResponse(clientId: String, ip: String, 
                               responseData: Map[TopicPartition, ProduceResponse.PartitionResponse]): Unit = {
    responseData.foreach { case (tp, response) =>
      if (response.error == Errors.NONE) {
        val key = buildKey(clientId, ip, tp.topic(), tp.partition())
        val currentTime = System.currentTimeMillis()
        
        val existing = Option(producerMetrics.get(key))
        val baseOffset = response.baseOffset
        
        val updated = existing match {
          case Some(m) => m.copy(lastOffset = baseOffset, timestamp = currentTime)
          case None =>
            val metric = ProducerMetrics(clientId, tp.topic(), tp.partition(), ip, baseOffset, 0, 0, currentTime)
            registerMetric(metric)
            metric
        }
        
        producerMetrics.put(key, updated)
      }
    }
  }

  private def buildKey(clientId: String, ip: String, topic: String, partition: Int): String =
    s"$clientId-$ip-$topic-$partition"

  def cleanupStaleMetrics(maxAgeMs: Long): Unit = {
    val currentTime = System.currentTimeMillis()
    val staleKeys = producerMetrics.asScala.collect {
      case (key, metric) if currentTime - metric.timestamp > maxAgeMs => key
    }
    
    staleKeys.foreach { key =>
      val metric = producerMetrics.get(key)
      if (metric != null) {
        val labels = Map(
          "clientId" -> metric.clientId,
          "topic" -> metric.topic,
          "partition" -> metric.partition.toString,
          "ip" -> metric.ip
        ).asJava
        metrics.removeMetric("producer_last_offset", labels)
        metrics.removeMetric("producer_request_bytes_total", labels)
      }
      producerMetrics.remove(key)
    }
  }

  def close(): Unit = {
    info("Closing ProducerMetricsCollector")
    producerMetrics.values().asScala.foreach { metric =>
      val labels = Map(
        "clientId" -> metric.clientId,
        "topic" -> metric.topic,
        "partition" -> metric.partition.toString,
        "ip" -> metric.ip
      ).asJava
      metrics.removeMetric("producer_last_offset", labels)
      metrics.removeMetric("producer_request_bytes_total", labels)
      metrics.removeMetric("producer_records_total", labels)
    }
    producerMetrics.clear()
  }
}

object ProducerMetricsCollector {
  private var instance: Option[ProducerMetricsCollector] = None

  def initialize(kafkaConfig: KafkaConfig): ProducerMetricsCollector = synchronized {
    if (instance.isEmpty) {
      val collector = new ProducerMetricsCollector(kafkaConfig)
      collector.initialize()
      instance = Some(collector)
    }
    instance.get
  }

  def getInstance: Option[ProducerMetricsCollector] = instance
}