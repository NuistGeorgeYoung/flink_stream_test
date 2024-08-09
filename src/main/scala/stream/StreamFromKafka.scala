package stream

import com.nimbusds.jose.util.StandardCharset
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import producer.TranKafkaProducer.{ByStore, Transaction, jsonToTran}

import java.io.ObjectOutputStream
import java.time.Duration
import java.util.Properties


object StreamFromKafka {
  def main(args: Array[String]): Unit = {

    val kafkaSource: KafkaSource[Transaction] = {
      // kafka 反序列化策略
      val deSchema: KafkaRecordDeserializationSchema[Transaction] =
        new KafkaRecordDeserializationSchema[Transaction] {
          override def
          deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]],
                      collector: Collector[Transaction]) = {
            val json = new String(consumerRecord.value(), StandardCharset.UTF_8)
            val tran: Transaction = jsonToTran(json)
            collector.collect(tran)
          }

          override def getProducedType
          = TypeInformation.of(classOf[Transaction])
        }

      // kafka 数据源
      val inTopic = "tran_test"

      KafkaSource
        .builder[Transaction]()
        .setBootstrapServers("master01:9092,master02:9092,worker01:9092")
        .setTopics(inTopic)
        // kafka 识别消费者
        .setClientIdPrefix("tran_test_yb12211")
        .setGroupId("tran_test_group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(deSchema)
        .build()
    }

    // 水位线策略
    val watermark: WatermarkStrategy[Transaction] = {
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withIdleness(Duration.ofMillis(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[Transaction] {
          override def extractTimestamp(t: Transaction, l: Long) = t.timestamp
        })
    }

    // 滑动窗口
    val slideETWindow: SlidingEventTimeWindows = SlidingEventTimeWindows
      .of(
        Time.seconds(10),
        Time.seconds(5),
        Time.seconds(2)
      )

    // 窗口处理函数
    val processWinFunc: ProcessWindowFunction[Transaction, ByStore, Int, TimeWindow]
    = new ProcessWindowFunction[Transaction, ByStore, Int, TimeWindow] {
      override def
      process(
               storeId: Int,
               context: Context,
               trans: Iterable[Transaction],
               out: Collector[ByStore]): Unit = {
        val sumAmount: Float = trans.map(_.price).sum
        val tranCount: Int = trans.size
        out.collect(ByStore(storeId, sumAmount, tranCount))
      }
    }

    val processWinFunc2: ProcessWindowFunction[Transaction, ByStore, Int, TimeWindow]
    = new ProcessWindowFunction[Transaction, ByStore, Int, TimeWindow] {
      override def
      process(
               storeId: Int,
               context: Context,
               trans: Iterable[Transaction],
               out: Collector[ByStore]): Unit = {
        val sumAmount: Float = trans.map(_.price).sum
        val tranCount: Int = trans.size
        out.collect(ByStore(storeId, sumAmount, tranCount))
      }
    }
    // KafkaSink : kafka生产者
    val kafkaSink = {
      val outTopic = "tran_by_store"
      // 序列化策略 ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
      val serSchema: SerializationSchema[ByStore] = new SerializationSchema[ByStore] {
        override def serialize(t: ByStore): Array[Byte] = {
          val bos = new ByteArrayOutputStream();
          val oos = new ObjectOutputStream(bos);
          oos.writeObject(t)
          oos.flush()
          bos.toByteArray
        }
      }

      // 生产者配置
      val producerConfig = {
        val config = new Properties()
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          "master01:9092,master02:9092,worker01:9092")
        // 失败重试
        config.setProperty(ProducerConfig.RETRIES_CONFIG,"3")
        config.setProperty(ProducerConfig.ACKS_CONFIG,"1")
        // 身份识别
        config.setProperty(ProducerConfig.CLIENT_ID_CONFIG,"tran_by_store_01")
        // 批量提交
        config.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"100")
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG,"1000")
        // 事务配置
        config.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"tran_by_store_yb12211")
        config.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"3000")
        config
      }

      // 精确一致性
      val exactlyOnce = FlinkKafkaProducer.Semantic.EXACTLY_ONCE

      // 分区策略 ProducerConfig.PARTITIONER_CLASS_CONFIG
      val partitioner: FlinkKafkaPartitioner[ByStore] = new FlinkKafkaPartitioner[ByStore] {
        var id = 0

        override def
        partition(
                   record: ByStore,
                   key: Array[Byte],
                   value: Array[Byte],
                   targetTopic: String,
                   partitions: Array[Int]
                 ): Int = {
          val parIx: Int = {
            id = id % partitions.size; id
          }
          id += 1
          parIx
        }
      }

      new FlinkKafkaProducer[ByStore](
        outTopic, serSchema, producerConfig,
        partitioner, exactlyOnce, 3
      )
    }

    val see = StreamExecutionEnvironment
      .getExecutionEnvironment
    see.setParallelism(3)

    val srcStream = see
      .fromSource(kafkaSource, watermark, "kafkaSource")
    srcStream
      .keyBy(_.product_id)
      .window(slideETWindow)
      .process(processWinFunc2)
      .addSink(new SinkFunction[ByStore] {
        override def invoke(value: ByStore): Unit = {
          println(value)
        }
      })

    srcStream
      .keyBy(_.store_id)
      .window(slideETWindow)
      .process(processWinFunc)
      .addSink(kafkaSink)

    see.execute("kafka_flink_kafka_01")
  }
}
























