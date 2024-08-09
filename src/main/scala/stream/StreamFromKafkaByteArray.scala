package stream

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, WindowStagger}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import producer.TranKafkaProducer.ByStore

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.time.Duration

object StreamFromKafkaByteArray {
  def main(args: Array[String]): Unit = {
    val kafkaSource: KafkaSource[ByStore] = {
      // kafka 反序列化策略
      val deSchema: KafkaRecordDeserializationSchema[ByStore] =
        new KafkaRecordDeserializationSchema[ByStore] {
          override def
          deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]],
                      collector: Collector[ByStore]) = {
            val stream = new ByteArrayInputStream(consumerRecord.value())
            val ois = new ObjectInputStream(stream)
            val store = ois.readObject().asInstanceOf[ByStore]
            collector.collect(store)
          }

          override def getProducedType: TypeInformation[ByStore]
          = TypeInformation.of(classOf[ByStore])
        }

      // kafka 数据源
      val inTopic = "tran_by_store"

      KafkaSource
        .builder[ByStore]()
        .setBootstrapServers("master01:9092,master02:9092,worker01:9092")
        .setTopics(inTopic)
        // kafka 识别消费者
        .setClientIdPrefix("tran_by_store_consumer")
        .setGroupId("tran_by_store_group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(deSchema)
        .build()
    }

    // 水位线策略
    val watermark: WatermarkStrategy[ByStore] = {
      WatermarkStrategy
        .forMonotonousTimestamps()
        .withIdleness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[ByStore] {
          override def extractTimestamp(t: ByStore, l: Long): Long = System.currentTimeMillis()
        })
    }
    val see = StreamExecutionEnvironment
      .getExecutionEnvironment
    see.setParallelism(3)

    val processStream = see.fromSource(kafkaSource, watermark, "kafkaByte")
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(5),
        Time.seconds(0),
        WindowStagger.NATURAL
      ))
      .process(new ProcessAllWindowFunction[ByStore, (Int, Float), TimeWindow] {
        override def process(context: Context, elements: Iterable[ByStore], out: Collector[(Int, Float)]): Unit = {
          val tranCnt = elements.map(_.tranCount).sum
          val sumAmt = elements.map(_.sumAmount).sum

          out.collect((tranCnt, sumAmt))
        }
      })

    processStream
      .getSideOutput[ByStore](OutputTag("tran_by_store_consumer_side_output"))
      .addSink(new RichSinkFunction[ByStore] {
        override def invoke(value: ByStore, context: SinkFunction.Context): Unit = {
          println(s"side output $context")
        }

        override def open(parameters: Configuration): Unit = {
          println("rich output sinl starting")
        }

        override def close(): Unit = {
          println("rich output sink stopping")
        }
      })

    processStream
      .addSink(new RichSinkFunction[(Int,Float)] {
        override def invoke(value: (Int, Float), context: SinkFunction.Context): Unit = {
          println(s"result : $value")
        }

        override def open(parameters: Configuration): Unit = {
          println("rich sink starting ...")
        }

        override def close(): Unit = {
          println("rich sink closing...")
        }
      })
    see.execute("flink_kafka_byte_arr")
  }
}
