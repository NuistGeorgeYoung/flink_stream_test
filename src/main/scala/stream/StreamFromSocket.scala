package stream

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, WindowStagger}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object StreamFromSocket {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment
      .getExecutionEnvironment
    environment.setParallelism(1)

    environment
      .socketTextStream("master02",9999,'\n',3)
      .setParallelism(1)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[String] {
            override def extractTimestamp(t: String, l: Long): Long = System.currentTimeMillis()
          })
      )
      .process((value: String,
                context: ProcessFunction[String, (String, Int)]#Context,
                collector: Collector[(String, Int)]
               ) => {
        value.replaceAll("[^a-zA-Z ]"," ")
          .split("\\s+")
          .map((_,1))
          .groupBy(_._1)
          .mapValues(_.length)
          .foreach(wc=>collector.collect(wc))
      }
      )
      .keyBy(_._1)
      .window(
        TumblingEventTimeWindows.of(Time.seconds(5),Time.seconds(2),WindowStagger.NATURAL)
      )
      .process(new ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow] {
        override def process
        (key: String,
         context: Context,
         elements: Iterable[(String, Int)],
         out: Collector[(String, Int)]): Unit = {
          out.collect((key,elements.map(_._2).sum))
        }
      }
      )
      .addSink(new SinkFunction[(String, Int)] {
        override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
          println(value)
        }
      })

    environment.execute("job_from socket")
  }
}
