import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.Random
object StreamFromInner {
  def main(args: Array[String]): Unit = {
    //创建环境
    val see = StreamExecutionEnvironment
      .getExecutionEnvironment

    /*StreamExecutionEnvironment
      .createLocalEnvironment()*/

    see.addSource(new SourceFunction[(String,Int)] {
      val words: Array[String] =Array("henry","pika","ariel","jack","rose")
      val rand=new Random()
      var flag=true

      override def run(sourceContext: SourceFunction.SourceContext[(String, Int)]): Unit = {
        while (flag){
          sourceContext.collectWithTimestamp((words(rand.nextInt(words.length)),1),System.currentTimeMillis())
          Thread.sleep(5+rand.nextInt(10))
        }
      }

      override def cancel(): Unit = flag=false
    })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[(String,Int)] {
            override def extractTimestamp(t: (String, Int), l: Long): Long = l
          })
      )
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .sum(1)
      .print()

    //println(see.getParallelism)
    see.execute("test01")

  }
}
