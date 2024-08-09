package stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import scala.util.Random

object FlinkRandomTemperatureSource {
  private case class TemperatureReading(timestamp: Long, sensorId: String, temperature: Double)

  private class RandomTemperatureSource(var totalElements: Int) extends SourceFunction[TemperatureReading] {
    private var isRunning = true
    private val random = new Random()
    private var currentTime = System.currentTimeMillis()
    private var baseTemperature = 30.0

    override def run(ctx: SourceFunction.SourceContext[TemperatureReading]): Unit = {
      while (isRunning && totalElements > 0) {
        val delay = (random.nextDouble() * 1000 + 500).toLong // 0.5到1.5秒之间的随机延迟
        Thread.sleep(delay)

        // 模拟时间流逝和温度增加
        currentTime += delay
        baseTemperature += delay / 1000000.0

        // 生成并发送数据
        val sensorId = "sensor" + random.nextInt(10)
        val temperatureReading = TemperatureReading(currentTime, sensorId, baseTemperature)
        ctx.collect(temperatureReading)

        totalElements -= 1
      }

      ctx.close()
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }

  /**
   * 自定义时间戳和水位线分配器
   * AssignerWithPunctuatedWatermarks其实已经过时了
   * 建议使用assignTimestampsAndWatermarks
   */
  private class TemperatureTimestampExtractor extends AssignerWithPunctuatedWatermarks[TemperatureReading] {
    override def extractTimestamp(element: TemperatureReading, previousElementTimestamp: Long): Long = {
      element.timestamp
    }

    override def checkAndGetNextWatermark(lastElement: TemperatureReading, extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 1) // 简单地，我们可以将水位线设置为当前时间戳减1
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val source = new RandomTemperatureSource(100000) // 生成100000条数据
    val stream = env.addSource(source)

    // 为数据源分配时间戳和水位线
    val timestampedStream = stream.assignTimestampsAndWatermarks(new TemperatureTimestampExtractor)

    timestampedStream
      .keyBy(_.sensorId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .reduce((val1,val2)=>TemperatureReading(val1.timestamp,val1.sensorId,(val1.temperature+val2.temperature)/2))
//      .reduce(new ReduceFunction[TemperatureReading] {
//        override def reduce(value1: TemperatureReading, value2: TemperatureReading): TemperatureReading = {
//          val avgTemp = (value1.temperature + value2.temperature) /2
//          TemperatureReading(value1.timestamp, value1.sensorId, avgTemp)
//        }
//      })
      .print()
    env.execute("Flink Random Temperature Source with Event Time")

  }
}