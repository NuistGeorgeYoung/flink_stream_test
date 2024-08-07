package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.{Properties, Random}

object MyKafkaProducer {
  case class Transaction(transaction_id:Int,customer_id:Int,store_id:Int,price:Float,product:String,date:String,time:String)
  var tran_id =10000
  var cus_id =510
  var store_id = 6
  var price = (10,1000)
  val product = Array("apple","huawei","xiaomi","samsung","vivo")
  var util = LocalDateTime.of(2019,1,1,0,0,0)
  private val random = new Random()
  def make()={
    val data = util.toLocalDate.toString
    val time = util.toLocalTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    util=util.plus(random.nextInt(1000),ChronoUnit.MILLIS)
    Transaction({tran_id+=1;tran_id},random.nextInt(cus_id),random.nextInt(store_id),price._1+random.nextInt(price._2-price._1),product(random.nextInt(product.length)),data,time)
  }
  def randJsonTran(): String ={
    Json.toJson(make()).toString()
  }

  def jsonToTran(json:String): Transaction ={
    Json.fromJson[Transaction](Json.parse(json)).get
  }
  object Transaction{
    implicit val reads :Reads[Transaction] =Json.reads[Transaction]
    implicit val writes: Writes[Transaction] =Json.writes[Transaction]
  }
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"single01:9092")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"100")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG,"2")
    properties.setProperty(ProducerConfig.ACKS_CONFIG,"1")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"50")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val topic = "test01"
    val producer:KafkaProducer[Int,String] = new KafkaProducer(properties)
    for (i <- 1 to 100000){
      val record = new ProducerRecord[Int, String](topic,0,System.currentTimeMillis(),i,randJsonTran())
      println(record)
      producer.send(record)
      Thread.sleep(5+random.nextInt(20))
    }
    producer.flush()
    producer.close()
  }
}
