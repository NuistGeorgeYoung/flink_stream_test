package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import play.api.libs.json.{Json, Reads, Writes}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.{Properties, Random}

object TranKafkaProducer {
  case class Transaction(transaction_id:Int, customer_id:Int, store_id:Int, price:Float, product_id:Int, date:String, time:String, timestamp:Long)
  var tran_id =40000
  var cus_id =10000
  var store_id = 2000
  var price = (10,1000)
  private val product_id = 1000
//  val product = Array("apple","huawei","xiaomi","samsung","vivo")
  var util = LocalDateTime.of(2024,1,1,0,0,0)
  private val random = new Random()
  def make()={
    val time = util.toLocalTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    val date = util.toLocalDate.toString
    util=util.plus(random.nextInt(1000),ChronoUnit.MILLIS)
    Transaction({tran_id+=1;tran_id}
      ,1+random.nextInt(cus_id)
      ,1+random.nextInt(store_id)
      ,price._1+random.nextInt(price._2-price._1)
      ,1+random.nextInt(product_id)
      ,date,time,System.currentTimeMillis())
  }
  def randJsonTran(): String ={
    Json.toJson(make()).toString()
  }
  case class ByProduct(productId:Int,sumAmount:Float,tranCount:Int)
  case class ByStore(storeId:Int,sumAmount:Float,tranCount:Int)

  object ByProduct{
    implicit val reads:Reads[ByProduct]=Json.reads[ByProduct]
    implicit val writes:Writes[ByProduct]=Json.writes[ByProduct]
  }

  object ByStore {
    implicit val reads: Reads[ByStore] = Json.reads[ByStore]
    implicit val writes: Writes[ByStore] = Json.writes[ByStore]
  }
  def jsonToTran(json:String): Transaction ={
    Json.fromJson[Transaction](Json.parse(json)).get
  }

  def toStoreTran(store:ByStore):String={
    Json.toJson(store).toString()
  }

  def toProductTran(product: ByStore): String = {
    Json.toJson(product).toString()
  }

  object Transaction{
    implicit val reads :Reads[Transaction] =Json.reads[Transaction]
    implicit val writes: Writes[Transaction] =Json.writes[Transaction]
  }
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master01:9092,master02:9092,worker01:9092")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"100")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG,"2")
    properties.setProperty(ProducerConfig.ACKS_CONFIG,"1")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"50")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val topic = "tran_test"
    val producer:KafkaProducer[String,String] = new KafkaProducer(properties)
    for (i <- 1 to 100000){
      val record:ProducerRecord[String,String] =
        new ProducerRecord[String, String](topic,i%3,System.currentTimeMillis(),s"$i",randJsonTran())
      producer.send(record)
      Thread.sleep(5+random.nextInt(20))
    }
    producer.flush()
    producer.close()
  }
}

