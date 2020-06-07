
import aggs.ECommercyStatistics
import com.mongodb.spark.MongoSpark
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{ECommercyConsts, ECommercyUtils}


object Main extends ECommercyConsts with Logging {


  def main(args: Array[String]) {

    val env = scala.util.Properties.envOrElse("ENVIRONMENT", "dev")
    val delay = scala.util.Properties.envOrElse("DELAY", "5").toInt
    val kafkaTopics = scala.util.Properties.envOrElse("KAFKA_TOPICS", "commerceRecords")
    val bootstrapServers = scala.util.Properties.envOrElse("BOOTSTRAP_SERVERS", "localhost:9092")



    val kafkaConfPath = s"/$env/kafka-$env.properties"

    val kafkaConfMap: collection.mutable.Map[String, String] = ECommercyUtils.loadConfig(kafkaConfPath)
    kafkaConfMap.put("bootstrap.servers", bootstrapServers)

    val spark = ECommercyUtils.startSparkSession()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(delay))

    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = kafkaTopics.split(',')
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaConfMap)
    )

    val schema = ECommercyUtils.loadSchema()

    val aggFuncs: List[DataFrame => DataFrame] = List(ECommercyStatistics.totalBrandPriceByEvent)

    applyAggFuncs(stream, spark, schema, aggFuncs)

    ssc.start()
    ssc.awaitTermination()
  }


  def applyAggFuncs(stream: InputDStream[ConsumerRecord[String, String]], spark: SparkSession, schema: StructType, functions: List[DataFrame => DataFrame]): Unit = {
    log.info("Start processing")
    functions.foreach {
      aggFunction =>
        stream.foreachRDD { rdd =>
          if (!rdd.isEmpty) {
            val mappedRDD = rdd.map(_.value())
            val df = ECommercyUtils.rddToDF(mappedRDD, spark, schema)
            val product_df = aggFunction(df)
              MongoSpark.save(product_df.write.mode("append"))
          } else {
            log.warn("RDD is empty")
          }
        }
    }
  }

}

// сделать обработку невозможности сохранить в бд
