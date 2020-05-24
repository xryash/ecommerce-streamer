
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{ECommercyConsts, ECommercyUtils}


object Main extends ECommercyConsts with Logging {


  def main(args: Array[String]) {

    val env = scala.util.Properties.envOrElse("ENVIRONMENT", "dev")

    val kafkaConfPath = s"/$env/kafka-$env.properties"
    val appConfPath = s"/$env/application-$env.properties"

    val kafkaConfMap: Map[String, String] = ECommercyUtils.loadConfig(kafkaConfPath)
    val appConfMap: Map[String, String] = ECommercyUtils.loadConfig(appConfPath)

    val spark = ECommercyUtils.startSparkSession()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(appConfMap(DELAY_CONF).toLong))

    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = appConfMap.get(KAFKA_TOPICS_CONF).toList

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaConfMap)
    )


    val schema = ECommercyUtils.loadSchema()

    stream.foreachRDD(rdd => rdd.count())
    /**
      * val offsetRanges = Array(
      * // topic, partition, inclusive starting offset, exclusive ending offset
      * OffsetRange("test", 0, 0, 100),
      * OffsetRange("test", 1, 0, 100)
      * )
      */
    ssc.start()
    ssc.awaitTermination()
  }


  def applyAggFuncs(stream: DStream[String], spark: SparkSession, schema: StructType, functions: List[DataFrame => DataFrame]): Unit = {
    functions.foreach {
      aggFunction =>
        stream.foreachRDD { rdd =>
          if (!rdd.isEmpty) {
            val df = ECommercyUtils.rddToDF(rdd, spark, schema)
            val product_df = aggFunction(df)
            MongoSpark.save(product_df.write.mode("append"))

          }
          else {}
        }
    }


  }

}
// сделать обработку невозможности сохранить в бд

// логгирование
// sbt сборка