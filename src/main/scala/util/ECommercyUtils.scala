package util


import java.io.FileNotFoundException
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.io.Source


object ECommercyUtils extends ECommercyConsts {
  def loadSchema(): StructType = {
    StructType(
      List(
        StructField(EVENT_TYPE_FIELD, StringType),
        StructField(PRODUCT_ID_FIELD, StringType),
        StructField(CATEGORY_ID_FIELD, StringType),
        StructField(CATEGORY_CODE, StringType),
        StructField(BRAND_NAME_FIELD, StringType),
        StructField(PRICE_FIELD, DoubleType),
        StructField(USER_ID_FIELD, StringType),
        StructField(TIMESTAMP_FIELD, TimestampType),
        StructField(SOURCE_ID_FIELD, IntegerType),
        StructField(PARTITION_NAME_FIELD, StringType)
      )
    )
  }

  def rddToDF(rdd: RDD[String], spark: SparkSession, schema: StructType): DataFrame = {
    import spark.implicits._
    spark.read.schema(schema).json(spark.createDataset(rdd))
  }

  def startSparkSession(sparkConf: SparkConf): SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  def startSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .getOrCreate()
  }

  def loadConfig(path: String): Map[String, String] = {
    val url = getClass.getResource(path)
    val properties: Properties = new Properties()

    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    else {
      //logger.error("properties file cannot be loaded at path " +path)
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    properties.asScala.toMap

  }


}


