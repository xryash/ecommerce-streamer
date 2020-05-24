package aggs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import util.ECommercyConsts

object ECommercyStatistics extends ECommercyConsts {
  def totalBrandPriceByEvent(df: DataFrame): DataFrame = df
    .groupBy(BRAND_NAME_FIELD, EVENT_TYPE_FIELD)
    .agg(
      sum(PRICE_FIELD),
      min(TIMESTAMP_FIELD).alias(START_PERIOD_DATE),
      max(TIMESTAMP_FIELD).alias(END_PERIOD_DATE)
    )
}
