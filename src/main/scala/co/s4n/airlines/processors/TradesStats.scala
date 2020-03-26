package co.s4n.airlines.processors

import co.s4n.airlines.DataAnalyser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

trait TradesStats {
  this: DataAnalyser =>

  import spark.implicits._

  def last6monthTrades(): DataFrame = {

    dataFrame
      .where('Date >= add_months( current_date(), -6))
      .groupBy('Mnemonic, 'Currency )
      .avg("EndPrice").as("avgPrice")
      .orderBy("avgPrice")

  }

  def lastThreeMonthTradesAveragePrice(): DataFrame = {

    val windowSpecAgg  = Window.partitionBy('Mnemonic)

    dataFrame
      .where('Date >= add_months( current_date(), -3))
      .groupBy('Mnemonic, year('Date).as('yearDate), month('Date).as('monthDate))
      .agg(
        sum('NumberOfTrades).alias("totalNumberOfTradesByMonth"),
        avg("EndPrice").alias("avgPrice")
      )
      .withColumn("totalThreeMonths", sum('totalNumberOfTradesByMonth).over(windowSpecAgg))
      .orderBy('totalThreeMonths.desc)


  }

  def openingClosingPriceTrades(trades: Seq[String]): DataFrame = {

      val startWeekDay = DateTime.now().withDayOfWeek(1).toString("yyyy-MM-dd")

      dataFrame
        .select('Date, 'Mnemonic, 'StartPrice, 'EndPrice, 'MinPrice, 'MaxPrice)
        .where( ( 'Mnemonic isin (trades:_*) ) && 'Date >= lit(startWeekDay))
        .orderBy('Date)

  }

  def tradeTrending(): DataFrame = {

    val startMonthDay = DateTime.now().withDayOfMonth(1).toString("yyyy-MM-dd")

    val windowSpecAggTimeLong  = Window.partitionBy('Mnemonic, 'Date).orderBy('TimeLong.desc)

    val windowSpecAggRowsBetween  = Window.partitionBy('Mnemonic ).orderBy('Date.desc).rowsBetween(Window.currentRow, 1)

    val priceDiff = 'EndPrice - 'AvgEndPrice

    val trend = when(col("DifferenceEndPrice").===(0) || col("DifferenceEndPrice").isNull, "Stable")
      .when(col("DifferenceEndPrice").>(0), "Up")
      .otherwise("Down")

    dataFrame
        .where(('Mnemonic isNotNull) && ('Date isNotNull) && ('Time isNotNull) && ('Date >= lit(startMonthDay)) )
        .withColumn("TimeLong", unix_timestamp(concat_ws(" ", 'Date, 'Time), "yyyy-MM-dd HH:mm"))
        .withColumn( "MaxTime", max('TimeLong) over(windowSpecAggTimeLong))
        .where('TimeLong === 'MaxTime)
        .withColumn("AvgEndPrice", avg('EndPrice).over(windowSpecAggRowsBetween) )
        .withColumn("DifferenceEndPrice", priceDiff)
        .withColumn("Trend", trend)
        .select( 'Mnemonic, 'Date, 'EndPrice, 'DifferenceEndPrice, 'Trend)
        .orderBy(  'Mnemonic, 'Date.desc)



  }

}
