package co.s4n.airlines

import co.s4n.airlines.processors.TradesStats
import org.apache.spark.sql.types.{DataType, DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset}
class DataAnalyser extends TradesStats with SparkSessionWrapper{

  import spark.implicits._

  val dataFrame: DataFrame = loadDataSet()

  //val dataSet: Dataset[?] = dataFrame.as[?]

  def loadDataSet(): DataFrame =  {
  //  val sc = spark.sparkContext

    //sc.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIBXSZZ5DF56OT5WQ")
    //sc.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    val structureSchema = new StructType()
      .add("ISIN", StringType)
      .add("Mnemonic", StringType)
      .add("SecurityDesc", StringType)
      .add("SecurityType", StringType)
      .add("Currency", StringType)
      .add("SecurityID", StringType)
      .add("Date", DateType)
      .add("Time", StringType)
      .add("StartPrice", DoubleType)
      .add("MaxPrice", DoubleType)
      .add("MinPrice", DoubleType)
      .add("EndPrice",  DoubleType )
      .add("TradedVolume", IntegerType)
      .add("NumberOfTrades", IntegerType)

    spark
      .read
      .option("header", true)
      .format("csv")
      .schema(structureSchema)
//      .load("s3a://deutsche-boerse-xetra-pds/2020*/*")
      //.load("data-sets/2020-*/*", "data-sets/2019-*/*")
      .load("data-sets/2020-*/*")
  }

}
