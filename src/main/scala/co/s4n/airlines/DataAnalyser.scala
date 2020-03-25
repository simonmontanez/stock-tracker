package co.s4n.airlines

import org.apache.spark.sql.{DataFrame, Dataset}

class DataAnalyser extends SparkSessionWrapper{

  import spark.implicits._

  //val dataFrame: DataFrame = loadDataSet()

  //val dataSet: Dataset[?] = dataFrame.as[?]

  /*def loadDataSet(): DataFrame =  {
    spark
      .read
      .option("header", true)
      .csv("")
  }*/

}
