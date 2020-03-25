package co.s4n.airlines


import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec

class DataAnalyserTest
  extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  val dataAnalyser = new DataAnalyser()


}
