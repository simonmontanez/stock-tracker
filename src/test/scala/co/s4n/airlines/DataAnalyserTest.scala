package co.s4n.airlines


import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec

class DataAnalyserTest
  extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  val dataAnalyser = new DataAnalyser()

  describe("Trades stats ") {

    it("returns last 6month Trades") { //1. ¿Cuales son las diferentes acciones de los últimos 6 meses?, ¿cuál es su moneda(currency) y cúal es su valor promedio?
      dataAnalyser.last6monthTrades().show(20)
    }

    it("returns last Three Month Trades Average Price") { //2. Buscar en los últimos tres meses cuáles acciones con más ventas y el precio promedio por cada mes
      dataAnalyser.lastThreeMonthTradesAveragePrice().show(50)
    }

    it("returns opening Closing Price Trades") { //3. De un listado de acciones,  listar el valor de apertura, de cierre, el valor mínimo y el valor máximo de la última semana
      dataAnalyser.openingClosingPriceTrades(Seq("FPE3", "TKA", "EVK")).show(25)
    }


    it("returns trade Trending") { // 4. Obtenga la tendencia de una acciones en los últimos dias con respecto al día anterior
      dataAnalyser.tradeTrending().show(25)
    }

  }


}
