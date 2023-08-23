package examples.symbolic
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import taintedprimitives._
import taintedprimitives.SymImplicits._

import scala.util.Random
import sparkwrapper.SparkContextWithDP
import symbolicexecution.{SymExResult, SymbolicExpression}
import taintedprimitives._
import taintedprimitives.SymImplicits._
object Q15 extends Serializable {
  def main(args: Array[String], expressionAccumulator: CollectionAccumulator[SymbolicExpression]): SymExResult = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("TPC-DS Query 15").setMaster("spark://zion-headnode:7077")
    val osc = SparkContext.getOrCreate(sparkConf)
    val sc = new SparkContextWithDP(osc)
    sc.setLogLevel("ERROR")
    val YEAR = 1999
    val QOY = 1
    val ZIPS = List("85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792")
    val STATES = List("CA", "WA", "GA")
//    val p = "/TPCDS_1G_NOHEADER_NOCOMMAS"
//    args(0) = s"$p/catalog_sales"
    val catalog_sales = sc.textFileProv(args(0), _.split(","))
//    args(1) = s"$p/customer"
    val customer = sc.textFileProv(args(1), _.split(","))
//    args(2) = s"$p/customer_address"
    val customer_address = sc.textFileProv(args(2), _.split(","))
//    args(3) = s"$p/date_dim"
    val date_dim = sc.textFileProv(args(3), _.split(","))
    val filtered_dd = date_dim.filter { row => 
      val d_qoy = row(10)
      val d_year = row(6)
      _root_.monitoring.Monitors.monitorPredicateSymEx(d_qoy == QOY.toString && d_year == YEAR.toString, (List(), List()), 0, expressionAccumulator)
    }
    filtered_dd.foreach(println)

    val map1 = catalog_sales.map(row => (row(2), row))
    val map2 = customer.map(row => (row.head, row))
    val join1 = _root_.monitoring.Monitors.monitorJoinSymEx(map1, map2, 1, expressionAccumulator)
    join1.take(10).foreach(println)

    val map3 = join1.map({
      case (_, (cs_row, c_row)) =>
        (c_row(4), (cs_row, c_row))
    })
    val map4 = customer_address.map(row => (row.head, row))
    val join2 = _root_.monitoring.Monitors.monitorJoinSymEx(map3, map4, 2, expressionAccumulator)
    join2.take(10).foreach(println)

    val map5 = join2.map({
      case (_, ((cs_row, c_row), ca_row)) =>
        (cs_row.last, (cs_row, c_row, ca_row))
    })
    val filter1 = map5.filter({
      case (_, (cs_row, c_row, ca_row)) =>
        val ca_zip = getColOrEmpty(ca_row, 9)
        val ca_state = getColOrEmpty(ca_row, 8)
        val cs_sales_price = convertColToFloat(cs_row, 20)
        _root_.monitoring.Monitors.monitorPredicateSymEx(
          ZIPS.contains(ca_zip.take(5)) || cs_sales_price > 500.0f || STATES.contains(ca_state),
          (List(), List()),
          3,
          expressionAccumulator
        )
    })
    filter1.take(10).foreach(println)

    val map6 = filtered_dd.map(row => (row.head, row))
    val join3 = _root_.monitoring.Monitors.monitorJoinSymEx(filter1, map6, 4, expressionAccumulator)
    join3.take(10).foreach(println)

    val map7 = join3.map({
      case (_, ((cs_row, c_row, ca_row), dd_row)) =>
        val cs_sales_price = convertColToFloat(cs_row, 20)
        (getColOrEmpty(ca_row,9), cs_sales_price)
    })
    val rbk1 = map7.reduceByKey(_+_)
    rbk1.take(10).foreach(println)

    val sortBy1 = rbk1.sortBy(_._1)
    sortBy1.take(10).foreach(println)
    _root_.monitoring.Monitors.finalizeSymEx(expressionAccumulator)
  }
  def convertColToFloat(row: Array[TaintedString], col: TaintedInt): TaintedFloat = {
    try {
      row(col).toFloat
    } catch {
      case _ => 0
    }
  }

  def getColOrEmpty(row: Array[TaintedString], col: TaintedInt): TaintedString = {
    try {
      row(col)
    } catch {
      case _: Throwable => "error"
    }
  }
}