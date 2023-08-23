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
object Q19 extends Serializable {
  def main(args: Array[String], expressionAccumulator: CollectionAccumulator[SymbolicExpression]): SymExResult = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("TPC-DS Query 19").setMaster("spark://zion-headnode:7077")
    val osc = SparkContext.getOrCreate(sparkConf)
    val sc = new SparkContextWithDP(osc)
    sc.setLogLevel("ERROR")
    val YEAR = "1999"
    val MONTH = "11"
    val MANAGER = "50"
//    val p = "/TPCDS_1G_NOHEADER_NOCOMMAS"
//    args(0) = s"$p/date_dim"
    val date_dim = sc.textFileProv(args(0), _.split(","))
//    args(1) = s"$p/store_sales"
    val store_sales = sc.textFileProv(args(1), _.split(","))
//    args(2) = s"$p/item"
    val item = sc.textFileProv(args(2), _.split(","))
//    args(3) = s"$p/customer"
    val customer = sc.textFileProv(args(3), _.split(","))
//    args(4) = s"$p/customer_address"
    val customer_address = sc.textFileProv(args(4), _.split(","))
//    args(5) = s"$p/store"
    val store = sc.textFileProv(args(5), _.split(","))
    val filtered_i = item.filter { row =>
      val i_manager_id = row(row.length - 2)
      _root_.monitoring.Monitors.monitorPredicateSymEx(i_manager_id == MANAGER, (List(), List()), 0, expressionAccumulator)
    }
    filtered_i.take(10).foreach(println)

    val filtered_dd = date_dim.filter { row =>
      val d_moy = row(8)
      val d_year = row(6)
      _root_.monitoring.Monitors.monitorPredicateSymEx(d_moy == MONTH && d_year == YEAR, (List(), List()), 1, expressionAccumulator)
    }
    filtered_dd.take(10).foreach(println)

    val map1 = date_dim.map(row => (row.head, row))
    val map2 = store_sales.map(row => (row.last, row))
    val join1 = _root_.monitoring.Monitors.monitorJoinSymEx(map1, map2, 2, expressionAccumulator)
    join1.take(10).foreach(println)

    val map3 = join1.map({
      case (_, (dd_row, ss_row)) =>
        (ss_row(1), (dd_row, ss_row))
    })
    val map4 = item.map(row => (row.head, row))
    val join2 = _root_.monitoring.Monitors.monitorJoinSymEx(map3, map4, 3, expressionAccumulator)
    join2.take(10).foreach(println)

    val map5 = join2.map({
      case (_, ((dd_row, ss_row), i_row)) =>
        (ss_row(2), (dd_row, ss_row, i_row))
    })
    val map6 = customer.map(row => (row.head, row))
    val join3 = _root_.monitoring.Monitors.monitorJoinSymEx(map5, map6, 4, expressionAccumulator)
    join3.take(10).foreach(println)

    val map7 = join3.map({
      case (_, ((dd_row, ss_row, i_row), c_row)) =>
        (c_row(4), (dd_row, ss_row, i_row, c_row))
    })
    val map8 = customer_address.map(row => (row.head, row))
    val join4 = _root_.monitoring.Monitors.monitorJoinSymEx(map7, map8, 5, expressionAccumulator)
    join4.take(10).foreach(println)

    val map9 = join4.map({
      case (_, ((dd_row, ss_row, i_row, c_row), ca_row)) =>
        (ss_row(6), (dd_row, ss_row, i_row, c_row, ca_row))
    })
    val map10 = store.map(row => (row.head, row))
    val join5 = _root_.monitoring.Monitors.monitorJoinSymEx(map9, map10, 6, expressionAccumulator)
    join5.take(10).foreach(println)

    val map11 = join5.map({
      case (_, ((dd_row, ss_row, i_row, c_row, ca_row), s_row)) =>
        (dd_row, ss_row, i_row, c_row, ca_row, s_row)
    })
    val filter1 = map11.filter({
      case (_, _, _, _, ca_row, s_row) =>
        val ca_zip = getColOrEmpty(ca_row, 9)
        val s_zip = s_row(25)
        _root_.monitoring.Monitors.monitorPredicateSymEx(ca_zip.take(5) != s_zip.take(5), (List(), List()), 7, expressionAccumulator)
    })
    filter1.take(10).foreach(println)

    val map12 = filter1.map({
      case (_, ss_row, i_row, _, _, _) =>
        val ss_ext_sales_price = convertColToFloat(ss_row, 14)
        val i_brand_id = i_row(7)
        val i_brand = i_row(8)
        val i_manufact_id = i_row(13)
        val i_manufact = i_row(14)
        ((i_brand_id, i_brand, i_manufact_id, i_manufact), ss_ext_sales_price)
    })
    val rbk1 = map12.reduceByKey(_ + _)
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