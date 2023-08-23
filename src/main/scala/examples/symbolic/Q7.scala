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
object Q7 extends Serializable {
  def main(args: Array[String], expressionAccumulator: CollectionAccumulator[SymbolicExpression]): SymExResult = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("TPC-DS Query 7").setMaster("spark://zion-headnode:7077")
    val osc = SparkContext.getOrCreate(sparkConf)
    val sc = new SparkContextWithDP(osc)
    sc.setLogLevel("ERROR")
    val YEAR = 1999
    val GENDER = "M"
    val MS = "M"
    val ES = "Primary"

//    val p = "/TPCDS_1G_NOHEADER_NOCOMMAS"
//    args(0) = s"$p/customer_demographics"
    val customer_demographics = sc.textFileProv(args(0), _.split(","))
//    args(1) = s"$p/promotion"
    val promotion = sc.textFileProv(args(1), _.split(","))
//    args(2) = s"$p/store_sales"
    val store_sales = sc.textFileProv(args(2), _.split(","))
//    args(3) = s"$p/date_dim"
    val date_dim = sc.textFileProv(args(3), _.split(","))
//    args(4) = s"$p/item"
    val item = sc.textFileProv(args(4), _.split(","))

    val filter_cd = customer_demographics.filter { row => 
      val cd_gender = row(1)
      val cd_marital_status = row(2)
      val cd_education_status = row(3)
      _root_.monitoring.Monitors.monitorPredicateSymEx(
        cd_gender == GENDER && cd_marital_status == MS && cd_education_status == ES,
        (List(), List()),
        0,
        expressionAccumulator)
    }
    filter_cd.take(10).foreach(println)

    val filtered_p = promotion.filter { row => 
      val p_channel_email = row(9)
      val p_channel_event = row(14)
      _root_.monitoring.Monitors.monitorPredicateSymEx(p_channel_email == "N" && p_channel_event == "N",
        (List(), List()),
        1,
        expressionAccumulator)
    }
    filtered_p.take(10).foreach(println)

    val filtered_dd = date_dim.filter { row => 
      val d_year = row(6)
      _root_.monitoring.Monitors.monitorPredicateSymEx(d_year == YEAR.toString,
        (List(), List()),
        1,
        expressionAccumulator)
    }
    filtered_p.take(10).foreach(println)

    val map2 = filtered_dd.map(row => (row.head, row))
    val map1 = store_sales.map(row => (row.last, row))
    val join1 = _root_.monitoring.Monitors.monitorJoinSymEx(map1, map2, 3, expressionAccumulator)
    join1.take(10).foreach(println)

    val map3 = join1.map({
      case (date_sk, (ss_row, dd_row)) =>
        (ss_row(1), (ss_row, dd_row))
    })
    val map4 = item.map(row => (row.head, row))
    val join2 = _root_.monitoring.Monitors.monitorJoinSymEx(map3, map4, 4, expressionAccumulator)
    join2.take(10).foreach(println)

    val map5 = join2.map({
      case (item_sk, ((ss_row, dd_row), i_row)) =>
        (ss_row(3), (ss_row, dd_row, i_row))
    })
    val map9 = filter_cd.map(row => (row.head, row))
    val join3 = _root_.monitoring.Monitors.monitorJoinSymEx(map5, map9, 5, expressionAccumulator)
    join3.take(10).foreach(println)

    val map6 = join3.map({
      case (cdemo_sk, ((ss_row, dd_row, i_row), cd_row)) =>
        (ss_row(7), (ss_row, dd_row, i_row, cd_row))
    })
    val map8 = filtered_p.map(row => (row.head, row))
    val join4 = _root_.monitoring.Monitors.monitorJoinSymEx(map6, map8, 6, expressionAccumulator)
    join4.take(10).foreach(println)

    val map10 = join4.map({
      case (promo_sk, ((ss_row, dd_row, i_row, cd_row), p_row)) =>
        val ss_quantity = convertColToFloat(ss_row, 9)
        val ss_list_price = convertColToFloat(ss_row, 11)
        val ss_coupon_amt = convertColToFloat(ss_row, 18)
        val ss_sales_price = convertColToFloat(ss_row, 12)
        (i_row(1), (ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price, 1))
    })
    val rbk1 = map10.reduceByKey {
      case ((a1, a2, a3, a4, count1), (b1, b2, b3, b4, count2)) =>
        (a1 + b1, a2 + b2, a3 + b3, a4 + b4, count1 + count2)
    }
    rbk1.take(10).foreach(println)

    val map7 = rbk1.map({
      case (i_item_id, (sum1, sum2, sum3, sum4, count)) =>
        (i_item_id, sum1 / count, sum2 / count, sum3 / count, sum4 / count)
    })
    map7.take(10).foreach(println)

    val sortBy1 = map7.sortBy(_._1)
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
}