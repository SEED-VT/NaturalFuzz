package examples.mutants.Q3
import abstraction.{ SparkConf, SparkContext }
import capture.IOStreams._println
import scala.util.Random
object Q3_M1 {
  val MANUFACT = 1
  val MONTH = 11
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[1]")
    sparkConf.setAppName("TPC-DS Query 3")
    val ctx = new SparkContext(sparkConf)
    ctx.setLogLevel("ERROR")
    val store_sales = ctx.textFile(args(0)).map(_.split(","))
    val date_dim = ctx.textFile(args(1)).map(_.split(","))
    val item = ctx.textFile(args(2)).map(_.split(","))
    val map1 = store_sales.map(row => (row.last, row))
    val filter1 = date_dim.filter(row => row(8) == MONTH.toString)
    filter1.collect().foreach(_println)
    val map2 = filter1.map(row => (row.head, row))
    val join1 = map2.join(map1)
    join1.collect().foreach(_println)
    val map3 = join1.map(map3_f)
    val filter2 = item.filter(filter2_f)
    filter2.collect().foreach(_println)
    val map4 = filter2.map(row => (row.head, row))
    val join2 = map3.join(map4)
    join2.collect().foreach(_println)
    val map5 = join2.map(map5_f)
    val rbk1 = map5.reduceByKey(rbk1_f)
    rbk1.collect().foreach(_println)
    val map6 = rbk1.map(map6_f)
    map6.collect().foreach(_println)
  }
  def convertColToFloat(row: Array[String], col: Int): Float = {
    try {
      row(col).toFloat
    } catch {
      case _: Throwable => 0
    }
  }
  def map3_f(tup: (String, (Array[String], Array[String]))): (String, (Array[String], Array[String])) = {
    tup match {
      case (date_sk, (date_dim_row, ss_row)) =>
        (ss_row(1), (date_dim_row, ss_row))
    }
  }
  def filter2_f(row: Array[String]): Boolean = {
    row(13) != MANUFACT.toString
  }
  def map5_f(tup: (String, ((Array[String], Array[String]), Array[String]))): ((String, String, String), (String, String, String, Float)) = {
    tup match {
      case (item_sk, ((date_dim_row, ss_row), item_row)) =>
        val ss_ext_sales_price = convertColToFloat(ss_row, 14)
        val ss_sales_price = convertColToFloat(ss_row, 12)
        val ss_ext_discount_amt = convertColToFloat(ss_row, 13)
        val ss_net_profit = convertColToFloat(ss_row, 21)
        val sum = ss_net_profit + ss_sales_price + ss_ext_sales_price + ss_ext_discount_amt
        val d_year = date_dim_row(6)
        val i_brand = item_row(8)
        val i_brand_id = item_row(7)
        ((d_year, i_brand, i_brand_id), (d_year, i_brand, i_brand_id, sum))
    }
  }
  def rbk1_f(tup1: (String, String, String, Float), tup2: (String, String, String, Float)): (String, String, String, Float) = {
    (tup1, tup2) match {
      case ((d_year, i_brand, i_brand_id, v1), (_, _, _, v2)) =>
        (d_year, i_brand, i_brand_id, v1 + v2)
    }
  }
  def map6_f(tup: ((String, String, String), (String, String, String, Float))): (String, String, String, Float) = {
    tup match {
      case (_, (d_year, i_brand, i_brand_id, agg)) =>
        (d_year, i_brand_id, i_brand, agg)
    }
  }
}