package examples.mutants.Q12
import abstraction.{ SparkConf, SparkContext }
import capture.IOStreams._println
import java.time.LocalDate
import java.time.format.DateTimeFormatter
object Q12_M4 extends Serializable {
  val YEAR = 1999
  val START_DATE = s"$YEAR-01-01"
  val END_DATE = s"$YEAR-02-01"
  val CAT = List("Home", "Electronics", "Shoes")
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("TPC-DS Query 12").setMaster("spark://zion-headnode:7077")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("ERROR")
    val web_sales = sc.textFile(args(0)).map(_.split(","))
    val date_dim = sc.textFile(args(1)).map(_.split(","))
    val item = sc.textFile(args(2)).map(_.split(","))
    val filtered_item = item.filter(filtered_item_f)
    filtered_item.take(10).foreach(_println)
    val filtered_dd = date_dim.filter(filtered_dd_f)
    filtered_dd.take(10).foreach(_println)
    val map1 = web_sales.map(map1_f)
    val map7 = filtered_item.map(map7_f)
    val join1 = map1.join(map7)
    join1.take(10).foreach(_println)
    val map2 = join1.map(map2_f)
    val map8 = filtered_dd.map(map8_f)
    val join2 = map2.join(map8)
    join2.take(10).foreach(_println)
    val map3 = join2.map(map3_f)
    val map4 = map3.map(map4_f)
    val rbk1 = map4.reduceByKey(rbk1_f)
    rbk1.take(10).foreach(_println)
    val rbk2 = map3.reduceByKey(rbk2_f)
    rbk2.take(10).foreach(_println)
    val map5 = rbk2.map(map5_f)
    val join3 = map5.join(rbk1)
    join3.take(10).foreach(_println)
    val map6 = join3.map(map6_f)
    val sortBy1 = map6.sortBy(_._3)
    sortBy1.take(10).foreach(_println)
  }
  def convertColToFloat(row: Array[String], col: Int): Float = {
    try {
      row(col).toFloat
    } catch {
      case _ => 0
    }
  }
  def isBetween(date: String, start: String, end: String): Boolean = {
    try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val givenDate = LocalDate.parse(date, formatter)
      val startDate = LocalDate.parse(start, formatter)
      val endDate = LocalDate.parse(end, formatter)
      givenDate.isAfter(startDate) && givenDate.isBefore(endDate)
    } catch {
      case _ => false
    }
  }
  def filtered_item_f(row: Array[String]) = {
    val category = row(12)
    category == CAT(0) || category == CAT(1) || category == CAT(2)
  }
  def filtered_dd_f(row: Array[String]) = {
    val d_date = row(2)
    isBetween(d_date, START_DATE, END_DATE)
  }
  def map1_f(row: Array[String]) = {
    (row(2), row)
  }
  def map7_f(row: Array[String]) = {
    (row.head, row)
  }
  def map2_f(row: (String, (Array[String], Array[String]))) = {
    row match {
      case (item_sk, (ws_row, i_row)) =>
        (ws_row.last, (ws_row, i_row))
    }
  }
  def map8_f(row: Array[String]) = {
    (row.head, row)
  }
  def map3_f(row: (String, ((Array[String], Array[String]), Array[String]))) = {
    row match {
      case (_, ((ws_row, i_row), dd_row)) =>
        val i_item_id = i_row(1)
        val i_item_desc = i_row(4)
        val i_category = i_row(12)
        val i_class = i_row(10)
        val i_current_price = i_row(5)
        val ws_ext_sales_price = convertColToFloat(ws_row, 22)
        ((i_item_id, i_item_desc, i_category, i_class, i_current_price), ws_ext_sales_price)
    }
  }
  def map4_f(row: ((String, String, String, String, String), Float)) = {
    row match {
      case ((i_item_id, i_item_desc, i_category, i_class, i_current_price), ws_ext_sales_price) =>
        (i_class, ws_ext_sales_price)
    }
  }
  def rbk1_f(x: Float, y: Float) = {
    x + y
  }
  def rbk2_f(x: Float, y: Float) = {
    x * y
  }
  def map5_f(row: ((String, String, String, String, String), Float)) = {
    row match {
      case ((i_item_id, i_item_desc, i_category, i_class, i_current_price), ws_ext_sales_price) =>
        (i_class, (i_item_id, i_item_desc, i_category, i_current_price, ws_ext_sales_price))
    }
  }
  def map6_f(row: (String, ((String, String, String, String, Float), Float))) = {
    row match {
      case (i_class, ((i_item_id, i_item_desc, i_category, i_current_price, ws_ext_sales_price), class_rev)) =>
        (i_item_id, i_item_desc, i_category, i_class, i_current_price, ws_ext_sales_price, ws_ext_sales_price / class_rev)
    }
  }
}