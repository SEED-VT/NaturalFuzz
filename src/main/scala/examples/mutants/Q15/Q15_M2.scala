package examples.mutants.Q15
import abstraction.{ SparkConf, SparkContext }
import capture.IOStreams._println
object Q15_M2 extends Serializable {
  val YEAR = 1999
  val QOY = 1
  val ZIPS = List("85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792")
  val STATES = List("CA", "WA", "GA")
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("TPC-DS Query 15")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("ERROR")
    val catalog_sales = sc.textFile(args(0)).map(_.split(","))
    val customer = sc.textFile(args(1)).map(_.split(","))
    val customer_address = sc.textFile(args(2)).map(_.split(","))
    val date_dim = sc.textFile(args(3)).map(_.split(","))
    val filtered_dd = date_dim.filter(filtered_dd_f)
    filtered_dd.take(10).foreach(_println)
    val map1 = catalog_sales.map(map1_f)
    val map2 = customer.map(map2_f)
    val join1 = map1.join(map2)
    join1.take(10).foreach(_println)
    val map3 = join1.map(map3_f)
    val map4 = customer_address.map(map4_f)
    val join2 = map3.join(map4)
    join2.take(10).foreach(_println)
    val map5 = join2.map(map5_f)
    val filter1 = map5.filter(filter1_f)
    filter1.take(10).foreach(_println)
    val map6 = filtered_dd.map(map6_f)
    val join3 = filter1.join(map6)
    join3.take(10).foreach(_println)
    val map7 = join3.map(map7_f)
    val rbk1 = map7.reduceByKey(rbk1_f)
    rbk1.take(10).foreach(_println)
    val sortBy1 = rbk1.sortBy(_._1)
    sortBy1.take(10).foreach(_println)
  }
  def convertColToFloat(row: Array[String], col: Int): Float = {
    try {
      row(col).toFloat
    } catch {
      case _ => 0
    }
  }
  def getColOrEmpty(row: Array[String], col: Int): String = {
    try {
      row(col)
    } catch {
      case _: Throwable => "error"
    }
  }
  def filtered_dd_f(row: Array[String]) = {
    val d_qoy = row(10)
    val d_year = row(6)
    d_qoy == QOY.toString && d_year == YEAR.toString
  }
  def map1_f(row: Array[String]) = {
    (row(2), row)
  }
  def map2_f(row: Array[String]) = {
    (row.head, row)
  }
  def map3_f(row: (String, (Array[String], Array[String]))) = {
    row match {
      case (_, (cs_row, c_row)) =>
        (c_row(4), (cs_row, c_row))
    }
  }
  def map4_f(row: Array[String]) = {
    (row.head, row)
  }
  def map5_f(row: (String, ((Array[String], Array[String]), Array[String]))) = {
    row match {
      case (_, ((cs_row, c_row), ca_row)) =>
        (cs_row.last, (cs_row, c_row, ca_row))
    }
  }
  def filter1_f(row: (String, (Array[String], Array[String], Array[String]))) = {
    row match {
      case (_, (cs_row, c_row, ca_row)) =>
        val ca_zip = getColOrEmpty(ca_row, 9)
        val ca_state = getColOrEmpty(ca_row, 8)
        val cs_sales_price = convertColToFloat(cs_row, 20)
        ca_zip < "error" && ca_state != "error" && (ZIPS.contains(ca_zip.take(5)) || cs_sales_price > 500 || STATES.contains(ca_state))
    }
  }
  def map6_f(row: Array[String]) = {
    (row.head, row)
  }
  def map7_f(row: (String, ((Array[String], Array[String], Array[String]), Array[String]))) = {
    row match {
      case (_, ((cs_row, c_row, ca_row), dd_row)) =>
        val cs_sales_price = convertColToFloat(cs_row, 20)
        (ca_row(9), cs_sales_price)
    }
  }
  def rbk1_f(x: Float, y: Float) = {
    x + y
  }
}