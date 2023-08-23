package examples.mutants.Q19
import abstraction.{ SparkConf, SparkContext }
import capture.IOStreams._println
object Q19_M4 extends Serializable {
  val YEAR = 1999
  val MONTH = 11
  val MANAGER = "50"
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("TPC-DS Query 19").setMaster("spark://zion-headnode:7077")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("ERROR")
    val date_dim = sc.textFile(args(0)).map(_.split(","))
    val store_sales = sc.textFile(args(1)).map(_.split(","))
    val item = sc.textFile(args(2)).map(_.split(","))
    val customer = sc.textFile(args(3)).map(_.split(","))
    val customer_address = sc.textFile(args(4)).map(_.split(","))
    val store = sc.textFile(args(5)).map(_.split(","))
    val filtered_i = item.filter(filtered_i_f)
    filtered_i.take(10).foreach(_println)
    val filtered_dd = date_dim.filter(filtered_dd_f)
    filtered_dd.take(10).foreach(_println)
    val map1 = date_dim.map(map1_f)
    val map2 = store_sales.map(map2_f)
    val join1 = map1.join(map2)
    join1.take(10).foreach(_println)
    val map3 = join1.map(map3_f)
    val map4 = item.map(map4_f)
    val join2 = map3.join(map4)
    join2.take(10).foreach(_println)
    val map5 = join2.map(map5_f)
    val map6 = customer.map(map6_f)
    val join3 = map5.join(map6)
    join3.take(10).foreach(_println)
    val map7 = join3.map(map7_f)
    val map8 = customer_address.map(map8_f)
    val join4 = map7.join(map8)
    join4.take(10).foreach(_println)
    val map9 = join4.map(map9_f)
    val map10 = store.map(map10_f)
    val join5 = map9.join(map10)
    join5.take(10).foreach(_println)
    val map11 = join5.map(map11_f)
    val filter1 = map11.filter(filter1_f)
    filter1.take(10).foreach(_println)
    val map12 = filter1.map(map12_f)
    val rbk1 = map12.reduceByKey(rbk1_f)
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
  def filtered_i_f(row: Array[String]) = {
    val i_manager_id = row(row.length - 2)
    i_manager_id == MANAGER
  }
  def filtered_dd_f(row: Array[String]) = {
    val d_moy = row(8)
    val d_year = row(6)
    d_moy == MONTH.toString && d_year == YEAR.toString
  }
  def map1_f(row: Array[String]) = {
    (row.head, row)
  }
  def map2_f(row: Array[String]) = {
    (row.last, row)
  }
  def map3_f(row: (String, (Array[String], Array[String]))) = {
    row match {
      case (_, (dd_row, ss_row)) =>
        (ss_row(1), (dd_row, ss_row))
    }
  }
  def map4_f(row: Array[String]) = {
    (row.head, row)
  }
  def map5_f(row: (String, ((Array[String], Array[String]), Array[String]))) = {
    row match {
      case (_, ((dd_row, ss_row), i_row)) =>
        (ss_row(2), (dd_row, ss_row, i_row))
    }
  }
  def map6_f(row: Array[String]) = {
    (row.head, row)
  }
  def map7_f(row: (String, ((Array[String], Array[String], Array[String]), Array[String]))) = {
    row match {
      case (_, ((dd_row, ss_row, i_row), c_row)) =>
        (c_row(4), (dd_row, ss_row, i_row, c_row))
    }
  }
  def map8_f(row: Array[String]) = {
    (row.head, row)
  }
  def map9_f(row: (String, ((Array[String], Array[String], Array[String], Array[String]), Array[String]))) = {
    row match {
      case (_, ((dd_row, ss_row, i_row, c_row), ca_row)) =>
        (ss_row(6), (dd_row, ss_row, i_row, c_row, ca_row))
    }
  }
  def map10_f(row: Array[String]) = {
    (row.head, row)
  }
  def map11_f(row: (String, ((Array[String], Array[String], Array[String], Array[String], Array[String]), Array[String]))) = {
    row match {
      case (_, ((dd_row, ss_row, i_row, c_row, ca_row), s_row)) =>
        (dd_row, ss_row, i_row, c_row, ca_row, s_row)
    }
  }
  def filter1_f(row: (Array[String], Array[String], Array[String], Array[String], Array[String], Array[String])) = {
    row match {
      case (_, _, _, _, ca_row, s_row) =>
        val ca_zip = getColOrEmpty(ca_row, 9)
        val s_zip = s_row(25)
        ca_zip.take(5) > s_zip.take(5)
    }
  }
  def map12_f(row: (Array[String], Array[String], Array[String], Array[String], Array[String], Array[String])) = {
    row match {
      case (_, ss_row, i_row, _, _, _) =>
        val ss_ext_sales_price = convertColToFloat(ss_row, 14)
        val i_brand_id = i_row(7)
        val i_brand = i_row(8)
        val i_manufact_id = i_row(13)
        val i_manufact = i_row(14)
        ((i_brand_id, i_brand, i_manufact_id, i_manufact), ss_ext_sales_price)
    }
  }
  def rbk1_f(x: Float, y: Float) = {
    x + y
  }
}