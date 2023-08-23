package examples.faulty

import abstraction.{SparkConf, SparkContext}
import capture.IOStreams._println

object Q1 {
  var avg = 0.0f
  val YEAR = 1999
  val STATE = "TN"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("TPC-DS Query 1")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")


    val Array(store_returns, date_dim, store, customer) = {
      val store_returns = sc.textFile(args(0)).map(_.split(","))
      val date_dim = sc.textFile(args(1)).map(_.split(","))
      val store = sc.textFile(args(2)).map(_.split(","))
      val customer = sc.textFile(args(3)).map(_.split(","))
      Array(store_returns, date_dim, store, customer)
    }

    val filter1 = date_dim.filter(row => row(6).toInt == YEAR)
    filter1.collect().foreach(_println)
    val map1 = filter1.map(row => (row.head, row))
    val map2 = store_returns.map(row => (row.last, row))
    val join1 = map2.join(map1)
    join1.collect().foreach(_println)
    val map3 = join1.map(map3_f)
    val rbk1 = map3.reduceByKey(rbk1_f)
    rbk1.collect().foreach(_println)
    val map4 = rbk1.map(map4_f)
    val map10 = store.map(row => (row.head, row))
    val join2 = map4.join(map10)
    join2.collect().foreach(_println)
    val map5 = join2.map(map5_f)
    val map9 = customer.map(row => (row.head, row))
    val join3 = map5.join(map9)
    join3.collect().foreach(_println)
    val map6 = join3.map(map6_f)
    val map7 = map6.map(map7_f)
    val reduce1 = map7.reduce{case ((v1, c1), (v2, c2)) => (v1+v2, c1+c2)}
    avg = reduce1._1 / reduce1._2.toFloat * 1.2f
    val filter2 = map6.filter(filter2_f)
    filter2.collect().foreach(_println)
    val map8 = filter2.map(map8_f)
  }

  def map3_f(row: (String, (Array[String], Array[String]))) = {
    val store_returns_row = row._2._1
    val sr_customer_sk = store_returns_row(2)
    val sr_store_sk = store_returns_row(6)
    // sum([AGG_FIELD])
    val sum_agg_field = List(10, 11, 12, 13, 15, 16, 17).map(n => convertColToFloat(store_returns_row, n)).reduce(_ + _)
    ((sr_customer_sk, sr_store_sk), (sr_customer_sk, sr_store_sk, sum_agg_field))
  }

  def convertColToFloat(row: Array[String], col: Int): Float = {
    try {
      row(col).toFloat
    } catch {
      case _ => 0.0f
    }
  }

  def rbk1_f(acc: (String, String, Float), e: (String, String, Float)) = {
    (acc, e) match {
      case ((r1c1, r1c2, sum1), (r2c1, r2c2, sum2)) =>
        (r1c1, r2c2, sum1 + sum2)
    }
  }

  def map4_f(row: ((String, String), (String, String, Float))) = {
    row match {
      case ((sr_customer_sk, sr_store_sk), rest) => (sr_store_sk, rest)
    }
  }

  def map5_f(row: (String, ((String, String, Float), Array[String]))) = {
    row match {
      case (store_sk, (ctr_row@(sr_customer_sk, st_store_sk, sum_agg_field), store_row)) => (sr_customer_sk, (ctr_row, store_row))
    }
  }

  def map6_f(row: (String, (((String, String, Float), Array[String]), Array[String]))) = {
    row match {
      case (customer_sk, ((ctr_row, store_row), customer_row)) =>
        (ctr_row, store_row, customer_row)
    }
  }

  def map8_f(row: ((String, String, Float), Array[String], Array[String])) = {
    row match {
      case (_, _, customer_row) =>
        customer_row(1)
    }
  }

  def map7_f(row: ((String, String, Float), Array[String], Array[String])) = {
    row match {
      case ((_, _, total_return), _, _) => (total_return, 1)
    }
  }

  def filter2_f(row: ((String, String, Float), Array[String], Array[String])) = {
    row match {
      case ((_, _, return_total), store_row, _) =>
        return_total > avg && store_row(24) == STATE
    }
  }

}