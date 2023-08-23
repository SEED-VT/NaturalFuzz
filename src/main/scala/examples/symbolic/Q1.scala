package examples.symbolic

import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance
import provenance.rdd.ProvenanceRDD.toPairRDD
import runners.Config
import symbolicexecution.{SymExResult, SymbolicExpression}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.SymImplicits._
import taintedprimitives.{TaintedString,TaintedInt, TaintedFloat}
import scala.util.Random

object Q1 extends Serializable {
  def main(args: Array[String], expressionAccumulator: CollectionAccumulator[SymbolicExpression]): SymExResult = {
//  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[*]").setAppName("TPC-DS Query 1")
    val sc = SparkContext.getOrCreate(sparkConf)
    val ctx = new SparkContextWithDP(sc)
    ctx.setLogLevel("ERROR")
    val YEAR = 1999
    val STATE = "TN"
//    val expressionAccumulator: CollectionAccumulator[SymbolicExpression] = sc.collectionAccumulator[SymbolicExpression]("ExpressionAccumulator")

    val Array(store_returns, date_dim, store, customer) = if (!args.isEmpty) {
      val store_returns = ctx.textFileProv(args(0),_.split(","))
      val date_dim = ctx.textFileProv(args(1),_.split(","))
      val store = ctx.textFileProv(args(2),_.split(","))
      val customer = ctx.textFileProv(args(3),_.split(","))
      Array(store_returns, date_dim, store, customer)
    } else {
      Array("store_returns", "date_dim", "store", "customer")
        .map(s => ctx.textFileProv(s"/home/ahmad/Documents/VT/project2/tpcds-datagen/data_csv_no_header/$s", _.split(",")))
    }

//    _root_.monitoring.Monitors.monitorPredicateSymEx()
//    store_returns.map(row => convertColToFloat(row, 10) + convertColToFloat(row, 11)).collect().foreach(x => println(x.expr))
//    store_returns
//      .filter(row => try{row(0).toInt + row(1).toInt; true} catch {case _: Throwable => false})
//      .map(row => row(0).toInt + row(1).toInt)
//      .collect()
//      .foreach(x => println(x.expr))
//    sys.exit(0)


    val filter1 = date_dim.filter(row => _root_.monitoring.Monitors.monitorPredicateSymEx(row(6).toInt == YEAR, (List(row(6).toInt, YEAR), List()), 0, expressionAccumulator))

    println("filter1")
    filter1.take(10).foreach(println)

    val map1 = filter1.map(row => (row.head, row))

    println("map1")
    map1.take(10).foreach(println)

    val map2 = store_returns.map(row => (row.last, row))

    println("map2")
    map2.take(10).foreach(println)

    val join1 = _root_.monitoring.Monitors.monitorJoinSymEx(map2, map1, 0, expressionAccumulator)

    println("join1")
    join1.take(10).foreach(println)

    val map3 = join1.map { row =>
      val store_returns_row = row._2._1
      val sr_customer_sk = store_returns_row(2)
      val sr_store_sk = store_returns_row(6)
      // sum([AGG_FIELD])
      val sum_agg_field = List(10, 11, 12, 13, 15, 16, 17).map(n => convertColToFloat(store_returns_row, n)).reduce(_+_)
      ((sr_customer_sk, sr_store_sk), (sr_customer_sk, sr_store_sk, sum_agg_field))
    }

    println("map3")
    map3.take(10).foreach(println)

    val rbk1 = map3.reduceByKey {
      case ((r1c1, r1c2, sum1), (r2c1, r2c2, sum2)) =>
        (r1c1, r2c2, sum1.value + sum2.value)
    }

    println("rbk1")
    rbk1.take(10).foreach(println)

    val map4 = rbk1.map {
      case ((sr_customer_sk, sr_store_sk), rest) => (sr_store_sk, rest)
    }

    println("map4")
    map4.take(10).foreach(println)

    val map10 = store.map(row => (row.head, row))
    val join2 = _root_.monitoring.Monitors.monitorJoinSymEx(map4,map10,1,expressionAccumulator)

    println("join2")
    join2.take(10).foreach(println)

    val map5 = join2.map{
        case (store_sk, (ctr_row @ (sr_customer_sk, st_store_sk, sum_agg_field), store_row)) => (sr_customer_sk, (ctr_row, store_row))
      }

    println("map5")
    map5.take(10).foreach(println)

    val map9 = customer.map(row => (row.head, row))
    val join3 = _root_.monitoring.Monitors.monitorJoinSymEx(map5, map9, 2, expressionAccumulator)

    println("join3")
    join3.take(10).foreach(println)

    val map6 = join3.map{
        case (customer_sk, ((ctr_row, store_row), customer_row)) => (ctr_row, store_row, customer_row)
      }

    println("map6")
    map6.take(10).foreach(println)

    val map7 = map6.map {
      case ((_,_,total_return), _, _) => (total_return, 1)
    }

    println("map7")
    map7.take(10).foreach(println)

    val reduce1 = map7.reduce{case ((v1, c1), (v2, c2)) => (v1+v2, c1+c2)}

    println("reduce1")
    println(reduce1)

    val avg = reduce1._1 / reduce1._2.toFloat * 1.2f

    // ---------------------------------------------------------------------------------------
    val filter2 = map6.filter {
        case ((_, _, return_total), store_row, _) =>
//          _root_.monitoring.Monitors.monitorPredicateSymEx(
//            return_total > avg && store_row(24) == STATE,
//            (List(return_total, avg, store_row(24),STATE), List()),
//            1,
//            expressionAccumulator)
          return_total > avg && store_row(24) == STATE
      }

    println("filter2")
    filter2.take(10).foreach(println)

    val map8 = filter2
      .map {
        case (_, _, customer_row) => customer_row(1)
      }
    println("map8")
    map8.take(10).foreach(println)

    _root_.monitoring.Monitors.finalizeSymEx(expressionAccumulator)

  }

  def convertColToFloat(row: Array[TaintedString], col: Int): TaintedFloat = {
    try {
      row(col).toFloat
    } catch {
      case _ => 0.0f
    }
  }
  /* ORIGINAL QUERY:
  define COUNTY = random(1, rowcount("active_counties", "store"), uniform);
  define STATE = distmember(fips_county, [COUNTY], 3);
  define YEAR = random(1998, 2002, uniform);
  define AGG_FIELD = text({"SR_RETURN_AMT",1},{"SR_FEE",1},{"SR_REFUNDED_CASH",1},{"SR_RETURN_AMT_INC_TAX",1},{"SR_REVERSED_CHARGE",1},{"SR_STORE_CREDIT",1},{"SR_RETURN_TAX",1});
  define _LIMIT=100;

  with customer_total_return as
  (
      select sr_customer_sk as ctr_customer_sk ,sr_store_sk as ctr_store_sk ,sum([AGG_FIELD])
                                                                                  as ctr_total_return
      from store_returns ,date_dim
      where sr_returned_date_sk = d_date_sk
      and d_year =[YEAR]
      group by sr_customer_sk ,sr_store_sk
  )
  [_LIMITA]

  select [_LIMITB] c_customer_id
  from customer_total_return ctr1 ,store ,customer
  where ctr1.ctr_total_return >   (
                                      -- subquery 1
                                      select avg(ctr_total_return)*1.2
                                      from customer_total_return ctr2
                                      where ctr1.ctr_store_sk = ctr2.ctr_store_sk
                                  )
  and s_store_sk = ctr1.ctr_store_sk
  and s_state = '[STATE]'
  and ctr1.ctr_customer_sk = c_customer_sk
  order by c_customer_id
  [_LIMITC];
   */
}