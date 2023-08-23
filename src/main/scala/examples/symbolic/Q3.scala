package examples.symbolic

import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance
import provenance.rdd.ProvenanceRDD.toPairRDD
import runners.Config
import symbolicexecution.{SymExResult, SymbolicExpression}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.SymImplicits._
import taintedprimitives.{TaintedString,TaintedInt}

import scala.util.Random

object Q3 extends Serializable {

  def main(args: Array[String], expressionAccumulator: CollectionAccumulator[SymbolicExpression]): SymExResult = {
    val sparkConf = new SparkConf()
    val ctx = new SparkContextWithDP(SparkContext.getOrCreate(sparkConf))
    ctx.setLogLevel("ERROR")
    val MANUFACT = 1 //rand.nextInt(1000 - 1) + 1
    val MONTH = 11 // rand.nextInt(2)+11

    val store_sales = ctx.textFileProv(args(0),_.split(","))
    val date_dim = ctx.textFileProv(args(1),_.split(","))
    val item = ctx.textFileProv(args(2),_.split(","))

//    store_sales.take(10).foreach(println)
//    date_dim.take(10).foreach(println)
//    item.take(10).foreach(println)

    val map1 = store_sales.map(row => (row.last, row))

    println("map1")
    map1.take(10).foreach(println)

    val safety1 = date_dim.filter{ row => try { row(8).toInt; true } catch { case _: Throwable => false}}
    val filter1 = safety1.filter(row => _root_.monitoring.Monitors.monitorPredicateSymEx(row(8).toInt/*d_moy*/ == MONTH, (List(), List()),0, expressionAccumulator))

    println("filter1")
    filter1.take(10).foreach(println)

    val map2 = filter1.map(row => (row.head, row))

    println("map2")
    map2.take(10).foreach(println)
      // t.d_date_sk = store_sales.ss_sold_date_sk
    val join1 = _root_.monitoring.Monitors.monitorJoinSymEx(map2,map1, 1, expressionAccumulator)

    println("join1")
    join1.take(10).foreach(println)

    val map3 = join1.map{
        case (date_sk, (date_dim_row, ss_row)) =>
          (ss_row(1)/*ss_item_sk*/, (date_dim_row, ss_row))
      }
      // and store_sales.ss_item_sk = item.i_item_sk

    val safety2 = item.filter{ row => try { row(13).toInt; true } catch { case _: Throwable => false}}
    val filter2 = safety2.filter(row => _root_.monitoring.Monitors.monitorPredicateSymEx(row(13).toInt /*i_manufact_id*/ == MANUFACT, (List(), List()), 1, expressionAccumulator)) // and item.i_manufact_id = [MANUFACT]
    val map4 = filter2.map(row => (row.head, row))

    println("filter2.map4")
    map4.take(10).foreach(println)

    val join2 = _root_.monitoring.Monitors.monitorJoinSymEx(map3, map4, 1, expressionAccumulator)

    println("join2")
    join2.take(10).foreach(println)

    val map5 = join2.map {
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

    println("map5")
    map5.take(10).foreach(println)

    val rbk1 = map5.reduceByKey {
        case ((d_year, i_brand, i_brand_id, v1),(_, _, _, v2)) =>
          (d_year, i_brand, i_brand_id, v1+v2)
      }

    println("rbk1")
    rbk1.take(10).foreach(println)

    val map6 = rbk1.map {
        case (_, (d_year, i_brand, i_brand_id, agg)) => (d_year, i_brand_id, i_brand, agg)
      }

    println("map6")
    map6.take(10).foreach(println)

    _root_.monitoring.Monitors.finalizeSymEx(expressionAccumulator)

    /*
    define AGGC= text({"ss_ext_sales_price",1},{"ss_sales_price",1},{"ss_ext_discount_amt",1},{"ss_net_profit",1});
    define MONTH = random(11,12,uniform);
    define MANUFACT= random(1,1000,uniform);
    define _LIMIT=100;

    [_LIMITA] select [_LIMITB] dt.d_year
          ,item.i_brand_id brand_id
          ,item.i_brand brand
          ,sum([AGGC]) sum_agg
    from  date_dim dt
         ,store_sales
         ,item
    where dt.d_date_sk = store_sales.ss_sold_date_sk
      and store_sales.ss_item_sk = item.i_item_sk
      and item.i_manufact_id = [MANUFACT]
      and dt.d_moy=[MONTH]
    group by dt.d_year
         ,item.i_brand
         ,item.i_brand_id
    order by dt.d_year
            ,sum_agg desc
            ,brand_id
    [_LIMITC];

    */

  }

  def convertColToFloat(row: Array[TaintedString], col: Int): Float = {
    try {
      row(col).toFloat
    } catch {
      case _ => 0
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