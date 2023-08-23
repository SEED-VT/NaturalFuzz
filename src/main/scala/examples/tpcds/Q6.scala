package examples.tpcds

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Q6 extends Serializable {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("TPC-DS Query 6")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("ERROR")
    //    val datasetsPath = "./data_tpcds"
    val MONTH = 1
    val YEAR = 2001
    val customer_address = sc.textFile(args(0)).map(_.split(","))
    val customer = sc.textFile(args(1)).map(_.split(","))
    val store_sales = sc.textFile(args(2)).map(_.split(","))
    val date_dim = sc.textFile(args(3)).map(_.split(","))
    val item = sc.textFile(args(4)).map(_.split(","))


    val filter1 = date_dim.filter {
      row => row(6).toInt == YEAR && row(8).toInt == MONTH
    }
    filter1.take(10).foreach(println)

    val map1 = filter1.map(row => row(3))
    val distinct = map1.distinct
    val take1 = distinct.take(1).head
    val map2 = customer_address.map(row => (row.head, row))
    val map3 = customer.map(row => (row(4), row))
    val join1 = map2.join(map3)
    join1.take(10).foreach(println)

    val map4 = join1.map({
      case (addr_sk, (ca_row, c_row)) =>
        (c_row.head, (ca_row, c_row))
    })
    val map5 = store_sales.map(row => (row(2), row))
    val join2 = map4.join(map5)
    join2.take(10).foreach(println)

    val map6 = join2.map({
      case (customer_sk, ((ca_row, c_row), ss_row)) =>
        (ss_row.last, (ca_row, c_row, ss_row))
    })
    val map7 = date_dim.map(row => (row.head, row))
    val join3 = map6.join(map7)
    join3.take(10).foreach(println)

    val map8 = join3.map({
      case (date_sk, ((ca_row, c_row, ss_row), dd_row)) =>
        (ss_row(1), (ca_row, c_row, ss_row, dd_row))
    })
    val map9 = item.map(row => (row.head, row))
    val join4 = map8.join(map9)
    join4.take(10).foreach(println)

    val map10 = join4.map({
      case (item_sk, ((ca_row, c_row, ss_row, dd_row), i_row)) =>
        (ca_row, c_row, ss_row, dd_row, i_row)
    })
    val map11 = map10.map({
      case (_, _, _, _, i_row) =>
        (convertColToFloat(i_row, 5), 1)
    })
    val reduce1 = map11.reduce({
      case ((v1, c1), (v2, c2)) =>
        (v1 + v2, c1 + c2)
    })
    println(s"reduce1 = $reduce1")


    val subquery2_result = reduce1._1 / reduce1._2
    println(s"subquery2 result = ${subquery2_result}")

    val filter2 = map10.filter(tup => tup._4(3) == take1)
    filter2.take(10).foreach(println)

    val filter3 = filter2.filter({
      case (_, _, _, _, i_row) =>
        val i_current_price = convertColToFloat(i_row, 5)
        i_current_price > (subquery2_result * 1.2f)
    })
    filter3.take(10).foreach(println)

    val map12 = filter3.filter {
        case (ca_row, c_row, ss_row, dd_row, i_row) =>
          ca_row.length >= 9
      }
      .map {
        case (ca_row, c_row, ss_row, dd_row, i_row) =>
          (ca_row(8), 1)
      }

    val rbk1 = map12.reduceByKey(_ + _)
    rbk1.take(10).foreach(println)

    val filter4 = rbk1.filter({
      case (state, count) =>
        count > 10
    })
    filter4.take(10).foreach(println)

    val sortBy1 = filter4.sortBy(_._2)
    val take2 = sortBy1.take(10)
    val sortWith1 = take2.sortWith({
      case (a, b) =>
        a._2 < b._2 || a._2 == b._2 && a._1 < b._1
    })
    sortWith1.foreach({
      case (state, count) =>
        println(state, count)
    })





    /*
    define YEAR = random(1998, 2002, uniform);
    define MONTH= random(1,7,uniform);
    define _LIMIT=100;

    [_LIMITA] select [_LIMITB] a.ca_state state, count(*) cnt
    from customer_address a
        ,customer c
        ,store_sales s
        ,date_dim d
        ,item i
    where       a.ca_address_sk = c.c_current_addr_sk
      and c.c_customer_sk = s.ss_customer_sk
      and s.ss_sold_date_sk = d.d_date_sk
      and s.ss_item_sk = i.i_item_sk
      and d.d_month_seq =
           (select distinct (d_month_seq)
            from date_dim
                  where d_year = [YEAR]
              and d_moy = [MONTH] )
      and i.i_current_price > 1.2 *
                (select avg(j.i_current_price)
           from item j
           where j.i_category = i.i_category)
    group by a.ca_state
    having count(*) >= 10
    order by cnt, a.ca_state
    [_LIMITC];


    */

  }

  def convertColToFloat(row: Array[String], col: Int): Float = {
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