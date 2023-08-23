package examples.tpcds

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Q15 extends Serializable {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("TPC-DS Query 15").setMaster("spark://zion-headnode:7077")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("ERROR")
    val YEAR = 1999
    val QOY = 1
    val ZIPS = List("85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792")
    val STATES = List("CA", "WA", "GA")
    val catalog_sales = sc.textFile(args(0)).map(_.split(","))
    val customer = sc.textFile(args(1)).map(_.split(","))
    val customer_address = sc.textFile(args(2)).map(_.split(","))
    val date_dim = sc.textFile(args(3)).map(_.split(","))
    val filtered_dd = date_dim.filter { row =>
      val d_qoy = row(10)
      val d_year = row(6)
      d_qoy == QOY.toString && d_year == YEAR.toString
    }
    filtered_dd.take(10).foreach(println)

    val map1 = catalog_sales.map(row => (row(2), row))
    val map2 = customer.map(row => (row.head, row))
    val join1 = map1.join(map2)
    join1.take(10).foreach(println)

    val map3 = join1.map({
      case (_, (cs_row, c_row)) =>
        (c_row(4), (cs_row, c_row))
    })
    val map4 = customer_address.map(row => (row.head, row))
    val join2 = map3.join(map4)
    join2.take(10).foreach(println)

    val map5 = join2.map({
      case (_, ((cs_row, c_row), ca_row)) =>
        (cs_row.last, (cs_row, c_row, ca_row))
    })
    val filter1 = map5.filter({
      case (_, (cs_row, c_row, ca_row)) =>
        val ca_zip = getColOrEmpty(ca_row, 9)
        val ca_state = getColOrEmpty(ca_row, 8)
        val cs_sales_price = convertColToFloat(cs_row, 20)
          ZIPS.contains(ca_zip.take(5)) || cs_sales_price > 500.0f || STATES.contains(ca_state)
    })
    filter1.take(10).foreach(println)

    val map6 = filtered_dd.map(row => (row.head, row))
    val join3 = filter1.join(map6)
    join3.take(10).foreach(println)

    val map7 = join3.map({
      case (_, ((cs_row, c_row, ca_row), dd_row)) =>
        val cs_sales_price = convertColToFloat(cs_row, 20)
        (getColOrEmpty(ca_row, 9), cs_sales_price)
    })
    val rbk1 = map7.reduceByKey(_ + _)
    rbk1.take(10).foreach(println)

    val sortBy1 = rbk1.sortBy(_._1)
    sortBy1.take(10).foreach(println)

    
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