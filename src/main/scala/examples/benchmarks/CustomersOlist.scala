package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object CustomersOlist extends Serializable {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("Customers OList")
    val orders_data = args(0) // seeds/full_data/olist/olist_orders_dataset.csv
    val items_data = "seeds/full_data/olist/olist_order_items_dataset.csv"
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.setLogLevel("ERROR")
    val orders = ctx.textFile(orders_data).map(_.split(","))
    val order_items = ctx.textFile(items_data).map(_.split(","))
    //  ---------------------------------------------------------------------------------------

    // ideas: could make more complicated by adding a time limit e.g. customers who spent $1000+ after 2017

    val pairs_oid_cid = orders
      .map(row => (row(0), row(1)))

    val pairs_oid_price = order_items
      .map(row => (row(0), row(5)))

    val purchases = pairs_oid_cid
      .join(pairs_oid_price)
      .map{ case (_, (cid, price)) => (removeQuotes(cid), price.toFloat) }

    val total_spending = purchases
      .reduceByKey{_+_}
      .filter(_._2 > 5000)
      .collect()
      .foreach(println)
    // customers:
    //  "customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"
    // orders:
    //  "order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"
    // items:
    // "order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"
  }

  def removeQuotes(str: String): String = {
    if(str.startsWith("\"")) {
      str.substring(1, str.length-1)
    } else {
      str
    }
  }
}