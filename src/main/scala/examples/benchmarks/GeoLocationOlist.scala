package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object GeoLocationOlist {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("GeoLocation")
    val sc = new SparkContext(conf)
    val ds1Path = "seeds/full_data/olist/olist_geolocation_dataset.csv"
    val geoloc = sc.textFile(ds1Path)

  }

}