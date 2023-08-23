package testfiles

import org.apache.spark.{SparkConf,SparkContext}

object Test5 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))
    sc.setLogLevel("ERROR")
    val ds1 = sc.parallelize(Seq(("a", "ds1-v1"), ("b", "ds1-v2"), ("c", "ds1-v3")))
    val ds2 = sc.parallelize(Seq(("a", "ds2-v1"), ("a", "ds2-v2"), ("a", "ds2-v3")))

    ds1
      .leftOuterJoin(ds2)
      .collect()
      .foreach {
        println
      }
  }
}
