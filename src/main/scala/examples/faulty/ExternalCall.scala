package examples.faulty

import abstraction.{SparkConf, SparkContext}

import scala.math.log10

object ExternalCall {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split("\\s")) // "datasets/fuzzing_seeds/commute/trips"
      .map { s =>
        if(s.startsWith("F3")) throw new RuntimeException()
        (s,1)
      }
      .reduceByKey { (a, b) =>
        val sum = a+b
        if(sum < 0) throw new RuntimeException()
        sum
      }// Numerical overflow
      .filter{ v =>
        val v1 = log10(v._2)
        if(v._1.startsWith("3")) throw new RuntimeException()
        v1 > 1
      }
      .collect()
      .foreach(println)
  }
}