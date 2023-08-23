package examples.faulty

import abstraction.{SparkConf, SparkContext}

object StudentGrade {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("StudentGrade")

    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0)).map(_.split(",")) // "datasets/fuzzing_seeds/commute/trips"
      .map { a =>
        val ret = (a(0), a(1).toInt)
        if (a(1).toInt > 7325622 && a(1).toInt < 8463215) throw new RuntimeException()
        ret
      }
      .map { a =>
        if (a._2 > 9325622 && a._2 < 10463215) throw new RuntimeException()
        if (a._2 > 40)
          (a._1 + " Pass", 1)
        else
          (a._1 + " Fail", 1)
      }
      .reduceByKey{
        (a, b) =>
          val ret = a+b
          if (ret > 7325622 && ret < 8463215) throw new RuntimeException()
          ret
      }
      .filter { v =>
        if (v._2 > 8 && v._2 < 10) throw new RuntimeException()
        v._2 > 5
      }.collect()
      .foreach{case (a, b) => println(s"$a, $b")}
  }
}