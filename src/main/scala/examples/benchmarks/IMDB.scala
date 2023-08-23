package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object IMDB {

  def main(args: Array[String]): Unit = {
    println(s"IMDB: ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("IMDB")
    val ctx = new SparkContext(sparkConf)
    ctx.setLogLevel("ERROR")
    val ds1p = "/home/ahmad/Downloads/IMDB/title.akas.tsv"
    val ds2p = "/home/ahmad/Downloads/IMDB/title.basics.tsv"

    /*
        Bug idea: Game show before
     */

    val ds1 = ctx.textFile(ds1p)
      .sample(false, 0.001f)
      .map {
        row =>
          val key :: rest = row.split("\t").toList
          (key, rest)
      }

    val ds2 = ctx.textFile(ds2p)
      .sample(false, 0.001f)
      .map {
        row =>
          val key :: rest = row.split("\t").toList
          (key, rest)
      }

    val date = 1930
    // ---- ANALYSIS -------------------------------------------
    //
    //    val yearsAndGenres = ds2
    //      .flatMap {
    //        case (_, List(_,_,_,_,year,_,_,genres)) =>
    //          if(year.forall(_.isDigit))
    //            genres.split(",").map((year.toInt,_))
    //          else
    //            Array((0, ""))
    //      }
    //
    //    yearsAndGenres
    //      .map {
    //        case (year, genre) =>
    //          if (year >= date) {
    //            if (genre == "Game-Show") println("HI")
    //            if (genre == "Talk-Show") println("BYE")
    //          }
    //      }.collect()
    //
    //    val before2000 = yearsAndGenres
    //      .filter{ case (year, genre) => year < date }
    //      .map { case (year, genre) => genre}
    //      .distinct
    //      .collect()
    ////      .foreach(println)
    //
    //    val after2000 = yearsAndGenres
    //      .filter { case (year, genre) => year >= date }
    //      .map { case (year, genre) => genre }
    //      .distinct
    //      .collect()
    ////      .foreach(println)
    //
    //
    //    before2000.foreach(println)
    //    println("===")
    //    after2000.foreach(println)
    //    println("\n=== diff ===")
    //
    //    after2000
    //      .toSet
    //      .diff(before2000.toSet)
    //      .foreach(println)
    // -----------------------------------------------

    ds1
      .join(ds2)
      .map {
        case (key, (l, r)) =>
          val year = if (r(4).forall(_.isDigit)) r(4).toInt else Int.MaxValue
          val genre = r(7)
          if (year > date) {
            println("blekh")
          } else {
            if (genre == "Talk-Show") {
              // ERRONEOUS PATH: WILL NOT BE COVERED BY ORIGINAL DATASET, COLUMN MIXING NEEDED
              throw new Exception("Talk shows weren't invented before 1930")
            }
          }
          key :: l ++ r
      }
      .collect()
      .foreach {
        s => println(s.mkString(" | "))
      }


    //    ds1.sample(false, 1.0/3000000).foreach(s => println(s.mkString(" ... ")))
    //    ds2.sample(false, 1.0/100000).foreach(s => println(s.mkString(" ... ")))
  }
}
