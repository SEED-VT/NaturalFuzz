//package examples.symbolic
//
//import org.apache.spark.{SparkConf, SparkContext}
//import sparkwrapper.SparkContextWithDP
//import taintedprimitives.TaintedInt
//
//object IMDB {
//
//  def main(args: Array[String]): Unit = {
//    println(s"IMDB: ${args.mkString(",")}")
//    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[*]")
//    sparkConf.setAppName("IMDB")
//    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
//    ctx.setLogLevel("ERROR")
//    val ds1p = "/home/ahmad/Downloads/IMDB/title.akas.tsv"
//    val ds2p = "/home/ahmad/Downloads/IMDB/title.basics.tsv"
//
//    val ds1 = ctx.textFileProv(ds1p, _.split("\t"))
//      .sample(false, 0.001f)
//      .map {
//        row =>
//          val key :: rest = row.toList
//          (key, rest)
//      }
//
//    val ds2 = ctx.textFileProv(ds2p, _.split("\t"))
//      .sample(false, 0.001f)
//      .map {
//        row =>
//          val key :: rest = row.toList
//          (key, rest)
//      }
//
//    val date = 1930
//    // ---- ANALYSIS -------------------------------------------
//    //
//    //    val yearsAndGenres = ds2
//    //      .flatMap {
//    //        case (_, List(_,_,_,_,year,_,_,genres)) =>
//    //          if(year.forall(_.isDigit))
//    //            genres.split(",").map((year.toInt,_))
//    //          else
//    //            Array((0, ""))
//    //      }
//    //
//    //    yearsAndGenres
//    //      .map {
//    //        case (year, genre) =>
//    //          if (year >= date) {
//    //            if (genre == "Game-Show") println("HI")
//    //            if (genre == "Talk-Show") println("BYE")
//    //          }
//    //      }.collect()
//    //
//    //    val before2000 = yearsAndGenres
//    //      .filter{ case (year, genre) => year < date }
//    //      .map { case (year, genre) => genre}
//    //      .distinct
//    //      .collect()
//    ////      .foreach(println)
//    //
//    //    val after2000 = yearsAndGenres
//    //      .filter { case (year, genre) => year >= date }
//    //      .map { case (year, genre) => genre }
//    //      .distinct
//    //      .collect()
//    ////      .foreach(println)
//    //
//    //
//    //    before2000.foreach(println)
//    //    println("===")
//    //    after2000.foreach(println)
//    //    println("\n=== diff ===")
//    //
//    //    after2000
//    //      .toSet
//    //      .diff(before2000.toSet)
//    //      .foreach(println)
//    // -----------------------------------------------
//
//    _root_.monitoring.Monitors.monitorJoinSymEx(ds1, ds2, 0)
//      .map {
//        case (key, (l, r)) =>
//          val year = if (r(4).forall(_.isDigit)) r(4).toInt else TaintedInt.MaxValue
//          val genre = r(7)
//          if (_root_.monitoring.Monitors.monitorPredicateSymEx(year > date, (List(year, date), List()), 0)) {
//            println("blekh")
//          } else {
//            if (_root_.monitoring.Monitors.monitorPredicateSymEx(genre.hashCodeTainted == "Talk-Show".hashCode, (List(genre), List()), 0)) {
//              // ERRONEOUS PATH: WILL NOT BE COVERED BY ORIGINAL DATASET, COLUMN MIXING NEEDED
//              throw new Exception("Talk shows weren't invented before 1930")
//            }
//          }
//          key :: l ++ r
//      }
//      .collect()
//      .foreach {
//        s => println(s.mkString(" | "))
//      }
//
//
//    //    ds1.sample(false, 1.0/3000000).foreach(s => println(s.mkString(" ... ")))
//    //    ds2.sample(false, 1.0/100000).foreach(s => println(s.mkString(" ... ")))
//    _root_.monitoring.Monitors.finalizeSymEx()
//  }
//}
