//package examples.symbolic
//
//import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}
//import provenance.rdd.ProvenanceRDD.toPairRDD
//import runners.Config
//import sparkwrapper.SparkContextWithDP
//import taintedprimitives.SymImplicits._
//
//object DummyTest extends Serializable {
//  def main(args: Array[String]): Unit = {
//    println(s"Dummy ${args.mkString(",")}")
//    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[*]")
//    sparkConf.setAppName("Dummy")
//    val before_data = Config.mapInputFilesReduced("WebpageSegmentation")(0) // args(0)
//    val after_data = Config.mapInputFilesReduced("WebpageSegmentation")(1)
//    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
//    ctx.setLogLevel("ERROR")
//    val ds1 = ctx.textFileProv(before_data, _.split(','))
//    val ds2 = ctx.textFileProv(after_data, _.split(','))
//
//    val joined = ds1.join(ds2) // ds1.col[0] == ds2.col[0]
//
//    val r1 = joined.map {
//      case (k, (v1, v2)) =>
//        (k, (v1.init.map(_.toInt), v2.init.map(_.toInt)))
//    }
//
//    // PC: ds2.containsKey(ds1.col[0]) (only for inner join)
//
//    val last = r1.map {
//      case (k, (v1, v2)) =>
//        // PC: ds2.containsKey(ds1.col[0]) && ds1.col[0] > ds2.col[0]
//        if (v1(0) > v2(0)) { //cross dataset comparison
//          (k , ("path1", "path1"))
//        }
//        // PC: ds2.containsKey(ds1.col[0]) && ds1.col[0] <= ds2.col[0]
//        else {
//          (k, ("path2", "path2"))
//        }
//    }
//
//    last
//      .collect()
//      .foreach{
//        case (k, (v1, v2)) =>
//          println(k.value, v1, v2)
//      }
//
//
//    _root_.monitoring.Monitors.finalizeSymEx()
//  }
//
//}