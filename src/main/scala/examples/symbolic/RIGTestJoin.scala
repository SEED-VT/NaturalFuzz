//package examples.symbolic
//
//import org.apache.spark.{SparkConf, SparkContext}
//import sparkwrapper.SparkContextWithDP
//import symbolicexecution.SymExResult
//
//object RIGTestJoin extends Serializable {
//
//  def main(args: Array[String]): SymExResult = {
//    println(s"RIGTestJoin: ${args.mkString(",")}")
//    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[*]")
//    sparkConf.setAppName("RIGTest Join")
//    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
//    ctx.setLogLevel("ERROR")
//    val ds1p = "mixmatch-data/rig-test-join/boxes1"
//    val ds2p = "mixmatch-data/rig-test-join/boxes2"
//
//    val ds1 = ctx.textFileProv(ds1p, _.split(","))
////      .map(row => Array(row.head) ++ row.tail.map(_.toInt))
//      .map(row => (row.head, (row(1), row(2).toInt)))
//      .filter {
//        row =>
//          _root_.monitoring.Monitors.monitorPredicateSymEx(row._2._2 == 3, (List(row._2._2), List()), 0)
//      }
//    val ds2 = ctx.textFileProv(ds2p, _.split(","))
////      .map(row => Array(row.head) ++ row.tail.map(_.toInt))
//      .map(row => (row.head, row.last))
////    ds1.join(ds2)
//
//    val joined = _root_.monitoring.Monitors.monitorJoinSymEx(ds1, ds2, 0) // PC: ds2.containsKey(ds1.col[0])
//
//    joined
//      .collect().foreach(println)
//
//    _root_.monitoring.Monitors.finalizeSymEx()
//  }
//
//  def if1(): Unit = {
//    println("if 1")
//  }
//
//  def if2(): Unit = {
//    println("if 2")
//  }
//
//  def if3(): Unit = {
//    println("if 2")
//  }
//
//}